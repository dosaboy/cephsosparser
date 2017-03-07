# Author: Edward Hope-Morley (opentastic@gmail.com)
# Description: Ceph log parser
# Copyright (C) 2016 Edward Hope-Morley
#
# License:
#
# This file is part of cephsosparser.
#
# cephsosparser is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# cephsosparser is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with cephsosparser.  If not, see <http://www.gnu.org/licenses/>.
import os
import re
import hashlib

from subprocess import check_output, CalledProcessError


class CacheNotReadyException(Exception):
    pass


class ResultsCache(object):
    def __init__(self, target, filter):
        self.filter = filter
        self.target = target
        cachedir = os.path.join('/tmp/cephsosparser')
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir, mode=0755)

        self.cachefile = os.path.join(cachedir, '%s.cache' %
                                      (os.path.basename(self.target)))
        self.hit = False
        self.header_index_hashsum = 0
        self.header_index_filter = 1
        self.header_max_lines = 10
        self.cache_header = {}

    def _get_header(self):
        if os.path.isfile(self.cachefile):
            with open(self.cachefile, 'r') as fd:
                for i in xrange(0, 9):
                    self.cache_header[i] = fd.readline().strip('\n')

    def check(self):
        self._get_header()
        h = hashlib.sha256()
        h.update(self.target)
        if os.path.isfile(self.cachefile):
            with open(self.cachefile, 'r') as fd:
                if ((self.cache_header[self.header_index_hashsum] ==
                     h.hexdigest()) and
                    (self.cache_header[self.header_index_filter] ==
                     self.filter)):
                    self.data = fd.readlines()
                    self.hit = True

        if not self.hit:
            with open(self.cachefile, 'w') as fd:
                fd.write("%s\n" % h.hexdigest())
                fd.write("%s\n" % self.filter)

        return self.hit

    def append(self, line):
        if not self.hit:
            with open(self.cachefile, 'a') as fd:
                fd.write("%s\n" % line)
        else:
            raise CacheNotReadyException("cache '%s' already populated - "
                                         "delete first before writing" %
                                         (self.cachefile))

    def readlines(self):
        if not self.hit:
            raise CacheNotReadyException("cache '%s' is not ready" %
                                         (self.cachefile))

        return self.data


def get_hostname_from_path(path):
    hostname = '<unknownhost>'
    res = re.search(r".+sosreport-(.+)\.[0-9]+-[0-9]+/var.+", path)
    if res:
        hostname = res.group(1)

    return hostname


def uniq(l):
    return list(set(l))


def avg(vals):
    return reduce(lambda x, y: x + y, vals) / len(vals)


def get(path, keywords, filter, cache_results=False):
    """
    @param path: path to logfile(s)
    @param keywords: grep filter to find relevant log files.
    @param filter: regular expression used to find relevant results.
    @param cache_results: If True will attempt to load results from cache.
                          A valid cache must have the same filter and sha256sum
                          as the current query otherwise the original query is
                          run on the target and the cache contents are
                          overwritten.
    """
    events = []

    if os.path.isfile(path):
        paths = [path]
    else:
        out = check_output(['find', path, '-type', 'f', '-name', 'ceph*'])
        paths = [path for path in out.split('\n')
                 if re.search('var/log/ceph/ceph-osd.+', path)]

    for path in paths:
        if cache_results:
            cache = ResultsCache(path, filter)
            cache.check()
        else:
            cache = None

        hostname = get_hostname_from_path(path)
        try:
            cmd = ['zgrep', '-EHi', keywords, path]
            if not (cache and cache.hit):
                lines = check_output(' '.join(cmd), shell=True).split('\n')
            else:
                lines = cache.readlines()

            for line in lines:
                if cache and not cache.hit:
                    cache.append("%s\n" % line)

                # filter out subthread logs
                if not re.search(r":\s+-[0-9]*>", line):
                    res = re.search(filter, line)
                    if res:
                        events.append({'host': hostname, 'data': res})

        except CalledProcessError:
            pass

    return events
