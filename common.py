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

from subprocess import check_output, CalledProcessError


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


def get(path, keywords, filter):
    events = []

    if os.path.isfile(path):
        paths = [path]
    else:
        out = check_output(['find', path, '-type', 'f', '-name', 'ceph*'])
        paths = [path for path in out.split('\n')
                 if re.search('var/log/ceph/ceph-osd.+', path)]

    for path in paths:
        hostname = get_hostname_from_path(path)
        try:
            cmd = ['zgrep', '-EHi', keywords, path]
            out = check_output(' '.join(cmd), shell=True)
            for line in out.split('\n'):
                # filter out subthread logs
                if not re.search(r":\s+-[0-9]*>", line):
                    res = re.search(filter, line)
                    if res:
                        events.append({'host': hostname, 'data': res})

        except CalledProcessError:
            pass

    return events
