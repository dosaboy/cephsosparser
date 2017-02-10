#!/usr/bin/python2
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

import re

from common import avg

env = {}
current = None


def parse(filename):
    with open(filename, 'r') as fd:
        dev = None
        for line in fd.readlines():
            res = re.search("^.+\.bskyb.com", line)
            if res:
                env[res.group(0)] = {'read': [], 'write': [], 'verify': []}
                current = res.group(0)
                continue

            res = re.search("^/dev/.*$", line)
            if res:
                dev = res.group(0)
                continue

            for t in ['read', 'write', 'verify']:
                res = re.search("^%s:\s*(.*?)\s" % (t), line)
                if res:
                    env[current][t].append((dev, int(res.group(1))))
                    continue


def findworst(data):
    maxdev = None
    maxval = 0
    for d in data:
        if d[1] > maxval:
            maxdev = d[0]

    return maxdev


if __name__ == "__main__":
    parse('smart_recovery_data')
    for k in env:
        print "{} recovery stats".format(k)
        for t in ['read', 'write', 'verify']:
            data = env[k][t]
            vals = [d[1] for d in data]
            a = avg(vals)
            print "  {}".format(t)
            print "      sum: {} avg: {} max: {}".format(sum(vals),
                                                         a, max(vals))
            worst = findworst(data)
            if worst is None:
                worst = 'n/a'

            print "      worst: {}".format(worst)

        print ''
