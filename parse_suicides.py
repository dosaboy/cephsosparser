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

import argparse
import datetime

from common import get


class CephSuicideStatsCollection(object):
    def __init__(self, month, events):
        self.suicide_stats = {}
        self.thread_index = {}
        self.month = month
        self.events = events

    def parse(self):
        for event in self.events:
            osd = event['data'].group(1)
            t = datetime.datetime.strptime(event['data'].group(2),
                                           '%Y-%m-%d %H:%M:%S.%f')
            thread = event['data'].group(3)
            timeout = event['data'].group(4)

            suicide = {'timestamp': t,
                       'timeout': timeout,
                       'thread': thread}
            if osd in self.suicide_stats:
                self.suicide_stats[osd]['suicides'].append(suicide)
            else:
                self.suicide_stats[osd] = {'suicides': [suicide],
                                           'host': event['host']}

    def get_osds_by_host(self):
        hosts = {}
        for osd in self.suicide_stats:
            host = self.suicide_stats[osd]['host']
            if host in hosts:
                hosts[host].append(osd)
            else:
                hosts[host] = [osd]

        for host in hosts:
            hosts[host] = sorted(hosts[host])

        return hosts

    def get_stats(self):
        stats = {}
        day_counters = {}
        for osd in self.suicide_stats:
            for s in self.suicide_stats[osd]['suicides']:
                t = s['timestamp']
                if int(t.month) == self.month:
                    if t.day not in day_counters:
                        day_counters[t.day] = {}

                    if osd not in day_counters[t.day]:
                        day_counters[t.day][osd] = 0

                    if t.day in stats:
                        stats[t.day]['count'] += 1
                    else:
                        stats[t.day] = {'count': 1, 'maxosd': None}

                    day_counters[t.day][osd] += 1

                if osd not in self.thread_index:
                    self.thread_index[osd] = {}

                if t.day not in self.thread_index[osd]:
                    self.thread_index[osd][t.day] = []

                self.thread_index[osd][t.day].append((t, s['thread']))

        for day in day_counters:
            _max = []
            for osd in day_counters[day]:
                if not _max or _max[1] < day_counters[day][osd]:
                    _max = (osd, day_counters[day][osd])

            stats[day]['maxosd'] = _max[0]

        return sorted(stats.keys()), stats

    def get_osd_threads(self, day, osd):
        return [t[1] for t in sorted(collection.thread_index[osd][day],
                                     key=lambda e: e[0])]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default=None, required=True)
    parser.add_argument('--month', type=int, default=None, required=True)
    parser.add_argument('--cache', action='store_true', default=False)
    args = parser.parse_args()

    keywords = '"had suicide timed out"'
    filter = (r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9][0-9]"
              "[0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:[0-9]+:"
              "[0-9]+\.[0-9]*)\s*([a-z0-9]*)\s*.+had suicide timed out "
              "after (.+)")

    collection = CephSuicideStatsCollection(args.month,
                                            get(args.path, keywords, filter,
                                                args.cache))
    collection.parse()

    print "OSD Suicide stats for month %s" % (args.month)
    suicides = []
    month = int(args.month)
    for osd in collection.suicide_stats:
        _suicides = collection.suicide_stats[osd]['suicides']
        suicides += [s for s in _suicides if s['timestamp'].month == month]
    print "Total suicides: %s" % len(suicides)

    keys, stats = collection.get_stats()
    data = ["\n    %s - %s (maxosd=%s, host=%s, threads=%s)" %
            (k, stats[k]['count'], stats[k]['maxosd'],
             collection.suicide_stats[stats[k]['maxosd']]['host'],
             collection.get_osd_threads(k, stats[k]['maxosd']))
            for k in keys] or ["\n    none"]
    print "\n  No. suicides by day: %s" % ' '.join(data)

    print ""
