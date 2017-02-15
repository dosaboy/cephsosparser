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
import re

from common import avg, uniq, get


class CephSlowRequestStatsCollection(object):
    def __init__(self, events):
        self.events = events
        self.MAX_TOPS = 10
        self.aggrs_by_osd = {}
        self.aggrs_by_date = {}
        self.aggrs_by_host = {}
        self.epoc = None
        self.osd_stats = {}
        self.mins = []
        self.maxs = []
        self.avgs = []
        self.date_avgs = []
        self.date_maxs = []

    def parse(self):
        for event in self.events:
            osd = event['data'].group(1)
            t = datetime.datetime.strptime(event['data'].group(2),
                                           '%Y-%m-%d %H:%M:%S.%f')
            if osd in self.osd_stats:
                data = (t, float(event['data'].group(3)))
                self.osd_stats[osd]['slow_requests'].append(data)
            else:
                val = float(event['data'].group(3))
                self.osd_stats[osd] = {'host': event['host'],
                                       'slow_requests': [(t, val)]}

    def aggregate(self, osd):
        host = self.osd_stats[osd]['host']
        vals = self.osd_stats[osd]['slow_requests']
        for d, v in vals:
            if d in self.aggrs_by_date:
                self.aggrs_by_date[d].append((osd, v))
            else:
                self.aggrs_by_date[d] = [(osd, v)]

            if host not in self.aggrs_by_host:
                self.aggrs_by_host[host] = []

            self.aggrs_by_host[host].append(v)

        if osd not in self.aggrs_by_osd:
            self.aggrs_by_osd[osd] = vals
        else:
            self.aggrs_by_osd[osd] += vals

    def keep_top_avgs(self, osd, val):
        if not self.avgs:
            self.avgs.append((osd, val))
            return

        if (val > max([e[1] for e in self.avgs]) or
                val > float(self.avgs[0][1])):
            self.avgs.append((osd, val))

        self.avgs = sorted(self.avgs, key=lambda e: float(e[1]))
        if len(self.avgs) > self.MAX_TOPS:
            self.avgs.pop(0)

    def keep_top_mins(self, osd, val):
        if not self.mins:
            self.mins.append((osd, val))
            return

        if (val < min([e[1] for e in self.mins]) or
                val < float(self.maxs[-1][1])):
            self.mins.append((osd, val))

        self.mins = sorted(self.mins, key=lambda e: float(e[1]))
        if len(self.mins) > self.MAX_TOPS:
            self.mins.pop(self.MAX_TOPS)

    def keep_top_maxs(self, osd, val):
        if not self.maxs:
            self.maxs.append((osd, val))
            return

        if (val > max([e[1] for e in self.maxs]) or
                val > float(self.maxs[0][1])):
            self.maxs.append((osd, val))

        self.maxs = sorted(self.maxs, key=lambda e: float(e[1]))
        if len(self.maxs) > self.MAX_TOPS:
            self.maxs.pop(0)

    def date_avg(self):
        for d in self.aggrs_by_date:
            a = avg([a[1] for a in self.aggrs_by_date[d]])
            self.date_avgs.append((d, a))

    def date_max(self):
        for d in self.aggrs_by_date:
            a = max([a[1] for a in self.aggrs_by_date[d]])
            self.date_maxs.append((d, a))

    def day_highest_osd(self, d, key):
        equals = []
        _osd = None
        for osd in self.osd_stats:
            for e in self.osd_stats[osd]['slow_requests']:
                if str(e[0].day) == str(d):
                    if (not _osd or
                            self.osd_stats[_osd][key] <
                            self.osd_stats[osd][key]):
                        _osd = osd
                        equals = []
                    elif self.osd_stats[_osd][key] == self.osd_stats[osd][key]:
                        equals.append(osd)

        return list(set([_osd] + equals))[0]

    def total_slow_requests(self):
        total = 0
        for osd in self.osd_stats:
            total += len(self.osd_stats[osd]['slow_requests'])

        return total

    def get_osds_by_host(self):
        hosts = {}
        for osd in self.osd_stats:
            host = self.osd_stats[osd]['host']
            if host in hosts:
                hosts[host].append(osd)
            else:
                hosts[host] = [osd]

        for host in hosts:
            hosts[host] = sorted(hosts[host])

        return hosts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default=None, required=True)
    args = parser.parse_args()

    filter = (r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9]"
              "[0-9][0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:"
              "[0-9]+:[0-9]+\.[0-9]*)\s.+blocked for > "
              "([0-9\.]*) secs")
    keywords = '"slow requests"'

    collection = CephSlowRequestStatsCollection(get(args.path, keywords,
                                                    filter))
    collection.parse()

    osds = list(collection.osd_stats.keys())
    osds = sorted(osds, key=lambda s: float(s.partition('.')[2]))
    print "Slow request stats for %s OSDs" % len(osds)
    print "Total slow requests: %s" % collection.total_slow_requests()

    for osd in osds:
        delays = [e[1] for e in collection.osd_stats[osd]['slow_requests']]
        m = min(delays)
        collection.keep_top_mins(osd, m)
        collection.osd_stats[osd]['min'] = m
        m = max(delays)
        collection.keep_top_maxs(osd, m)
        collection.osd_stats[osd]['max'] = m
        a = avg(delays)
        collection.keep_top_avgs(osd, a)
        collection.osd_stats[osd]['avg'] = a
        collection.aggregate(osd)

    collection.date_avg()
    collection.date_max()

    _collection = {}
    for a in collection.date_avgs:
        d = str(a[0].month)
        if d not in _collection:
            _collection[d] = [a[1]]
        else:
            _collection[d].append(a[1])

    month_avgs = sorted([(d, avg(v)) for d, v in _collection.iteritems()],
                        key=lambda e: int(e[0]))

    _collection = {}
    for a in collection.date_avgs:
        d = str(a[0].day)
        if d not in _collection:
            _collection[d] = [a[1]]
        else:
            _collection[d].append(a[1])

    day_avgs = sorted([(d, avg(v)) for d, v in _collection.iteritems()],
                      key=lambda e: int(e[0]))

    _collection = {}
    for a in collection.date_maxs:
        d = str(a[0].day)
        if d not in _collection:
            _collection[d] = [a[1]]
        else:
            _collection[d].append(a[1])

    day_maxs = sorted([(d, max(v)) for d, v in _collection.iteritems()],
                      key=lambda e: int(e[0]))

    hosts = collection.get_osds_by_host()
    for host in hosts:
        print "\n%s:" % host
        osds = sorted(hosts[host], key=lambda e: int(e.partition('.')[2]))
        data = ["    %s" % osd for osd in osds]
        print "%s" % '\n'.join(data)

    aggrs_by_osd = collection.aggrs_by_osd
    print "\nTop %s:" % collection.MAX_TOPS
    data = ["\n      %s - %s (%s)" %
            (e[0], e[1], ' '.join(uniq([str(a[0]) for a in aggrs_by_osd[e[0]]
                                        if e[1] == a[1]])))
            for e in collection.mins]
    print "\n    Min Wait (s): %s" % ' '.join(data)

    data = ["\n      %s - %s (%s)" %
            (e[0], e[1], ' '.join(uniq([str(a[0]) for a in aggrs_by_osd[e[0]]
                                        if e[1] == a[1]])))
            for e in collection.maxs]
    data = sorted(data,
                  key=lambda v:
                  float(re.search(".+ - ([0-9]*\.[0-9]*).+", v).group(1)),
                  reverse=True)
    print "\n    Max Wait (s): %s" % ' '.join(data)

    aggrs_by_host = collection.aggrs_by_host
    for host in aggrs_by_host:
        aggrs_by_host[host] = sum(aggrs_by_host[host])

    data = ["\n      %s - %d" %
            (host, aggrs_by_host[host]) for host in aggrs_by_host]
    data = sorted(data, key=lambda v: int(v.partition(' - ')[2]),
                  reverse=True)
    data = data or ["\n    none"]
    print "\n    Total Wait By Host (s): %s" % ' '.join(data)

    data = ["\n      %s - %s" % (e[0], e[1]) for e in collection.avgs]
    data = sorted(data, key=lambda v: float(v.partition(' - ')[2]),
                  reverse=True)
    data = data or ["\n    none"]
    print "\n    Avg Wait (s): %s" % ' '.join(data)

    data = ["\n      %s - %s" % (e[0], e[1]) for e in month_avgs]
    data = data or ["\n    none"]
    print "\n    Avg Wait By Month (s): %s" % ' '.join(data)

    data = ["\n      %s - %s (max:%s)" %
            (e[0], e[1], collection.day_highest_osd(e[0], 'avg'))
            for e in day_avgs]
    data = data or ["\n    none"]
    print "\n    Avg Wait By Day (s): %s" % ' '.join(data)

    data = ["\n      %s - %s (max:%s)" %
            (e[0], e[1], collection.day_highest_osd(e[0], 'max'))
            for e in day_maxs]
    data = data or ["\n    none"]
    print "\n    Max Wait By Day (s): %s" % ' '.join(data)

    print ''
