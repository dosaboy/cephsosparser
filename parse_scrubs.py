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

from common import get


class CephScrubStatsCollection(object):
    def __init__(self, args, events):
        self.args = args
        self.month = args.month
        self.aggrs_by_osd = {}
        self.aggrs_by_date = {}
        self.epoc = None
        self.scrub_stats = {}
        self.mins = []
        self.maxs = []
        self.avgs = []
        self.date_avgs = []
        self.events = events
        self.repeats = []

    def parse(self):
        last_completed = {}

        for event in self.events:
            data = event['data']
            osd = data.group(1)
            t = datetime.datetime.strptime(data.group(2),
                                           '%Y-%m-%d %H:%M:%S.%f')

            info = data.group(3)
            pg = info.split()[0]
            action = info.split()[1]
            status = info.split()[2]

            empty = {'start': None, 'end': None}
            empty_actions = {'scrub': empty, 'deep-scrub': empty}
            empty_pg = {pg: {'actions': empty_actions,
                             'shelved_actions': {'deep-scrub': [],
                                                 'scrub': []}}}
            if osd not in self.scrub_stats:
                self.scrub_stats[osd] = {'pgs': empty_pg}

            if pg not in self.scrub_stats[osd]['pgs']:
                self.scrub_stats[osd]['pgs'].update(empty_pg)

            _pg = self.scrub_stats[osd]['pgs'][pg]
            if status == "starts":
                _pg['actions'][action]['start'] = t
            elif status == "ok":
                _pg['actions'][action]['end'] = t
                info = _pg['actions'][action]
                if all(info):
                    # Track repeats - http://tracker.ceph.com/issues/16474
                    if action == 'deep-scrub':
                        if osd not in last_completed:
                            last_completed[osd] = {'pg': pg, 'count': 1}
                        elif last_completed[osd]['pg'] == pg:
                            last_completed[osd]['count'] += 1
                        else:
                            if last_completed[osd]['count'] > 1:
                                count = last_completed[osd]['count']
                                self.repeats.append({'osd': osd, 'pg': pg,
                                                     'count': count})
                            last_completed[osd] = {'pg': pg, 'count': 1}

                    actions = _pg['shelved_actions'][action]
                    actions.append(info)
                    _pg['actions'][action] = empty
            else:
                raise Exception("Unknown status '%s'" % (status))

    def get_stats(self, action):
        stats = {}
        for osd in self.scrub_stats:
            pgs = self.scrub_stats[osd]['pgs']
            for pg in pgs:
                for s in pgs[pg]['shelved_actions'][action]:
                    day = s['end'].day
                    month = s['end'].month
                    if str(month) == self.month:
                        if day in stats:
                            stats[day]['count'] += 1
                            stats[day]['osds'].append(osd)
                            stats[day]['pgs'].append(pg)
                        else:
                            stats[day] = {'count': 1,
                                          'osds': [osd],
                                          'pgs': [pg]}

        return sorted(stats.keys()), stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default=None, required=True)
    parser.add_argument('--month', type=str, default=None, required=True)
    args = parser.parse_args()

    filter = (r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9]"
              "[0-9][0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:"
              "[0-9]+:[0-9]+\.[0-9]*).+ : ([0-9]*\."
              "[0-9]*[a-z]*.+)")
    keywords = '" scrub | deep-scrub "'
    collection = CephScrubStatsCollection(args,
                                          get(args.path, keywords, filter))
    collection.parse()

    print "%s OSDs" % len(collection.scrub_stats)

    PGs = []
    for osd in collection.scrub_stats:
        PGs += collection.scrub_stats[osd]['pgs'].keys()

    print "%s PGs" % len(set(PGs))

    scrubs = 0
    for osd in collection.scrub_stats:
        pgs = collection.scrub_stats[osd]['pgs']
        for pg in pgs:
            scrubs += len(pgs[pg]['shelved_actions']['scrub'])

    print "%s scrubs" % scrubs

    deepscrubs = 0
    for osd in collection.scrub_stats:
        pgs = collection.scrub_stats[osd]['pgs']
        for pg in pgs:
            deepscrubs += len(pgs[pg]['shelved_actions']['deep-scrub'])

    print "%s deep-scrubs" % deepscrubs

    print "\nStats for the last month (%s):" % (args.month)

    def osd_most_pg_scrubs(day, month, action, osd=None):
        highest = None
        if not osd:
            for osd in collection.scrub_stats:
                stat = osd_most_pg_scrubs(day, month, action, osd)
                if not highest or highest[0] < stat:
                    highest = [stat, osd]

            return "%s(%s)" % (highest[1], highest[0])
        else:
            total = 0
            pgs = collection.scrub_stats[osd]['pgs']
            for pg in pgs:
                for s in pgs[pg]['shelved_actions'][action]:
                    _day = s['end'].day
                    _month = s['end'].month
                    if str(_month) == str(month) and str(_day) == str(day):
                        total += 1

            return total

    keys, stats = collection.get_stats('scrub')
    data = ["\n    %s - %s (osds:%s, pgs:%s, mostscrubs:%s)" %
            (k, stats[k]['count'], len(set(stats[k]['osds'])),
             len(set(stats[k]['pgs'])),
             osd_most_pg_scrubs(k, args.month, 'scrub')) for k in keys]
    print "\n  No. scrubs by day: %s" % ' '.join(data)

    keys, stats = collection.get_stats('deep-scrub')
    data = ["\n    %s - %s (osds:%s, pgs:%s, mostscrubs:%s)" %
            (k, stats[k]['count'], len(set(stats[k]['osds'])),
             len(set(stats[k]['pgs'])),
             osd_most_pg_scrubs(k, args.month, 'deep-scrub')) for k in keys]
    print "\n  No. deep-scrubs by day: %s" % ' '.join(data)

    print "\n  Repeated deep-scrubs:"
    if collection.repeats:
        for r in collection.repeats:
            print "    %s repeated %s times on osd %s" % (r['pg'], r['count'],
                                                          r['osd'])
    else:
        print "No repeated deep-scrubs detected"

    print ""
