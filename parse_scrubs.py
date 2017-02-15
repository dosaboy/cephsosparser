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
import copy
import datetime

from common import get


class CephScrubStatsCollection(object):
    def __init__(self, month, events):
        self.month = month
        self.aggrs_by_osd = {}
        self.aggrs_by_date = {}
        self.epoc = None
        self.scrub_stats = {'osds': {}, 'pgs': {}}
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
            empty_actions = {'scrub': empty,
                             'deep-scrub': copy.deepcopy(empty)}
            empty_pg = {pg: {'actions': empty_actions,
                             'shelved_actions': {'deep-scrub': [],
                                                 'scrub': []}}}

            if osd not in self.scrub_stats['osds']:
                self.scrub_stats['osds'][osd] = {'pgs': empty_pg}
            elif pg not in self.scrub_stats['osds'][osd]['pgs']:
                self.scrub_stats['osds'][osd]['pgs'].update(empty_pg)

            _pg = self.scrub_stats['osds'][osd]['pgs'][pg]
            if pg not in self.scrub_stats['pgs']:
                self.scrub_stats['pgs'][pg] = {osd: _pg}
            else:
                self.scrub_stats['pgs'][pg][osd] = _pg

            pg_action = _pg['actions'][action]
            if status == "starts":
                pg_action['start'] = t
            elif status == "ok":
                if not pg_action['start']:
                    # Ignore this event since it probably started before the
                    # beginning of the current analysis window
                    continue

                pg_action['end'] = t
                pg_action['length'] = t - pg_action['start']
                if all(pg_action):
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
                    actions.append(pg_action)
                    _pg['actions'][action] = copy.deepcopy(empty)

            else:
                raise Exception("Unknown status '%s'" % (status))

    def get_stats(self, action):
        stats = {}
        for osd in self.scrub_stats['osds']:
            pgs = self.scrub_stats['osds'][osd]['pgs']
            for pg in pgs:
                for s in pgs[pg]['shelved_actions'][action]:
                    action_start = s['start']
                    month = action_start.month
                    if int(month) == self.month:
                        day = action_start.day
                        if day in stats:
                            stats[day]['count'] += 1
                            stats[day]['osds'].append(osd)
                            stats[day]['pgs'].append(pg)
                            length = {'pg': pg, 'length': s['length']}
                            current_length = stats[day]['max_length']
                            if not current_length:
                                stats[day]['max_length'] = length
                            elif current_length['length'] < s['length']:
                                stats[day]['max_length'] = length
                        else:
                            stats[day] = {'count': 1,
                                          'osds': [osd],
                                          'pgs': [pg],
                                          'max_length': None}

        return sorted(stats.keys()), stats

    def osd_most_pg_scrubs(self, day, action, osd=None):
        highest = []
        if not osd:
            for osd in self.scrub_stats['osds']:
                stat = self.osd_most_pg_scrubs(day, action, osd)
                if not highest or highest[0] < stat:
                    highest = [stat, osd]

            return "%s(%s)" % (highest[1], highest[0])
        else:
            total = 0
            pgs = self.scrub_stats['osds'][osd]['pgs']
            for pg in pgs:
                for s in pgs[pg]['shelved_actions'][action]:
                    _day = s['end'].day
                    _month = s['end'].month
                    if (int(_month) == int(self.month) and
                            int(_day) == int(day)):
                        total += 1

            return total

    def day_longest_scrubaction(self, day, action):
        days = {}
        for pg in self.scrub_stats['pgs']:
            for pg_osd in self.scrub_stats['pgs'][pg]:
                _pg = self.scrub_stats['pgs'][pg][pg_osd]
                for s in _pg['shelved_actions'][action]:
                    if day != s['start'].day:
                        continue

                    if s['end'].day not in days:
                        days[s['end'].day] = {'length': s['length'], 'pg': pg}
                    elif days[s['end'].day]['length'] < s['length']:
                        days[s['end'].day] = {'length': s['length'], 'pg': pg}

        _day = days.get(day, {'pg': 'n/a', 'length': 'n/a'})
        return "pg=%s,length=%s" % (_day['pg'], _day['length'])

    @property
    def total_osds(self):
        osds = []
        for pg in collection.scrub_stats['pgs']:
            for pg_osd in collection.scrub_stats['pgs'][pg]:
                _pg = collection.scrub_stats['pgs'][pg]
                for action in ['scrub', 'deep-scrub']:
                    for event in _pg[pg_osd]['shelved_actions'][action]:
                        if event['start'].month == args.month:
                            if pg_osd not in osds:
                                osds.append(pg_osd)
        return len(osds)

    @property
    def total_pgs(self):
        pgs = []
        for pg in collection.scrub_stats['pgs']:
            for pg_osd in collection.scrub_stats['pgs'][pg]:
                _pg = collection.scrub_stats['pgs'][pg]
                for action in ['scrub', 'deep-scrub']:
                    for event in _pg[pg_osd]['shelved_actions'][action]:
                        if event['start'].month == args.month:
                            if pg not in pgs:
                                pgs.append(pg)

        return len(pgs)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default=None, required=True)
    parser.add_argument('--month', type=int, default=None, required=True)
    args = parser.parse_args()

    filter = (r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9]"
              "[0-9][0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:"
              "[0-9]+:[0-9]+\.[0-9]*).+ : ([0-9]*\."
              "[0-9]*[a-z]*.+)")
    keywords = '" scrub | deep-scrub "'
    collection = CephScrubStatsCollection(args.month,
                                          get(args.path, keywords, filter))
    collection.parse()
    print "Scrubbing stats for month %s:\n" % (args.month)

    print "%s OSDs scrubbed" % collection.total_osds
    print "%s PGs scrubbed" % collection.total_pgs

    scrubs = 0
    for pg in collection.scrub_stats['pgs']:
        for pg_osd in collection.scrub_stats['pgs'][pg]:
            _pg = collection.scrub_stats['pgs'][pg]
            scrubs += len(_pg[pg_osd]['shelved_actions']['scrub'])

    print "%s scrubs" % scrubs

    deepscrubs = 0
    for pg in collection.scrub_stats['pgs']:
        for pg_osd in collection.scrub_stats['pgs'][pg]:
            _pg = collection.scrub_stats['pgs'][pg]
            deepscrubs += len(_pg[pg_osd]['shelved_actions']['deep-scrub'])

    print "%s deep-scrubs" % deepscrubs

    days, stats = collection.get_stats('scrub')
    data = ["\n    %s - %s scrubs (osds:%s, pgs:%s, mostscrubs:%s, "
            "longest:%s))" %
            (day, stats[day]['count'], len(set(stats[day]['osds'])),
             len(set(stats[day]['pgs'])),
             collection.osd_most_pg_scrubs(day, 'scrub'),
             collection.day_longest_scrubaction(day, 'scrub')) for day in days]
    data = data or ["\n    none"]
    print "\n  No. scrubs by day: %s" % ' '.join(data)

    days, stats = collection.get_stats('deep-scrub')
    data = ["\n    %s - %s deep-scrubs (osds:%s, pgs:%s, mostscrubs:%s, "
            "longest:%s)" %
            (day, stats[day]['count'], len(set(stats[day]['osds'])),
             len(set(stats[day]['pgs'])),
             collection.osd_most_pg_scrubs(day, 'deep-scrub'),
             collection.day_longest_scrubaction(day, 'deep-scrub'))
            for day in days]
    data = data or ["\n    none"]
    print "\n  No. deep-scrubs by day: %s" % ' '.join(data)

    print "\n  Repeated deep-scrubs:"
    if collection.repeats:
        for r in collection.repeats:
            print "    %s repeated %s times on osd %s" % (r['pg'], r['count'],
                                                          r['osd'])
    else:
        print "    No repeated deep-scrubs detected"

    print ""
