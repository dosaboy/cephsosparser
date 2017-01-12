#!/usr/bin/python2
import argparse
import datetime
import re

from common import get
from sphinx.application import events


class CephSuicideStatsCollection(object):
    def __init__(self, args, events):
        self.suicide_stats = {}
        self.args = args
        self.month = args.month
        self.events = events

    def parse(self):
        for event in self.events:
            osd = event['data'].group(1)
            t = datetime.datetime.strptime(event['data'].group(2),
                                           '%Y-%m-%d %H:%M:%S.%f')
            timeout = event['data'].group(3)

            if osd in self.suicide_stats:
                self.suicide_stats[osd]['suicides'].append((t, timeout))
            else:
                self.suicide_stats[osd] = {'suicides': [(t, timeout)],
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
                t = s[0]
                if str(t.month) == args.month:
                    if t.day not in day_counters:
                        day_counters[t.day] = {}

                    if osd not in day_counters[t.day]:
                        day_counters[t.day][osd] = 0

                    if t.day in stats:
                        stats[t.day]['count'] += 1
                    else:
                        stats[t.day] = {'count': 1, 'maxosd': None}

                    day_counters[t.day][osd] += 1

        for day in day_counters:
            _max = None
            for osd in day_counters[day]:
                if not _max or _max[1] < day_counters[day][osd]:
                    _max = (osd, day_counters[day][osd])

            stats[day]['maxosd'] = _max[0]

        return sorted(stats.keys()), stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default=None, required=True)
    parser.add_argument('--month', type=str, default=None, required=True)
    args = parser.parse_args()

    keywords = '"had suicide timed out"'
    filter = (r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9][0-9]"
              "[0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:[0-9]+:"
              "[0-9]+\.[0-9]*).+had suicide timed out "
              "after (.+)")

    collection = CephSuicideStatsCollection(args,
                                            get(args.path, keywords, filter))
    collection.parse()

    print "%s OSDs" % len(collection.suicide_stats)

    suicides = []
    for osd in collection.suicide_stats:
        suicides += collection.suicide_stats[osd]['suicides']
    print "%s Suicides" % len(suicides)

    keys, stats = collection.get_stats()
    data = ["\n    %s - %s (maxosd=%s, host=%s)" %
            (k, stats[k]['count'], stats[k]['maxosd'],
             collection.suicide_stats[stats[k]['maxosd']]['host'])
            for k in keys]
    print "\n  No. suicides by day: %s" % ' '.join(data)

    print ""
