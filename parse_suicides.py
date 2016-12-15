#!/usr/bin/python2
import argparse
import datetime
import re

from common import get_hostname_from_path
from subprocess import check_output, CalledProcessError

suicide_stats = {}
events = []
MONTH = '11'


def get(path):
    out = check_output(['find', path, '-type', 'f', '-name', 'ceph*'])
    paths = [path for path in out.split('\n')
             if re.search('var/log/ceph/ceph-osd.+', path)]
    for path in paths:
        hostname = get_hostname_from_path(path)
        try:
            cmd = ['zgrep', '-EHi', '"had suicide timed out"', path]
            out = check_output(' '.join(cmd), shell=True)
            for line in out.split('\n'):
                if not re.search(":\s+-[0-9]*>", line):
                    res = re.search(".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9][0-9]"
                                    "[0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:[0-9]+:"
                                    "[0-9]+\.[0-9]*).+had suicide timed out "
                                    "after (.+)", line)
                    if res:
                        events.append({'host': hostname, 'data': res})

        except CalledProcessError:
            pass


def parse():
    global suicide_stats
    for event in events:
        osd = event['data'].group(1)
        t = datetime.datetime.strptime(event['data'].group(2),
                                       '%Y-%m-%d %H:%M:%S.%f')
        timeout = event['data'].group(3)

        if osd in suicide_stats:
            suicide_stats[osd]['suicides'].append((t, timeout))
        else:
            suicide_stats[osd] = {'suicides': [(t, timeout)],
                                  'host': event['host']}

def get_osds_by_host():
    hosts = {}
    for osd in suicide_stats:
        host = suicide_stats[osd]['host']
        if host in hosts:
            hosts[host].append(osd)
        else:
            hosts[host] = [osd]

    for host in hosts:
        hosts[host] = sorted(hosts[host])

    return hosts


def get_stats():
    stats = {}
    day_counters = {}
    for osd in suicide_stats:
        for s in suicide_stats[osd]['suicides']:
            t = s[0]
            if t.day not in day_counters:
                day_counters[t.day] = {}

            if osd not in day_counters[t.day]:
                day_counters[t.day][osd] = 0

            if str(t.month) == MONTH:
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
    args = parser.parse_args()

    get(args.path)
    parse()

    print "%s OSDs" % len(suicide_stats)

    suicides = []
    for osd in suicide_stats:
        suicides += suicide_stats[osd]['suicides']
    print "%s Suicides" % len(suicides)

    keys, stats = get_stats()
    data = ["\n    %s - %s (maxosd=%s, host=%s)" %
            (k, stats[k]['count'], stats[k]['maxosd'],
             suicide_stats[stats[k]['maxosd']]['host']) for k in keys]
    print "\n  No. suicides by day: %s" % ' '.join(data)

    print ""
