#!/usr/bin/python2
import argparse
import datetime
import re

from common import avg, get_hostname_from_path, uniq
from subprocess import check_output, CalledProcessError

MAX_TOPS = 10
events = []
aggrs_by_osd = {}
aggrs_by_date = {}
aggrs_by_host = {}
epoc = None
osd_stats = {}
mins = []
maxs = []
avgs = []
date_avgs = []
date_maxs = []


def get(path):
    out = check_output(['find', path, '-type', 'f', '-name', 'ceph*'])
    paths = [path for path in out.split('\n')
                if re.search('var/log/ceph/ceph-osd.+', path)]
    for path in paths:
        hostname = get_hostname_from_path(path)
        try:

            cmd = ['zgrep', '-EHi', '"slow requests"', path]
            out = check_output(' '.join(cmd), shell=True)
            for line in out.split('\n'):
                if not re.search(r":\s+-[0-9]*>", line):
                    res = re.search(r".+(ceph-osd\.[0-9]*)\.log.*:.*([0-9]"
                                    "[0-9][0-9][0-9]-[0-9]+-[0-9]+\s[0-9]+:"
                                    "[0-9]+:[0-9]+\.[0-9]*)\s.+blocked for > "
                                    "([0-9\.]*) secs", line)
                    if res:
                        events.append({'host': hostname, 'data': res})

        except CalledProcessError:
            pass


def parse():
    global osd_stats
    for event in events:
        osd = event['data'].group(1)
        t = datetime.datetime.strptime(event['data'].group(2),
                                       '%Y-%m-%d %H:%M:%S.%f')
        if osd in osd_stats:
            data = (t, float(event['data'].group(3)))
            osd_stats[osd]['slow_requests'].append(data)
        else:
            osd_stats[osd] = {'host': event['host'],
                              'slow_requests': [(t,
                                                 float(event['data'].group(3)))]}

def keep_top_avgs(osd, val):
    global avgs
    if not avgs:
        avgs.append((osd, val))
        return

    if val > max([e[1] for e in avgs]) or val > float(avgs[0][1]):
        avgs.append((osd, val))

    avgs = sorted(avgs, key=lambda e: float(e[1]))
    if len(avgs) > MAX_TOPS:
        avgs.pop(0)


def keep_top_mins(osd, val):
    global mins
    if not mins:
        mins.append((osd, val))
        return

    if val < min([e[1] for e in mins]) or val < float(maxs[-1][1]):
        mins.append((osd, val))

    mins = sorted(mins, key=lambda e: float(e[1]))
    if len(mins) > MAX_TOPS:
        mins.pop(MAX_TOPS)


def keep_top_maxs(osd, val):
    global maxs
    if not maxs:
        maxs.append((osd, val))
        return

    if val > max([e[1] for e in maxs]) or val > float(maxs[0][1]):
        maxs.append((osd, val))

    maxs = sorted(maxs, key=lambda e: float(e[1]))
    if len(maxs) > MAX_TOPS:
        maxs.pop(0)


def aggregate(osd):
    global aggrs_by_date, aggrs_by_osd, aggrs_by_host

    host = osd_stats[osd]['host']
    vals = osd_stats[osd]['slow_requests']
    for d, v in vals:
        if d in aggrs_by_date:
            aggrs_by_date[d].append((osd, v))
        else:
            aggrs_by_date[d] = [(osd, v)]

        if host not in aggrs_by_host:
            aggrs_by_host[host] = []

        aggrs_by_host[host].append(v)

    if osd not in aggrs_by_osd:
        aggrs_by_osd[osd] = vals
    else:
        aggrs_by_osd[osd] += vals


def date_avg():
    global aggrs_by_date, aggrs_by_osd, date_avgs
    for d in aggrs_by_date:
        a = avg([a[1] for a in aggrs_by_date[d]])
        date_avgs.append((d, a))


def date_max():
    global aggrs_by_date, aggrs_by_osd, date_maxs
    for d in aggrs_by_date:
        a = max([a[1] for a in aggrs_by_date[d]])
        date_maxs.append((d, a))


def day_highest_osd(d, key):
    equals = []
    _osd = None
    for osd in osd_stats:
        for e in osd_stats[osd]['slow_requests']:
            if str(e[0].day) == str(d):
                if not _osd or osd_stats[_osd][key] < osd_stats[osd][key]:
                    _osd = osd
                    equals = []
                elif osd_stats[_osd][key] == osd_stats[osd][key]:
                    equals.append(osd)

    return list(set([_osd] + equals))[0]


def total_slow_requests():
    total = 0
    for osd in osd_stats:
        total += len(osd_stats[osd]['slow_requests'])

    return total

def get_osds_by_host():
    hosts = {}
    for osd in osd_stats:
        host = osd_stats[osd]['host']
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

    get(args.path)
    parse()
    osds = list(osd_stats.keys())
    osds = sorted(osds, key=lambda s: float(s.partition('.')[2]))
    print "Slow request stats for %s OSDs" % len(osds)
    print "Total slow requests: %s" % total_slow_requests()

    for osd in osds:
        delays = [e[1] for e in osd_stats[osd]['slow_requests']]
        m = min(delays)
        keep_top_mins(osd, m)
        osd_stats[osd]['min'] = m
        m = max(delays)
        keep_top_maxs(osd, m)
        osd_stats[osd]['max'] = m
        a = avg(delays)
        keep_top_avgs(osd, a)
        osd_stats[osd]['avg'] = a
        aggregate(osd)

    date_avg()
    date_max()

    collection = {}
    for a in date_avgs:
        d = str(a[0].month)
        if d not in collection:
            collection[d] = [a[1]]
        else:
            collection[d].append(a[1])

    month_avgs = sorted([(d, avg(v)) for d, v in collection.iteritems()],
                        key=lambda e: e[0])

    collection = {}
    for a in date_avgs:
        d = str(a[0].day)
        if d not in collection:
            collection[d] = [a[1]]
        else:
            collection[d].append(a[1])

    day_avgs = sorted([(d, avg(v)) for d, v in collection.iteritems()],
                      key=lambda e: e[0])

    collection = {}
    for a in date_maxs:
        d = str(a[0].day)
        if d not in collection:
            collection[d] = [a[1]]
        else:
            collection[d].append(a[1])

    day_maxs = sorted([(d, max(v)) for d, v in collection.iteritems()],
                      key=lambda e: e[0])

    hosts = get_osds_by_host()
    for host in hosts:
        print "\n%s:" % host
        osds = sorted(hosts[host], key=lambda e: int(e.partition('.')[2]))
        data = ["    %s" % osd for osd in osds]
        print "%s" % '\n'.join(data)


    print "\nTop %s:" % MAX_TOPS
    data = ["\n      %s - %s (%s)" %
            (e[0], e[1], ' '.join(uniq([str(a[0]) for a in aggrs_by_osd[e[0]]
                                        if e[1] == a[1]]))) for e in mins]
    print "\n    Min Wait (s): %s" % ' '.join(data)


    data = ["\n      %s - %s (%s)" %
            (e[0], e[1], ' '.join(uniq([str(a[0]) for a in aggrs_by_osd[e[0]]
                                        if e[1] == a[1]]))) for e in maxs]
    print "\n    Max Wait (s): %s" % ' '.join(data)

    data = ["\n      %s - %d" %
            (host, int(sum(aggrs_by_host[host]))) for host in aggrs_by_host]
    print "\n    Wait By Host (s): %s" % ' '.join(data)

    data = ["\n      %s - %s" % (e[0], e[1]) for e in avgs]
    print "\n    Avg Wait (s): %s" % ' '.join(data)

    data = ["\n      %s - %s" % (e[0], e[1]) for e in month_avgs]
    print "\n    Avg Wait By Month (s): %s" % ' '.join(data)

    data = ["\n      %s - %s (max:%s)" %
            (e[0], e[1], day_highest_osd(e[0], 'avg')) for e in day_avgs]
    print "\n    Avg Wait By Day (s): %s" % ' '.join(data)

    data = ["\n      %s - %s (max:%s)" %
            (e[0], e[1], day_highest_osd(e[0], 'max')) for e in day_maxs]
    print "\n    Max Wait By Day (s): %s" % ' '.join(data)

    print ''
