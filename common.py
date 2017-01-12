import re

from subprocess import check_output, CalledProcessError


def get_hostname_from_path(path):
    hostname = 'unknown'
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
