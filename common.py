import re

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

