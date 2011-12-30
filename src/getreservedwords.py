#!/usr/bin/env python

"""Get a list of reserved words in the most recent versions of
PostgreSQL"""

import subprocess
import sys
import urllib2

KEYWORDURLBASE = 'http://www.postgresql.org/docs/%s/static/sql-keywords-appendix.html'
PGVERSIONS = ('8.0', '8.1', '8.2', '8.3', '8.4', '9.0', '9.1')

def getreservedwords(url):
    """Given the URL of a PostgreSQL webpage listing reserved
    keywords, yield each of the keywords on that page"""
    print >> sys.stderr, 'Fetching', url
    tidy = subprocess.Popen(('tidy'),
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    tidy.stdin.write(urllib2.urlopen(url).read())
    tidy.stdin.close()

    # Skip to the table of reserved words
    while True:
        if 'CALSTABLE' in tidy.stdout.readline():
            break

    # Keep going until we run out of lines or we exit the table
    while True:
        while True:
            line = tidy.stdout.readline()
            if not line or line.startswith('</tbody>'):
                return
            if line.startswith('<tr>'):
                break
        key = tidy.stdout.readline()
        # Only look at <tt class="TOKEN"> rows
        if not 'TOKEN' in key:
            continue
        if not tidy.stdout.readline().startswith('<td>reserved</td>'):
            continue
        # Print just the column name
        yield key.split('>')[2].split('<')[0].lower()


if __name__ == '__main__':
    reservedwords = {}
    for version in PGVERSIONS:
        for reservedword in getreservedwords(KEYWORDURLBASE % version):
            try:
                reservedwords[reservedword].append(version)
            except KeyError:
                reservedwords[reservedword] = [version]

    for reservedword, versions in sorted(reservedwords.items()):
        print '    "%s",%s/* PostgreSQL version%s %s */' % (
            reservedword,
            ' ' * (max(0, 20 - len(reservedword))),
            's' if len(versions) > 1 else '',
            ', '.join(versions))
