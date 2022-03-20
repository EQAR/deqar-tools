#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from deqarclient import EqarApi, DataError
import os
import argparse
import csv
import coloredlogs

parser = argparse.ArgumentParser()
parser.add_argument("FILE", help="CSV file to import")
parser.add_argument("-b", "--base", help="Base URL to the DEQAR admin API (can also be set as DEQAR_BASE environment variable)")
parser.add_argument("--direct", help="post institution records directly (otherwise, whole file is read first)",
                    action="store_true")
parser.add_argument("-o", "--output", help="create CSV file with DEQARINST IDs of newly added institutions")
parser.add_argument("-i", "--ignore", help="ignore data errors: skip input line instead of raising an exception",
                    action="store_true")
parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="store_true")
args = parser.parse_args()

if args.verbose:
    coloredlogs.install(level='DEBUG')
else:
    coloredlogs.install(level='INFO')

if args.base:
    api = EqarApi(args.base)
elif 'DEQAR_BASE' in os.environ and os.environ['DEQAR_BASE']:
    api = EqarApi(os.environ['DEQAR_BASE'])
else:
    raise Exception("Base URL needs to be passed as argument or in DEQAR_BASE environment variable")

# will hold the institutions to be added (unless --direct is used)
institutions = list()

with open(args.FILE, newline='', encoding='utf-8-sig') as infile:

    inreader = csv.DictReader(infile)

    if args.output:
        outfile = open(args.output, 'w', newline='', encoding='utf-8-sig')
        outfields = inreader.fieldnames.copy()
        outfields.reverse()
        outfields.append('deqar_id')
        outfields.reverse()
        outwriter = csv.DictWriter(outfile, fieldnames=outfields)
        outwriter.writeheader()

    for data in inreader:

        try:

            print('#{}'.format(inreader.line_num), end='')

            institution = api.create_institution(data)

            if args.direct:
                # in direct-post mode, we upload immediately
                data['deqar_id'] = institution.post()
                if args.output:
                    outwriter.writerow(data)
            else:
                # otherwise, add to list for later commit
                institutions.append((institution, data))
                print("(queued) {}".format(institution))

        except DataError as data_error:
            if args.ignore:
                print("-- {} -- skipped line".format(data_error))
            else:
                raise

if not args.direct:
    # in non-direct mode, commit the whole list now
    yn = input('Commit? > ')
    if len(yn) > 0 and yn[0].upper() == 'Y':
        for (institution, data) in institutions:
            data['deqar_id'] = institution.post()
            if args.output:
                outwriter.writerow(data)

if args.output:
    outfile.close()

