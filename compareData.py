#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import click
import os
import argparse
import csv
import re
import json

def error(text):
    raise Exception("#{}: ".format(inreader.line_num) + text)

parser = argparse.ArgumentParser()
parser.add_argument("CSV", help="CSV file with reports in DEQAR")
parser.add_argument("JSON", help="JSON file to compare")
parser.add_argument("-o", "--outcsv", help="create CSV file with reports in DEQAR but not in JSON")
parser.add_argument("-j", "--outjson", help="create JSON file with reports in JSON but not in DEQAR")
args = parser.parse_args()

# read file
with open(args.JSON, 'r') as compfile:
    complist = json.loads(compfile.read())
    compdict = dict()
    for i in complist:
        #print("JSON: local={} agency={}".format(i['agency'], i['local_identifier']))
        compdict[i['local_identifier']] = i

csvonly = 0
jsononly = 0
common = 0

with open(args.CSV, newline='') as infile:

    inreader = csv.DictReader(infile)

    if args.outcsv:
        outfile = open(args.outcsv, 'w', newline='')
        outwriter = csv.DictWriter(outfile, fieldnames=inreader.fieldnames)
        outwriter.writeheader()

    for data in inreader:
        if data['report_local_identifier'] in compdict:
            del compdict[data['report_local_identifier']]
            common += 1
        else:
            if args.outcsv:
                outwriter.writerow(data)
            csvonly += 1
            if data['programme_name']:
                print("""Only in CSV:
    agency={report_agency}
    local_identifier={report_local_identifier}
    activity={report_esg_activity_long}
    institution={hei_name} ({hei_deqar_id})
    programme={programme_name}, {programme_qf_ehea_level}""".format(**data))
            else:
                print("""Only in CSV:
    agency={report_agency}
    local_identifier={report_local_identifier}
    activity={report_esg_activity_long}
    institution={hei_name} ({hei_deqar_id})""".format(**data))

for new in compdict.values():
    jsononly += 1
    if 'programmes' in new and new['programmes']:
        print("""Only in JSON:
    agency={agency}
    local_identifier={local_identifier}
    activity={activity}
    institution={institutions[0][eter_id]}
    programme={programmes[0][name_primary]}, {programmes[0][qf_ehea_level]}""".format(**new))
    else:
        print("""Only in JSON:
    agency={agency}
    local_identifier={local_identifier}
    activity={activity}
    institution={institutions[0][eter_id]}""".format(**new))

print("""
---------------------------
Summary:
 {} records in common
 {} records only in CSV
 {} records only in JSON
""".format(common,csvonly,jsononly))

if args.outcsv:
    outfile.close()

if args.outjson:
    with open(args.outjson, 'w') as outjson:
        json.dump(list(compdict.values()), outjson, indent='\t')

