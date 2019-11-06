#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import deqar
from tabulate import tabulate
from transliterate import translit
import click
import os
import argparse
import csv
import re
import json
import requests

def error(text):
    raise Exception("#{}: ".format(inreader.line_num) + text)

def post_and_log(institution, data):
    response = api.post('/adminapi/v1/institutions/', institution)
    institution['deqar_id'] = "DEQARINST{:04d}".format(response['id'])
    data['deqar_id'] = institution['deqar_id']
    print("{0[deqar_id]}: {0[name_primary]} ({0[website_link]}, {1[name_english]})".format(institution, countries.get(institution['countries'][0]['country'])))
    if args.output:
        outwriter.writerow(data)

parser = argparse.ArgumentParser()
parser.add_argument("FILE", help="CSV file to import")
parser.add_argument("-b", "--base", help="Base URL to the DEQAR admin API (can also be set as DEQAR_BASE environment variable)")
parser.add_argument("--direct", help="post institution records directly (otherwise, whole file is read first)",
                    action="store_true")
parser.add_argument("-o", "--output", help="create CSV file with DEQARINST IDs of newly added institutions")
parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="store_true")
args = parser.parse_args()

if args.base:
    api = deqar.EqarApi(args.base)
elif 'DEQAR_BASE' in os.environ and os.environ['DEQAR_BASE']:
    api = deqar.EqarApi(os.environ['DEQAR_BASE'])
else:
    raise Exception("Base URL needs to be passed as argument or in DEQAR_BASE environment variable")

try:
    print("Loading lists: countries ", end='')
    countries = deqar.Countries(api)
    print(" - levels ", end='')
    levels = deqar.QfEheaLevels(api)
    print(" - Done.")
except (requests.exceptions.ConnectionError):
    raise Exception("Failed to load country or level list.")

# will hold the institutions to be added (unless --direct is used)
institutions = list()

with open(args.FILE, newline='') as infile:

    inreader = csv.DictReader(infile)

    if args.output:
        outfile = open(args.output, 'w', newline='')
        outfields = inreader.fieldnames.copy()
        outfields.reverse()
        outfields.append('deqar_id')
        outfields.reverse()
        outwriter = csv.DictWriter(outfile, fieldnames=outfields)
        outwriter.writeheader()

    for data in inreader:
        # check if name and website present
        if not ( 'name_official' in data and data['name_official'] and 'website_link' in data and data['website_link']):
            error("Institution must have an official name and a website.")

        # determine primary name
        name_primary = data['name_english'] if 'name_english' in data and data['name_english'] else data['name_official']

        if args.verbose:
            print('\n{}: {}:'.format(inreader.line_num, name_primary))
            if 'name_english' in data and data['name_english']:
                print('- English name given, used as primary')
            else:
                print('- No English name, used official name as primary')
            print('- webiste={}'.format(data['website_link']))

        # resolve country ISO to ID if needed
        if 'country_id' in data and data['country_id']:
            which = (data['country_id'], 'ID')
        elif 'country_iso' in data and data['country_iso']:
            which = (data['country_iso'], 'ISO code')
        else:
            error("Country needs to be specified")

        country = countries.get(which[0])
        if not country:
            error("Unknown country {} [{}]".format(which[1], which[0]))
        elif args.verbose:
            print('- country={}/{} ({} [{}] specified)'.format(country['id'],country['name_english'],which[1],which[0]))

        # basic record
        institution = dict(
            name_primary=name_primary,
            website_link=data['website_link'],
            names=[ { 'name_official': data['name_official'] }],
            countries=[ { 'country': country['id'] } ],
            flags=[ ]
        )

        # sanity check names
        if 'name_english' in data and data['name_english'] == data['name_official']:
            print("- !!! DUPLICATE NAME: English name [{}] identical to official name.".format(data['name_english']))
            del data['name_english']
        if 'name_version' in data and data['name_version'] == data['name_official']:
            print("- !!! DUPLICATE NAME: Name version [{}] identical to official name.".format(data['name_version']))
            del data['name_version']
        if 'name_version' in data and 'name_english' in data and data['name_version'] and data['name_version'] == data['name_english']:
            print("- !!! DUPLICATE NAME: Name version [{}] identical to English name.".format(data['name_version']))
            del data['name_version']

        # add optional attributes
        if 'name_english' in data and data['name_english']:
            institution['names'][0]['name_english'] = data['name_english']
        if 'name_official_transliterated' in data and data['name_official_transliterated']:
            if data['name_official_transliterated'][0] == '*':
                institution['names'][0]['name_official_transliterated'] = translit(data['name_official'], data['name_official_transliterated'][1:3], reversed=True)
                if args.verbose:
                    print("- transliterated '{}' -> '{}'".format(data['name_official'], institution['names'][0]['name_official_transliterated']))
            else:
                institution['names'][0]['name_official_transliterated'] = data['name_official_transliterated']
        if 'name_version' in data and data['name_version']:
            institution['names'][0]['alternative_names'] = [ { 'name': data['name_version'] } ]
        if 'acronym' in data and data['acronym']:
            institution['names'][0]['acronym'] = data['acronym']
        if 'city' in data and data['city']:
            institution['countries'][0]['city'] = data['city']
        if 'founding_date' in data and data['founding_date']:
            institution['founding_date'] = data['founding_date']

        # process identifier
        if 'identifier' in data and data['identifier']:
            institution['identifiers'] = [ { 'identifier': data['identifier'] } ]
            if 'resource' not in data and 'agency_id' not in data:
                error("Identifier needs to have an agency ID or a resource.")
            if 'resource' in data:
                institution['identifiers'][0]['resource'] = data['resource']
                if args.verbose:
                    print('- identifier [{}] with resource [{}]'.format(data['identifier'], data['resource']))
            else:
                institution['identifiers'][0]['resource'] = 'local identifier'
                if args.verbose:
                    print('- identifier [{}] as local identifier'.format(data['identifier']))
            if 'agency_id' in data:
                institution['identifiers'][0]['agency'] = data['agency_id']
                if args.verbose:
                    print('  linked to agency ID [{}]'.format(data['agency_id']))

        # process parent institution
        if 'parent_id' in data and data['parent_id']:
            institution['hierarchical_parent'] = [ { 'institution': data['parent_id'] } ]
            if args.verbose:
                print('- hierarchical parent [{}] (numeric)'.format(data['parent_id']))
        elif 'parent_deqar_id' in data and data['parent_deqar_id']:
            match = re.match('\s*DEQARINST([0-9]+)\s*', data['parent_deqar_id'].upper())
            if match:
                institution['hierarchical_parent'] = [ { 'institution': int(match.group(1)) } ]
                if args.verbose:
                    print('- hierarchical parent [{}] (from DEQAR ID [{}])'.format(int(match.group(1)), data['parent_deqar_id']))
            else:
                error('Malformed parent_deqar_id: [{}]'.format(data['parent_deqar_id']))

        # process QF levels
        if 'qf_ehea_levels' in data and data['qf_ehea_levels']:
            if args.verbose:
                print('- QF-EHEA levels given as [{}], translated to:'.format(data['qf_ehea_levels']))
            institution['qf_ehea_levels'] = list()
            for l in re.split(r'\s*[\,\.\s]\s*', data['qf_ehea_levels'].strip(" ,.\n\r\t\v\f")):
                if l.isdigit() and int(l) > 4:
                    l = int(l) - 5
                level = levels.get(l)
                if not level:
                    error('QF-EHEA level [{}] not recognised.'.format(l))
                institution['qf_ehea_levels'].append({ 'qf_ehea_level': level['id'] })
                if args.verbose:
                    print('  [{}]=>{}/{}'.format(l, level['id'], level['level']))

        if args.direct:
            # in direct-post mode, we upload immediately
            post_and_log(institution, data)
        else:
            # otherwise, add to list for later commit
            institutions.append((institution, data))
            print("queued({}): {} ({}, {})".format(inreader.line_num, institution['name_primary'], institution['website_link'], country['name_english']))


if not args.direct:
    # in non-direct mode, commit the whole list now
    yn = input('Commit? > ')
    if len(yn) > 0 and yn[0].upper() == 'Y':
        for (institution, data) in institutions:
            post_and_log(institution, data)

if args.output:
    outfile.close()

