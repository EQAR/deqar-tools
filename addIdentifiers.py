#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import json
import re

from deqarclient import EqarApi, Institution, HttpError, DataError

from collections import Counter

class AddIdentifiers:

    """
    load additional identifiers from JSON file and add to database via Admin API
    """

    def __init__(self, api, id_list, key):
        self.api     = api              # EqarApi
        self.key     = key              # key (= identifier resource) to match records

        self.StatsDiscover = Counter()
        self.StatsAdded    = Counter()
        self.StatsSkipped  = Counter()

        # transform array of IDs into a dict, indexed by key
        self.AdditionalIds = {}
        for i in id_list:
            if self.key in i:
                self.AdditionalIds[i[self.key].strip()] = i
                del self.AdditionalIds[i[self.key].strip()][self.key]

    def _run_step(self, offset, limit, country=None, commit=False):

        heis = self.api.get('/connectapi/v1/institutions', offset=offset, limit=limit, country=country)

        for i in heis['results']:
            try:
                hei = Institution(api, i['id'])
            except HttpError:
                self.api._log(f"error loading {i}", level=self.api.ERROR)
                # this is most likely an index error: HEI deleted from DB, but still in index - simply skip
                continue

            self.api._log(str(hei), level=self.api.NOTICE)

            # find HEI in dict of additional IDs, using identifier with resource=key
            this_key = None
            for id in hei.data['identifiers']:
                if id['agency'] is None and id['resource'] == self.key:
                    if id['identifier'] in self.AdditionalIds:
                        this_key = id['identifier']
                        self.StatsDiscover['Identifier present in DEQAR, found in additional IDs'] += 1
                    else:
                        self.StatsDiscover['Identifier present in DEQAR, not found in additional IDs'] += 1
                    break
            else:
                self.StatsDiscover['Identifier not present in DEQAR'] += 1

            if this_key:
                # when HEI is found in additional IDs, we add them to DEQAR
                changed = False
                for resource, identifier in self.AdditionalIds[this_key].items():
                    resource = resource.strip()
                    identifier = identifier.strip()
                    for existing in hei.data['identifiers']:
                        if existing['agency'] is None and existing['resource'] == resource:
                            if existing['identifier'] == identifier:
                                # ... but make sure to skip duplicates
                                self.api._log(f"  = {resource}:{identifier} already present")
                                self.StatsSkipped[resource] += 1
                                break
                            else:
                                self.api._log(f"  ? {resource}:{existing['identifier']} already present, new {identifier} will be added", level=self.api.ERROR)
                    else:
                        self.api._log(f"  + add {resource}:{identifier}", level=self.api.GOOD)
                        hei.data['identifiers'].append(dict(
                            agency=None,
                            identifier=identifier,
                            resource=resource,
                            identifier_valid_from=hei.data.get('founding_date', '1970-01-01'),
                            note='EUF/EWP API 2021'
                        ))
                        self.StatsAdded[resource] += 1
                        changed = True
                        try:
                            if resource == 'SCHAC' and self.api.DomainChecker.core_domain(hei.data['website_link']) != identifier:
                                self.api._log(f"  - mismatch between SCHAC={identifier} and core_domain={self.api.DomainChecker.core_domain(hei.data['website_link'])} (extracted from {hei.data['website_link']})", level=self.api.WARN)
                        except DataError:
                            pass
                if changed and commit:
                    hei.save(comment="addIdentifiers.py - import from EUF/EWP API")

        return(heis['count'])

    def run(self, country=None, commit=False):
        offset = 0
        limit = 25
        count = 1

        self.api._log(f"Syncing identifiers: 0% (0/NA)\r", nl=False, level=self.api.WARN)
        while offset < count:
            count = self._run_step(offset, limit, country, commit)
            offset += limit
            self.api._log(f"Syncing identifiers: {round(100*min(offset,count)/count)}% ({min(offset,count)}/{count})\r", nl=False, level=self.api.WARN)

        self.api._log("", nl=True, level=self.api.WARN)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("FILE", help="JSON file with identifier data")
    parser.add_argument("-b", "--base", help="Base URL to the DEQAR admin API (can also be set as DEQAR_BASE environment variable)")
    parser.add_argument("--country", help="Country for which to run (default: all HEIs)")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-c", "--color", help="force ANSI color output even if not on terminal",
                        action="store_true")
    parser.add_argument("-n", "--dry-run", help="log, but do not commit any changes",
                        action="store_true")
    args = parser.parse_args()

    if args.base:
        api = EqarApi(args.base, verbose=args.verbose, color=True if args.color else None)
    elif 'DEQAR_BASE' in os.environ and os.environ['DEQAR_BASE']:
        api = EqarApi(os.environ['DEQAR_BASE'], verbose=args.verbose, color=True if args.color else None)
    else:
        raise Exception("Base URL needs to be passed as argument or in DEQAR_BASE environment variable")

    with open(args.FILE, encoding='utf-8-sig') as infile:
        ids = AddIdentifiers(api, json.load(infile), key='Erasmus')

    try:
        ids.run(country=args.country, commit=not args.dry_run)
    except KeyboardInterrupt:
        print()

    print("Discover: ", ids.StatsDiscover)
    print("Added:    ", ids.StatsAdded)
    print("Skipped:  ", ids.StatsSkipped)

