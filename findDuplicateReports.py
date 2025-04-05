#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from deqarclient.api import EqarApi
from deqarclient.errors import DataError
from deqarclient.auth import EqarApiInteractiveAuth

import os
import argparse
import csv
import logging
import coloredlogs

import collections
import json
import hashlib

import tracemalloc

PAGESIZE = 2500

class DuplicateSets:
    """
    collects sets of duplicates
    """

    def __init__(self):
        self.store = collections.defaultdict(lambda: collections.defaultdict(set))

    def add(self, report):
        h = self._hash_report(report)
        self.store[report["agency_acronym"]][h].add(report["id"])

    def _hash_report(self, report):
        """
        core logic: generate a "hash" string from Report object - reports with identical
        hash are considered possibly unique
        """
        fields = (
            sorted([ hei["id"] for hei in report["institutions"] ]),
            sorted([ f"{prog['name_primary']}|{prog['qf_ehea_level']}|{prog['nqf_level']}|{prog['programme_type']}|{prog['workload_ects']}" for prog in report["programmes"] ]),
            report["agency_esg_activity"],
            report["valid_from"],
            report.get("valid_to", None),
            report["decision"],
        ) 
        return hashlib.md5(json.dumps(fields, sort_keys=True).encode()).hexdigest()

    def __iter__(self):
        return iter(self.store.items())

    def __len__():
        return len(self.store)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("PATH",
                        help="path where to save CSV files",
                        nargs="?")
    parser.add_argument("-b", "--base",
                        help="Base URL to the DEQAR admin API (can also be set as DEQAR_BASE environment variable)")
    parser.add_argument("-n", "--dry-run",
                        help="do not save CSV files, simply output statistics",
                        action="store_true")
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-t", "--trace",
                        help="trace memory use with tracemalloc",
                        action="store_true")
    parser.add_argument("-a", "--agency",
                        help="check specific agency only")
    args = parser.parse_args()

    if args.trace:
        tracemalloc.start()

    if args.verbose:
        coloredlogs.install(level='DEBUG')
    else:
        coloredlogs.install(level='INFO')
    logger = logging.getLogger(__name__)

    if not (args.PATH or args.dry_run):
        raise Exception("You need to either provide a path to save CSV files or -n/--dry-run")

    if args.base:
        api = EqarApi(args.base, authclass=EqarApiInteractiveAuth)
    elif 'DEQAR_BASE' in os.environ and os.environ['DEQAR_BASE']:
        api = EqarApi(os.environ['DEQAR_BASE'], authclass=EqarApiInteractiveAuth)
    else:
        raise Exception("Base URL needs to be passed as argument or in DEQAR_BASE environment variable")

    offset = 0
    total = 1

    sets = DuplicateSets()

    n_sets = 0
    n_dups = 0
    n_read = 0

    if args.trace:
        current, peak = tracemalloc.get_traced_memory()
        logger.info(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    kwargs = {}
    if args.agency:
        kwargs['agency'] = args.agency

    try:
        while offset < total:
            logger.info(f"Checking report {offset}-{offset+PAGESIZE-1} of {total}")
            response = api.get("/webapi/v2/browse/reports/", offset=offset, limit=PAGESIZE, **kwargs)
            total = response['count']
            offset += PAGESIZE
            for r in response['results']:
                sets.add(r)
    except KeyboardInterrupt:
        pass

    if args.trace:
        current, peak = tracemalloc.get_traced_memory()
        logger.info(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    for (agency, agency_sets) in sorted(sets):
        this_sets = 0
        this_dups = 0
        if any(len(s) > 1 for s in agency_sets.values()):
            if args.dry_run:
                logger.info(f"- {agency}:")
                for (h, s) in agency_sets.items():
                    if len(s) > 1:
                        n_sets += 1
                        n_dups += len(s)
                        this_sets += 1
                        this_dups += len(s)
                        logger.debug(f"duplicate set: {', '.join([ str(i) for i in s])}")
            else:
                target = os.path.join(args.PATH, f'{agency}.csv')
                logger.info(f"- {agency}: writing to {target}")
                with open(target, 'w') as file:
                    writer = csv.DictWriter(file, fieldnames=[
                            "_hash",
                            "id",
                            "agency_acronym",
                            "local_identifier",
                            "agency_esg_activity__type",
                            "agency_esg_activity",
                            "institutions",
                            "programme__names",
                            "programme__level",
                            "programme__qualifications",
                            "programme__type",
                            "programme__workload_ects",
                            "valid_from",
                            "valid_to",
                            "status",
                            "decision",
                            "crossborder",
                            "files",
                            "flag",
                        ], extrasaction='ignore')
                    writer.writeheader()
                    for (h, s) in agency_sets.items():
                        if len(s) > 1:
                            n_sets += 1
                            n_dups += len(s)
                            this_sets += 1
                            this_dups += len(s)
                            logger.debug(f"duplicate set: {', '.join([ str(i) for i in s])}")
                            for report_id in s:
                                report = api.get(f"/webapi/v2/browse/reports/{report_id}/")
                                report['_hash'] = h
                                report['institutions'] = " | ".join([ f"{i['deqar_id']} {i['name_primary']}" for i in report['institutions'] ])
                                report['programme__names'] = " | ".join([ "/".join([ pn['name'] for pn in p['programme_names'] ]) for p in report['programmes'] ])
                                report['programme__qualifications'] = " | ".join([ "/".join([ pn['qualification'] for pn in p['programme_names'] ]) for p in report['programmes'] ])
                                report['programme__level'] = " | ".join([ f"{p['qf_ehea_level']} - {p['nqf_level']}" for p in report['programmes'] ])
                                report['programme__type'] = " | ".join([ p['programme_type'] for p in report['programmes'] ])
                                report['programme__workload_ects'] = " | ".join([ str(p['workload_ects']) for p in report['programmes'] ])
                                report['files'] = " | ".join([ f['file'] or f"[FILE MISSING: {f['file_display_name']}]" for f in report['report_files'] ])
                                writer.writerow(report)
            logger.info(f"  > {this_sets} sets with {this_dups} reports")

    logger.info(f'{n_dups} of {total} reports are possibly duplicates (in {n_sets} sets)')

    if args.trace:
        current, peak = tracemalloc.get_traced_memory()
        logger.info(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

        tracemalloc.stop()

