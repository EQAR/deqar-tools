#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
import json
import sys

from xml.etree import ElementTree
from collections import Counter

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def fixErasmus(code):
    """
    check Erasmus code syntax and fix up, if possible
    """

    if re.match(r'([A-Z]  |[A-Z]{2} |[A-Z]{3})[A-Z-]{1,8}[0-9]{2}', code):
        return code

    fixable = re.match(r'([A-Z]{1,3}) +([A-Z-]{1,8}) *([0-9]{1,2})', code)

    if fixable:
        fixed = '{:3}{}{:02d}'.format(fixable.group(1), fixable.group(2), int(fixable.group(3)))
        print("!!! '{}' fixed up to '{}'".format(code, fixed), file=sys.stderr)
        return fixed

    return None

class EwpRegistry:
    # Registry URL
    ewpRegistry = 'https://registry.erasmuswithoutpaper.eu/catalogue-v1.xml' # production
    #ewpRegistry = 'https://dev-registry.erasmuswithoutpaper.eu/catalogue-v1.xml' # test
    tagContainer = '{https://github.com/erasmus-without-paper/ewp-specs-api-registry/tree/stable-v1}institutions'
    tagInstitution = '{https://github.com/erasmus-without-paper/ewp-specs-api-registry/tree/stable-v1}hei'
    tagIdentifiers = '{https://github.com/erasmus-without-paper/ewp-specs-api-registry/tree/stable-v1}other-id'

    institutions = None

    idmap = {
        None: 'SCHAC',
        'eche': 'Erasmus-Charter',
        'erasmus': 'Erasmus',
        'pic': 'EU-PIC'
    }

    def __init__(self):
        # get EWP Registry
        ewpSession = requests.Session()
        ewpSession.headers.update({
            'user-agent': 'deqar-fetchEwpRegistry/0.1 ' + ewpSession.headers['User-Agent'],
            'accept': 'application/xml'
        })
        print("Fetching EWP Registry...", end='', file=sys.stderr)
        registry = ewpSession.get(self.ewpRegistry)
        print("Done.", file=sys.stderr)

        print("Parsing EWP Registry...", end='', file=sys.stderr)
        root = ElementTree.fromstring(registry.text)
        # <institutions> contains the mappings
        self.institutions = root.find(self.tagContainer).iter(self.tagInstitution)
        if self.institutions is None:
            raise Exception(f"Container {self.tagContainer} not found.")

    def __iter__(self):
        return(self)

    def __next__(self):
        hei = self.institutions.__next__()
        ret = { self.idmap[None]: hei.get('id') }
        for other in hei.findall(self.tagIdentifiers):
            if other.get('type') in self.idmap.keys():
                ret[self.idmap[other.get('type')]] = other.text
        return(ret)


class EufApi:
    # URL
    eufUrl = "https://hei.api.uni-foundation.eu/api/public/v1/hei"

    current = None

    idmap = {
        None: 'SCHAC',
        'erasmus-charter': 'Erasmus-Charter',
        'erasmus': 'Erasmus',
        'pic': 'EU-PIC'
    }

    def __init__(self):
        # get EUF HEI list
        self.eufSession = requests.Session()
        self.eufSession.headers.update({
            'user-agent': 'deqar-fetchEwpRegistry/0.1 ' + self.eufSession.headers['User-Agent'],
            'accept': 'application/json'
        })

        self.heis = iter(self.eufSession.get(self.eufUrl).json()['data'])

    def _iter_country(self):
        c = self.countries.__next__()
        self.current = iter(self.eufSession.get(f"{c['links']['list']['href']}").json()['data'])

    def __iter__(self):
        return(self)

    def __next__(self):
        hei = self.heis.__next__()

        ret = { self.idmap[None]: hei['id'] }

        if type(hei['attributes']['other_id']) == list:
            ids = hei['attributes']['other_id']
        elif type(hei['attributes']['other_id']) == dict:
            ids = [ hei['attributes']['other_id'] ]
        else:
            ids = [ ]
        for i in ids:
            if i['type'] in self.idmap.keys():
                ret[self.idmap[i['type']]] = i['value']

        return(ret)

"""
work starts here
"""

#source = EwpRegistry()
source = EufApi()

session = requests.Session()
session.headers.update({
    'user-agent': 'deqar-fetchEwpRegistry/0.1 ' + session.headers['User-Agent'],
    'accept': 'application/json'
})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
session.mount('https://', HTTPAdapter(max_retries=retries))

stats = Counter()

output = list()

try:
    count = 0
    for hei in source:
        count += 1
        print(hei, file=sys.stderr)
        if 'EU-PIC' in hei:
            query = session.post("https://ec.europa.eu/info/funding-tenders/opportunities/api/organisation/search.json", json={ 'pic': hei['EU-PIC'] })
            if query.status_code == requests.codes.ok:
                found = query.json()
                if len(found) == 1:
                    vat = found[0]['vat']
                    if vat is None or vat == '.' or vat == 'not applicable':
                        stats['PIC code found, but no VAT'] += 1
                    else:
                        hei['EU-VAT'] = vat
                        stats['PIC code found, VAT present'] += 1
                elif len(found) == 0:
                    stats['PIC code not found'] += 1
                else:
                    stats['PIC code got multiple results - odd!'] += 1
            else:
                stats['PIC code malformed or similar'] += 1

        if 'Erasmus' in hei:
            hei['Erasmus'] = fixErasmus(hei['Erasmus'])
            stats['Erasmus code'] += 1
            output.append(hei)
        else:
            stats['No Erasmus code'] += 1
    print(f'{count} HEIs')

except KeyboardInterrupt:
    pass

json.dump(output, sys.stdout)

print(stats, file=sys.stderr)

