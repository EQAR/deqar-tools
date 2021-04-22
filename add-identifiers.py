#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from collections import Counter
from institutions.models import Institution

stats = Counter()
ids = Counter()

for hei in json.load(sys.stdin):
    if 'Erasmus' in hei:
        erasmus = hei['Erasmus']
        del hei['Erasmus']
        try:
            pass
            deqar_hei = Institution.objects.get(institutionidentifier__identifier=erasmus, institutionidentifier__resource='Erasmus')
        except Institution.DoesNotExist:
            stats['Erasmus code, but not found'] += 1
        else:
            stats['Erasmus code, found'] += 1
            for resource, identifier in hei.items():
                deqar_hei.institutionidentifier_set.create(identifier=identifier, resource=resource)
                ids[resource] += 1

print(stats)
print(ids)

