# -*- coding: utf-8 -*-

import json
import os
import re
import logging

from getpass import getpass
from warnings import warn

import requests
from tldextract import TLDExtract

from .errors import *

class NewInstitution:

    """
    creates a new institution record from CSV input
    """

    def __init__(self, api, data):

        def csv_coalesce(*args):
            for column in args:
                if column in data and data[column]:
                    if isinstance(data[column], str):
                        return(data[column].strip())
                    else:
                        return(data[column])
            return(False)

        # save api for later use
        self.api = api

        # check if name and website present
        if not ( csv_coalesce('name_official') and csv_coalesce('website_link') ):
            raise DataError("Institution must have an official name and a website.")

        # determine primary name
        name_primary = csv_coalesce('name_english', 'name_official')

        self.api.logger.info('* {}:'.format(name_primary))
        if csv_coalesce('name_english'):
            self.api.logger.debug('  - English name given, used as primary')
        else:
            self.api.logger.debug('  - No English name, used official name as primary')
        self.api.logger.debug('  - webiste={}'.format(csv_coalesce('website_link')))

        # normalise website
        website = self._url_normalise(csv_coalesce('website_link'))
        data['website_link'] = website

        # check for duplicate by internet domain
        self.api.DomainChecker.query(csv_coalesce('website_link'))
        if self.api.DomainChecker.core_domain(website) != self.api.DomainChecker.core_domain(csv_coalesce('website_link')):
            self.api.DomainChecker.query(website)

        # resolve country ISO to ID if needed
        if csv_coalesce('country_id', 'country_iso', 'country'):
            which = csv_coalesce('country_id', 'country_iso', 'country')
            country = self.api.Countries.get(which)
            if not country:
                raise DataError("Unknown country [{}]".format(which))
            self.api.logger.debug('  - country [{}] resolved to {} (ID {})'.format(which,country['name_english'],country['id']))
        else:
            raise DataError("Country needs to be specified")

        # basic record
        self.institution = dict(
            name_primary=name_primary,
            website_link=website,
            names=[ { 'name_official': csv_coalesce('name_official') }],
            countries=[ { 'country': country['id'] } ],
            flags=[ ]
        )

        # sanity check names
        if 'name_english' in data and data['name_english'] == data['name_official']:
            warn(DataWarning("  - !!! DUPLICATE NAME: English name [{}] identical to official name.".format(data['name_english'])))
            del data['name_english']
        if 'name_version' in data and data['name_version'] == data['name_official']:
            warn(DataWarning("  - !!! DUPLICATE NAME: Name version [{}] identical to official name.".format(data['name_version'])))
            del data['name_version']
        if 'name_version' in data and 'name_english' in data and data['name_version'] and data['name_version'] == data['name_english']:
            warn(DataWarning("  - !!! DUPLICATE NAME: Name version [{}] identical to English name.".format(data['name_version'])))
            del data['name_version']

        self._query_name(csv_coalesce('name_official'))

        # add optional attributes
        if csv_coalesce('name_english'):
            self._query_name(csv_coalesce('name_english'))
            self.institution['names'][0]['name_english'] = csv_coalesce('name_english')
        if csv_coalesce('name_official_transliterated'):
            if data['name_official_transliterated'][0] == '*':
                try:
                    from transliterate import translit
                    self.institution['names'][0]['name_official_transliterated'] = translit(csv_coalesce('name_official'), data['name_official_transliterated'][1:3], reversed=True)
                    self.api.logger.info("  - transliterated '{}' -> '{}'".format(csv_coalesce('name_official'), self.institution['names'][0]['name_official_transliterated']))
                except ImportError:
                    warn(DataWarning("  - !!! transliteration to [{}] requested, but transliterate module not available".format(data['name_official_transliterated'][1:3])))
                    del self.institution['names'][0]['name_official_transliterated']
            else:
                self.institution['names'][0]['name_official_transliterated'] = csv_coalesce('name_official_transliterated')
        if csv_coalesce('name_version'):
            self._query_name(csv_coalesce('name_version'))
            self.institution['names'][0]['alternative_names'] = [ { 'name': csv_coalesce('name_version') } ]
        if csv_coalesce('acronym'):
            self.institution['names'][0]['acronym'] = csv_coalesce('acronym')
        if csv_coalesce('city'):
            self.institution['countries'][0]['city'] = csv_coalesce('city')
        if csv_coalesce('founding_date'):
            match = re.match(r'^\s*([0-9]{4})(-(?:1[012]|0?[0-9])-(?:31|30|[012]?[0-9]))?\s*$', data['founding_date'])
            if match:
                if match[2] is None:
                    self.institution['founding_date'] = match[1] + '-01-01'
                else:
                    self.institution['founding_date'] = match[1] + match[2]
            else:
                raise DataError("Malformed founding_date: [{}]".format(data['founding_date']))
        if csv_coalesce('closing_date'):
            match = re.match(r'^\s*([0-9]{4})(-(?:1[012]|0?[0-9])-(?:31|30|[012]?[0-9]))?\s*$', data['closing_date'])
            if match:
                if match[2] is None:
                    self.institution['closing_date'] = match[1] + '-12-31'
                else:
                    self.institution['closing_date'] = match[1] + match[2]
            else:
                raise DataError("Malformed closing_date: [{}]".format(data['closing_date']))

        # process identifier
        if csv_coalesce('identifier'):
            self.institution['identifiers'] = [ { 'identifier': csv_coalesce('identifier') } ]
            if 'identifier_resource' not in data and 'agency_id' not in data:
                raise(DataError("Identifier needs to have an agency ID or a resource."))
            if 'identifier_resource' in data and 'agency_id' in data:
                warn(DataWarning("  - identifier [{}] should not have both agency_id AND a resource".format(csv_coalesce('identifier'))))
            if 'identifier_resource' in data:
                self.institution['identifiers'][0]['resource'] = csv_coalesce('identifier_resource')
                self.api.logger.info('  - identifier [{}] with resource [{}]'.format(csv_coalesce('identifier'), csv_coalesce('identifier_resource')))
            else:
                self.institution['identifiers'][0]['resource'] = 'local identifier'
                self.api.logger.info('  - identifier [{}] as local identifier'.format(csv_coalesce('identifier')))
            if 'agency_id' in data:
                self.institution['identifiers'][0]['agency'] = csv_coalesce('agency_id')
                self.api.logger.info('  linked to agency ID [{}]'.format(csv_coalesce('agency_id')))

        # process parent institution
        if csv_coalesce('parent_id', 'parent_deqar_id'):
            match = re.match('\s*(DEQARINST)?([0-9]+)\s*', str(csv_coalesce('parent_id', 'parent_deqar_id')).upper())
            if match:
                self.institution['hierarchical_parent'] = [ { 'institution': int(match.group(2)) } ]
                if csv_coalesce('parent_type'):
                    if self.api.HierarchicalTypes.get(csv_coalesce('parent_type')):
                        self.institution['hierarchical_parent'][0]['relationship_type'] = self.api.HierarchicalTypes.get(csv_coalesce('parent_type'))['id']
                    else:
                        raise DataError('Unknown parent_type: [{}]'.format(csv_coalesce('parent_type')))
                self.api.logger.info('  - hierarchical parent ID [{}] (source: [{}])'.format(int(match.group(2)), csv_coalesce('parent_id', 'parent_deqar_id')))
            else:
                raise DataError('Malformed parent_id: [{}]'.format(csv_coalesce('parent_id', 'parent_deqar_id')))

        # process QF levels
        if csv_coalesce('qf_ehea_levels'):
            self.institution['qf_ehea_levels'] = self.api.create_qf_ehea_level_set(data['qf_ehea_levels'])
        else:
            self.institution['qf_ehea_levels'] = list()

    def _url_normalise(self, website):
        """
        normalises the URL, add http protocol if none specified, resolves redirects
        """
        match = re.match(r'^\s*([a-z0-9]+://)?([^/]+)(/.*)?$', website, flags=re.IGNORECASE)
        if match:
            protocol = (match[1] or 'http://').lower()
            domain = match[2].lower()
            path = match[3] or '/'
            url = protocol + domain + path
            try:
                r = requests.head(url, allow_redirects=True)
            except requests.exceptions.ConnectionError:
                self.api.logger.warning("  - could not connect to URL [{}]".format(url))
                return url
            else:
                if r.status_code in [ requests.codes.ok, requests.codes.created ]:
                    if r.url != url:
                        self.api.logger.warning("  - URL [{}] was redirected to [{}]".format(url, r.url))
                    return r.url
                else:
                    self.api.logger.warning("  - URL [{}] did not return a successful status: {} {}".format(r.url, r.status_code, r.reason))
                    return url
        else:
            raise(DataError('[{}] is not a valid http/https URL.'.format(website)))


    def _query_name(self, name):
        """
        search for existing institution by name
        """
        candidates = self.api.get('/connectapi/v1/institutions/', query=name)
        if candidates['count']:
            for hei in candidates['results']:
                self.api.logger.warning('  - possible duplicate, name match: {deqar_id} {name_primary}'.format(**hei))
            return candidates['results']
        return False


    def post(self):
        """
        POST the prepared institution object and return the new DEQARINST ID
        """
        response = self.api.post('/adminapi/v1/institutions/', self.institution)
        self.institution['deqar_id'] = "DEQARINST{:04d}".format(response['id'])
        self.api.logger.info(str(self))
        return(self.institution['deqar_id'])


    def __str__(self):
        if 'deqar_id' in self.institution:
            return("{0[deqar_id]}: {0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.api.Countries.get(self.institution['countries'][0]['country'])))
        else:
            return("{0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.api.Countries.get(self.institution['countries'][0]['country'])))


class Institution:

    """
    load and modify an existing institution record
    """

    def __init__(self, api, pk):
        # save api for later use
        self.api = api

        self.data = self.api.get(f'/adminapi/v1/institutions/{pk}/')

        for item in [ 'id', 'deqar_id', 'created_at', 'update_log' ]:
            setattr(self, item, self.data.get(item))
            del self.data[item]

        self.data['names'] = self.data['names_actual'] + self.data['names_former']
        del self.data['names_actual']
        del self.data['names_former']

        self.data['identifiers'] = self.data['identifiers_local'] + self.data['identifiers_national']
        del self.data['identifiers_local']
        del self.data['identifiers_national']

        def replace_dict_by_pk(array, item, pk = 'id'):
            for i in array:
                if type(i.get(item)) == dict:
                    i[item] = i[item].get(pk)

        replace_dict_by_pk(self.data['countries'], 'country')
        replace_dict_by_pk(self.data['identifiers'], 'agency')

        for item in [ 'hierarchical_parent', 'hierarchical_child', 'historical_source', 'historical_target' ]:
            replace_dict_by_pk(self.data[item], 'institution')
        for item in [ 'hierarchical_parent', 'hierarchical_child', 'historical_source', 'historical_target' ]:
            replace_dict_by_pk(self.data[item], 'relationship_type')

    def save(self, comment='changed by deqarclient.py'):
        """
        PUT the prepared institution object
        """
        data = self.data.copy()
        data['submit_comment'] = comment
        response = self.api.put(f'/adminapi/v1/institutions/{self.id}/', data)
        self.api.logger.info(str(self))
        return(response)

    def __str__(self):
        return(f"{self.deqar_id}: {self.data['name_primary']} ({self.data['website_link']})")

