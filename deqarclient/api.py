# -*- coding: utf-8 -*-

import json
import os
import re
import logging

from getpass import getpass
from warnings import warn

import requests
from tldextract import TLDExtract

from .auth import EqarApiEnvAuth
from .institution import NewInstitution
from .errors import *

class EqarApi:
    """ EqarApi : REST API client for the DEQAR database """

    _version = '0.3.1'

    _Countries = None           # these properties will be instantiated
    _QfEheaLevels = None        # as objects when first accessed
    _HierarchicalTypes = None
    _DomainChecker = None
    _OrganizationTypes = None

    def __init__(self, base, authclass=EqarApiEnvAuth, request_timeout=10, **kwargs):
        """ Constructor prepares for request. Token is taken from parameter, environment or user is prompted to log in. """
        self.session            = requests.Session()
        self.base               = base.rstrip('/')
        self.webapi             = '/webapi/v2'
        self.request_timeout    = request_timeout
        self.logger             = logging.getLogger(__name__)

        self.session.headers.update({
            'user-agent': f'deqar-api-client/{self._version} ' + self.session.headers['User-Agent'],
            'accept': 'application/json'
        })

        self.logger.debug("DEQAR API at {}".format(self.base))
        self.session.headers.update({ 'authorization': 'Bearer ' + authclass(api=self, **kwargs).token })

    def _request(self, method, path, **kwargs):
        """ make a request to [path] with parameters from [kwargs] """

        self.logger.debug("[{} {} started]".format(method, self.base + path))

        r = self.session.request(method, self.base + path, timeout=self.request_timeout, **kwargs)

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            self.logger.error("[HTTP error {}: {}]".format(r.status_code, r.reason))
            raise HttpError("{} {}".format(r.status_code, r.reason))
        else:
            self.logger.debug("[HTTP {} {}]".format(r.status_code, r.reason))
            return(r.json())

    def get(self, path, **kwargs):
        """ make a GET request to [path] with parameters from [kwargs] """
        return(self._request('GET', path, params=kwargs))

    def post(self, path, data):
        """ POST [data] to API endpoint [path] """
        return(self._request('POST', path, json=data))

    def put(self, path, data):
        """ PUT [data] to API endpoint [path] """
        return(self._request('PUT', path, json=data))

    def get_institutions(self, **kwargs):
        """ search institutions, as defined by [kwargs] """
        return(self.get(self.webapi + "/browse/institutions/", **kwargs))

    def get_institution(self, id):
        """ get single institution record [id] """
        return(self.get(self.webapi + "/browse/institutions/{:d}".format(id)))

    def create_qf_ehea_level_set(self, *args, **kwargs):

        class QfEheaLevelSet (list):
            """ Actual set of QfEheaLevels - constructed from input list, mainly for HEI data import """

            LevelKeywords   = dict(
                                short=0,
                                first=1,
                                second=2,
                                secound=2,
                                third=3
                            )

            def __init__(self, api, source_list, strict=False):
                """ parses a string for a set of levels, specified by digits or key words, eliminating duplicates and ignoring unknowns """

                recognised = set()

                for l in source_list:
                    match = re.search('([01235678]|{})'.format("|".join(self.LevelKeywords.keys())), l, re.IGNORECASE);
                    if match:
                        m = match.group(1)
                        if m.isdigit():
                            if int(m) > 4:
                                m = int(m) - 5
                        else:
                            m = self.LevelKeywords[m.lower()]
                        level = api.QfEheaLevels.get(m)
                        recognised.add(level['code'])
                        api.logger.debug('  [{}] => {}/{}'.format(l, level['id'], level['level']))
                    elif strict:
                        raise(DataError('  [{}] : QF-EHEA level not recognised, ignored.'.format(l)))
                    else:
                        api.logger.debug('  [{}] : QF-EHEA level not recognised, ignored.'.format(l))

                for i in recognised:
                    self.append(api.QfEheaLevels.get(i))

            def __str__(self):
                return("QF-EHEA: {}".format("-".join([ str(level['id'] + 4) for level in self ])))

        return(QfEheaLevelSet(self, *args, **kwargs))

    def create_institution(self, *args, **kwargs):
        """ create a new institution record """
        return(NewInstitution(self, *args, **kwargs))

    @property
    def Countries(self):
        if not self._Countries:
            class Countries:
                """ Class allows to look up countries by ISO code or ID """
                def __init__(self, api):
                    self.countries = api.get("/adminapi/v1/select/country/")
                def get(self, which):
                    if type(which) == str and which.isdigit():
                        which = int(which)
                    for c in self.countries:
                        if which in [ c['id'], c['iso_3166_alpha2'], c['iso_3166_alpha3'] ]:
                            return c
            self._Countries = Countries(self)

        return(self._Countries)

    @property
    def QfEheaLevels(self):
        if not self._QfEheaLevels:
            class QfEheaLevels:
                """ Class allows to look up QF EHEA levels by numeric ID or name """
                def __init__(self, api):
                    self.levels = api.get("/adminapi/v1/select/qf_ehea_level/")
                def get(self, which):
                    if type(which) == str and which.isdigit():
                        which = int(which)
                    for l in self.levels:
                        if which in [ l['code'], l['level'] ]:
                            return l
            self._QfEheaLevels = QfEheaLevels(self)

        return(self._QfEheaLevels)

    @property
    def HierarchicalTypes(self):
        if not self._HierarchicalTypes:
            class HierarchicalTypes:
                """ Class allows to look up hierarchical relationship types by numeric ID or name """
                def __init__(self, api):
                    self.types = api.get("/adminapi/v1/select/institution_hierarchical_relationship_types/")
                def get(self, which):
                    if type(which) == str and which.isdigit():
                        which = int(which)
                    for l in self.types:
                        if which in [ l['id'], l['type'] ]:
                            return l
                    return None
            self._HierarchicalTypes = HierarchicalTypes(self)

        return(self._HierarchicalTypes)

    @property
    def OrganizationTypes(self):
        if not self._OrganizationTypes:
            class OrganizationTypes:
                """ Class to look up organization types (for AP) """
                def __init__(self, api):
                    self.types = api.get("/adminapi/v1/select/institutions/organization_type/")
                def get(self, which):
                    if type(which) == str and which.isdigit():
                        which = int(which)
                    for l in self.types:
                        if which in [ l['id'], l['type'] ]:
                            return l
                    return None
            self._OrganizationTypes = OrganizationTypes(self)
        return(self._OrganizationTypes)

    @property
    def DomainChecker(self):

        if not self._DomainChecker:

            class DomainChecker:

                """ Fetches website addresses of all known institutions and allows to check against it """

                EXTRACT = TLDExtract(include_psl_private_domains=True)

                def __init__(self, api):

                    self.api = api
                    heis = self.api.get('/connectapi/v1/institutions', limit=10000)

                    self.domains = dict()
                    for hei in heis['results']:
                        url = None
                        if 'website_link' in hei:
                            try:
                                url = self.core_domain(hei['website_link'])
                            except DataError:
                                pass
                        if url:
                            if url not in self.domains:
                                self.domains[url] = list()
                            self.domains[url].append(hei)

                def core_domain(self, website):
                    """
                    identifies the core domain of a URL using known TLDs and Public Suffix List
                    """
                    match = self.EXTRACT(website)
                    if match.suffix:
                        return f'{match.domain}.{match.suffix}'.lower()
                    else:
                        raise(DataError('[{}] is not a valid http/https URL.'.format(website)))

                def query(self, website):
                    """
                    query if core domain is already known
                    """
                    if self.core_domain(website) in self.domains:
                        for hei in self.domains[self.core_domain(website)]:
                            self.api.logger.warning('  - possible duplicate: {deqar_id} {name_primary} - URL [{website_link}]'.format(**hei))
                        return self.domains[self.core_domain(website)]
                    else:
                        return False

            self._DomainChecker = DomainChecker(self)

        return(self._DomainChecker)

