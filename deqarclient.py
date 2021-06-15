#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re

from getpass import getpass
from warnings import warn

import requests
from tldextract import TLDExtract

class DataError (Exception):

    """ Raised when input data is malformed etc. """

    pass

class DataWarning (Warning):

    """ Warnings triggered by input data issues """

    pass

class EqarApi:

    """ EqarApi : REST API client for the DEQAR database """

    PLAIN  = 0
    NOTICE = 1
    WARN   = 2
    ERROR  = 3
    GOOD   = 4

    _Countries = None           # these class properties will be instantiated
    _QfEheaLevels = None        # as objects when first accessed
    _HierarchicalTypes = None
    _DomainChecker = None

    def __init__(self, base, token=None, verbose=False, color=None):
        """ Constructor prepares for request. Token is taken from parameter, environment or user is prompted to log in. """
        self.session            = requests.Session()
        self.base               = base.rstrip('/')
        self.webapi             = '/webapi/v2'
        self.request_timeout    = 5
        self.verbose            = verbose
        self.color              = color

        self.session.headers.update({
            'user-agent': 'deqar-api-client/0.2 ' + self.session.headers['User-Agent'],
            'accept': 'application/json'
        })


        self._log("DEQAR API at {}".format(self.base), self.NOTICE)

        if token:
            self.session.headers.update({ 'authorization': 'Bearer ' + token })
        elif os.getenv('DEQAR_TOKEN'):
            self._log("DEQAR_TOKEN variable set", self.NOTICE)
            self.session.headers.update({ 'authorization': 'Bearer ' + os.getenv('DEQAR_TOKEN') })
        else:
            self.session.headers.update({ 'authorization': 'Bearer ' + self.login() })

    def _log(self, msg, level=PLAIN, nl=True):
        """ output message, using click if available """
        if self.verbose or level == self.WARN or level == self.ERROR:
            try:
                from click import secho
                if level == self.NOTICE:
                    secho(msg, bold=True, nl=nl, color=self.color)
                elif level == self.WARN:
                    secho(msg, fg='yellow', nl=nl, color=self.color)
                elif level == self.ERROR:
                    secho(msg, fg='red', nl=nl, color=self.color)
                elif level == self.GOOD:
                    secho(msg, fg='green', nl=nl, color=self.color)
                else:
                    secho(msg, nl=nl, color=self.color)

            except ImportError:
                print(msg, end='\n' if nl else '')

    def login(self):
        """ Interactive login prompt """
        self._log("DEQAR login required:", self.NOTICE)

        if 'DEQAR_USER' in os.environ:
            user = os.environ['DEQAR_USER']
            self._log("Username [{}] from environment variable".format(user))
        else:
            user = input("DEQAR username: ")

        if 'DEQAR_PASSWORD' in os.environ:
            password = os.environ['DEQAR_PASSWORD']
            self._log("Password [***] from environment variable")
        else:
            password = getpass("DEQAR password: ")

        r = self.session.post(self.base + '/accounts/get_token/', data={ 'username': user, 'password': password }, timeout=self.request_timeout)

        if r.status_code == requests.codes.ok:
            self._log("Login successful: {} {}".format(r.status_code, r.reason))
            return(r.json()['token'])
        else:
            self._log("Error: {} {}".format(r.status_code, r.reason), self.ERROR)
            raise Exception("DEQAR login failed.")

    def _request(self, method, path, **kwargs):
        """ make a request to [path] with parameters from [kwargs] """

        self._log("[{} {} ".format(method, self.base + path), nl=False)

        r = self.session.request(method, self.base + path, timeout=self.request_timeout, **kwargs)

        if r.status_code == requests.codes.ok:
            self._log("{} {}".format(r.status_code, r.reason), self.GOOD, nl=False)
            self._log("]")
            return(r.json())
        else:
            self._log("{} {}".format(r.status_code, r.reason), self.ERROR, nl=False)
            self._log("]")
            self._log(r.text, self.ERROR)
            raise Exception("{} {}".format(r.status_code, r.reason))

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
            """ Actual set of QfEheaLevels - constructed from input string, mainly for HEI data import """

            LevelKeywords   = dict(
                                short=0,
                                first=1,
                                second=2,
                                secound=2,
                                third=3
                            )

            def __init__(self, api, string, verbose=False, strict=False):
                """ parses a string for a set of levels, specified by digits or key words, eliminating duplicates and ignoring unknowns """

                recognised = set()

                for l in re.split(r'\s*[^A-Za-z0-9]\s*', string.strip(" ,.\n\r\t\v\f")):
                    match = re.match('([01235678]|cycle|{})'.format("|".join(self.LevelKeywords.keys())), l);
                    if match and match.group(1) != 'cycle':
                        m = match.group(1)
                        if m.isdigit():
                            if int(m) > 4:
                                m = int(m) - 5
                        else:
                            m = self.LevelKeywords[m]
                        level = api.QfEheaLevels.get(m)
                        recognised.add(level['code'])
                        if verbose:
                            api._log('  [{}] => {}/{}'.format(l, level['id'], level['level']))
                    elif match and match.group(1) == 'cycle':
                        pass
                    elif strict:
                        raise(DataError('  [{}] : QF-EHEA level not recognised, ignored.'.format(l)))
                    elif verbose:
                        api._log('  [{}] : QF-EHEA level not recognised, ignored.'.format(l), api.WARN)

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
        if not self.__class__._Countries:
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
            self.__class__._Countries = Countries(self)

        return(self.__class__._Countries)

    @property
    def QfEheaLevels(self):
        if not self.__class__._QfEheaLevels:
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
            self.__class__._QfEheaLevels = QfEheaLevels(self)

        return(self.__class__._QfEheaLevels)

    @property
    def HierarchicalTypes(self):
        if not self.__class__._HierarchicalTypes:
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
            self.__class__._HierarchicalTypes = HierarchicalTypes(self)

        return(self.__class__._HierarchicalTypes)

    @property
    def DomainChecker(self):

        if not self.__class__._DomainChecker:

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
                            self.api._log('  - possible duplicate: {deqar_id} {name_primary} - URL [{website_link}]'.format(**hei), self.api.WARN)
                        return self.domains[self.core_domain(website)]
                    else:
                        return False

            self.__class__._DomainChecker = DomainChecker(self)

        return(self.__class__._DomainChecker)

class NewInstitution:

    """
    creates a new institution record from CSV input
    """

    def __init__(self, api, data, verbose=False):

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

        if verbose:
            self.api._log('\n* {}:'.format(name_primary), self.api.NOTICE)
            if csv_coalesce('name_english'):
                self.api._log('  - English name given, used as primary')
            else:
                self.api._log('  - No English name, used official name as primary')
            self.api._log('  - webiste={}'.format(csv_coalesce('website_link')))

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
            elif verbose:
                self.api._log('  - country [{}] resolved to {} (ID {})'.format(which,country['name_english'],country['id']))
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
                    if verbose:
                        self.api._log("  - transliterated '{}' -> '{}'".format(csv_coalesce('name_official'), self.institution['names'][0]['name_official_transliterated']))
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
                if verbose:
                    self.api._log('  - identifier [{}] with resource [{}]'.format(csv_coalesce('identifier'), csv_coalesce('identifier_resource')))
            else:
                self.institution['identifiers'][0]['resource'] = 'local identifier'
                if verbose:
                    self.api._log('  - identifier [{}] as local identifier'.format(csv_coalesce('identifier')))
            if 'agency_id' in data:
                self.institution['identifiers'][0]['agency'] = csv_coalesce('agency_id')
                if verbose:
                    self.api._log('  linked to agency ID [{}]'.format(csv_coalesce('agency_id')))

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
                if verbose:
                    self.api._log('  - hierarchical parent ID [{}] (source: [{}])'.format(int(match.group(2)), csv_coalesce('parent_id', 'parent_deqar_id')))
            else:
                raise DataError('Malformed parent_id: [{}]'.format(csv_coalesce('parent_id', 'parent_deqar_id')))

        # process QF levels
        if csv_coalesce('qf_ehea_levels'):
            self.institution['qf_ehea_levels'] = self.api.create_qf_ehea_level_set(data['qf_ehea_levels'], verbose=verbose)
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
                self.api._log("  - could not connect to URL [{}]".format(url), self.api.WARN)
                return url
            else:
                if r.status_code in [ requests.codes.ok, requests.codes.created ]:
                    if r.url != url:
                        self.api._log("  - URL [{}] was redirected to [{}]".format(url, r.url), self.api.WARN)
                    return r.url
                else:
                    self.api._log("  - URL [{}] did not return a successful status: {} {}".format(r.url, r.status_code, r.reason), self.api.WARN)
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
                self.api._log('  - possible duplicate, name match: {deqar_id} {name_primary}'.format(**hei), self.api.WARN)
            return candidates['results']
        return False


    def post(self, verbose=False):
        """
        POST the prepared institution object and return the new DEQARINST ID
        """
        response = self.api.post('/adminapi/v1/institutions/', self.institution)
        self.institution['deqar_id'] = "DEQARINST{:04d}".format(response['id'])
        if verbose:
            self.api._log(str(self))
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

    def __init__(self, api, pk, verbose=False):
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

    def save(self, comment='changed by deqarclient.py', verbose=False):
        """
        PUT the prepared institution object
        """
        data = self.data.copy()
        data['submit_comment'] = comment
        response = self.api.put(f'/adminapi/v1/institutions/{self.id}/', data)
        if verbose:
            self.api._log(str(self))
        return(response)

    def __str__(self):
        return(f"{self.deqar_id}: {self.data['name_primary']} ({self.data['website_link']})")

