#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re

from getpass import getpass
from warnings import warn

import requests

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

    def __init__(self, base, token=None, verbose=False):
        """ Constructor prepares for request. Token is taken from parameter, environment or user is prompted to log in. """
        self.session            = requests.Session()
        self.base               = base.rstrip('/')
        self.webapi             = '/webapi/v2'
        self.request_timeout    = 5
        self.verbose            = verbose

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

    def _log(self, msg, level=PLAIN):
        """ output message, using click if available """
        if self.verbose or level == self.WARN or level == self.ERROR:
            try:
                from click import secho
                if level == self.NOTICE:
                    secho(msg, bold=True)
                elif level == self.WARN:
                    secho(msg, fg='yellow')
                elif level == self.ERROR:
                    secho(msg, fg='red')
                else:
                    secho(msg)

            except ImportError:
                print(msg)

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

    def get(self, path, **kwargs):
        """ make a GET request to [path] with parameters from [kwargs] """
        try:
            r = self.session.get(self.base + path, params=kwargs, timeout=self.request_timeout)
        except (KeyboardInterrupt):
            self._log("Request aborted.", self.WARN)
            return(False)

        if r.status_code == requests.codes.ok:
            self._log("Success: {} {}".format(r.status_code, r.reason))
            return(r.json())
        else:
            self._log("{}\nError: {} {}".format(r.url, r.status_code, r.reason), self.ERROR)
            raise Exception("{} {}".format(r.status_code, r.reason))

    def post(self, path, data):
        """ POST [data] to API endpoint [path] """
        try:
            r = self.session.post(self.base + path, json=data, timeout=self.request_timeout)
        except (KeyboardInterrupt):
            self._log("Request aborted.", self.WARN)
            return(False)

        if r.status_code in [ requests.codes.ok, requests.codes.created ]:
            self._log("Success: {} {}".format(r.status_code, r.reason))
            return(r.json())
        else:
            self._log("{}\nError: {} {}".format(r.url, r.status_code, r.reason), self.ERROR)
            self._log(r.text, self.ERROR)
            raise Exception("{} {}".format(r.status_code, r.reason))

    def get_institutions(self, **kwargs):
        """ search institutions, as defined by [kwargs] """
        return(self.get(self.webapi + "/browse/institutions/", **kwargs))

    def get_institution(self, id):
        """ get single institution record [id] """
        return(self.get(self.webapi + "/browse/institutions/{:d}".format(id)))

    def get_countries(self):
        return(Countries(self))

    def get_qf_ehea_levels(self):
        return(QfEheaLevels(self))

    def create_qf_ehea_level_set(self, *args, **kwargs):
        return(QfEheaLevelSet(self, *args, **kwargs))

    def create_institution(self, *args, **kwargs):
        return(NewInstitution(self, *args, **kwargs))

    def domain_checker(self):
        return DomainChecker(self)


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

class DomainChecker:

    """ Fetches website addresses of all known institutions and allows to check against it """

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
        identifies the core domain of a URL by stripping protocol, www, etc.
        """
        match = re.match(r'^\s*(?:[a-z0-9]+://)?(?:www\.)?([^/]+)/?.*$', website, flags=re.IGNORECASE)
        if match:
            return match[1].lower()
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


class QfEheaLevelSet (list):

    """ Actual set of QfEheaLevels - constructed from input string, mainly for HEI data import """

    Levels          = None   # this is a class property and will be filled when the first object is made
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

        if not QfEheaLevelSet.Levels:
            """ if reference list does not exist, we'll fetch it now """
            QfEheaLevelSet.Levels = api.get_qf_ehea_levels()

        for l in re.split(r'\s*[^A-Za-z0-9]\s*', string.strip(" ,.\n\r\t\v\f")):
            match = re.match('([01235678]|cycle|{})'.format("|".join(self.LevelKeywords.keys())), l);
            if match and match.group(1) != 'cycle':
                m = match.group(1)
                if m.isdigit():
                    if int(m) > 4:
                        m = int(m) - 5
                else:
                    m = self.LevelKeywords[m]
                level = self.Levels.get(m)
                recognised.add(level['id'])
                if verbose:
                    api._log('  [{}] => {}/{}'.format(l, level['id'], level['level']))
            elif match and match.group(1) == 'cycle':
                pass
            elif strict:
                raise(DataError('  [{}] : QF-EHEA level not recognised, ignored.'.format(l)))
            elif verbose:
                api._log('  [{}] : QF-EHEA level not recognised, ignored.'.format(l), api.WARN)

        for i in recognised:
            self.append({ 'qf_ehea_level': i })

    def __str__(self):
        return("QF-EHEA: {}".format("-".join([ str(level['qf_ehea_level'] + 4) for level in self ])))

class NewInstitution:

    """
    creates a new institution record from CSV input
    """

    Countries = None   # this is a class property and will be filled when the first object is made
    Domains = None

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

        # get reference list
        if not NewInstitution.Countries:
            NewInstitution.Countries = api.get_countries()
        if not NewInstitution.Domains:
            NewInstitution.Domains = api.domain_checker()

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

        # check for duplicate by internet domain
        self.Domains.query(csv_coalesce('website_link'))
        if self.Domains.core_domain(website) != self.Domains.core_domain(csv_coalesce('website_link')):
            self.Domains.query(website)

        # resolve country ISO to ID if needed
        if csv_coalesce('country_id', 'country_iso', 'country'):
            which = csv_coalesce('country_id', 'country_iso', 'country')
            country = self.Countries.get(which)
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
                if verbose:
                    self.api._log('  - hierarchical parent ID [{}] (source: [{}])'.format(int(match.group(2)), csv_coalesce('parent_id', 'parent_deqar_id')))
            else:
                raise DataError('Malformed parent_id: [{}]'.format(csv_coalesce('parent_id', 'parent_deqar_id')))

        # process QF levels
        if csv_coalesce('qf_ehea_levels'):
            self.institution['qf_ehea_levels'] = api.create_qf_ehea_level_set(data['qf_ehea_levels'], verbose=verbose)
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
                    self.api._log("  - URL [{}] did nor return a successful status: {} {}".format(r.url, r.status_code, r.reason), self.api.WARN)
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
            return("{0[deqar_id]}: {0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.Countries.get(self.institution['countries'][0]['country'])))
        else:
            return("{0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.Countries.get(self.institution['countries'][0]['country'])))

