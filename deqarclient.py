#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import click
import os
import re
from getpass import getpass
from warnings import warn
from transliterate import translit

class DataError (Exception):

    """ Raised when input data is malformed etc. """

    pass

class DataWarning (Warning):

    """ Warnings triggered by input data issues """

    pass

class EqarApi:

    """ EqarApi : REST API client for the DEQAR database """

    def __init__(self, base, token=None):
        """ Constructor prepares for request. Token is taken from parameter, environment or user is prompted to log in. """
        self.session            = requests.Session()
        self.base               = base.rstrip('/')
        self.webapi             = '/webapi/v2'
        self.request_timeout    = 5

        self.session.headers.update({
            'user-agent': 'deqar-api-client/0.1 ' + self.session.headers['User-Agent'],
            'accept': 'application/json'
        })

        click.secho("DEQAR API at {}".format(self.base), bold=True)

        if token:
            self.session.headers.update({ 'authorization': 'Bearer ' + token })
        elif 'DEQAR_TOKEN' in os.environ and os.environ['DEQAR_TOKEN']:
            click.secho("DEQAR_TOKEN variable set", bold=True)
            self.session.headers.update({ 'authorization': 'Bearer ' + os.environ['DEQAR_TOKEN'] })
        else:
            self.session.headers.update({ 'authorization': 'Bearer ' + self.login() })

    def login(self):
        """ Interactive login prompt """
        click.secho("DEQAR login required:", bold=True)

        if 'DEQAR_USER' in os.environ:
            user = os.environ['DEQAR_USER']
            click.secho("Username [{}] from environment variable".format(user))
        else:
            user = input("DEQAR username: ")

        if 'DEQAR_PASSWORD' in os.environ:
            password = os.environ['DEQAR_PASSWORD']
            click.secho("Password [***] from environment variable")
        else:
            password = getpass("DEQAR password: ")

        r = self.session.post(self.base + '/accounts/get_token/', data={ 'username': user, 'password': password }, timeout=self.request_timeout)

        if r.status_code == requests.codes.ok:
            click.secho("Login successful: {} {}".format(r.status_code, r.reason))
            return(r.json()['token'])
        else:
            click.secho("Error: {} {}".format(r.status_code, r.reason), fg='red')
            raise Exception("DEQAR login failed.")

    def get(self, path, kwargs=None):
        """ make a GET request to [path] with parameters from [kwargs] """
        try:
            r = self.session.get(self.base + path, params=kwargs, timeout=self.request_timeout)
        except (KeyboardInterrupt):
            click.secho("Request aborted.", bold=True)
            return(False)

        if r.status_code == requests.codes.ok:
            click.secho("Success: {} {}".format(r.status_code, r.reason), nl=False)
            return(r.json())
        else:
            click.secho("{}\nError: {} {}".format(r.url, r.status_code, r.reason), fg='red')
            raise Exception("{} {}".format(r.status_code, r.reason))

    def post(self, path, data):
        """ POST [data] to API endpoint [path] """
        try:
            r = self.session.post(self.base + path, json=data, timeout=self.request_timeout)
        except (KeyboardInterrupt):
            click.secho("Request aborted.", bold=True)
            return(False)

        if r.status_code in [ requests.codes.ok, requests.codes.created ]:
            click.secho("Success: {} {}".format(r.status_code, r.reason), nl=False)
            return(r.json())
        else:
            click.secho("{}\nError: {} {}".format(r.url, r.status_code, r.reason), fg='red')
            print(r.text)
            raise Exception("{} {}".format(r.status_code, r.reason))

    def get_institutions(self, **kwargs):
        """ search institutions, as defined by [kwargs] """
        return(self.get(self.webapi + "/browse/institutions/", kwargs))

    def get_institution(self, id):
        """ get single institution record [id] """
        return(self.get(self.webapi + "/browse/institutions/{:d}".format(id), None))

    def get_countries(self):
        return(Countries(self))

    def get_qf_ehea_levels(self):
        return(QfEheaLevels(self))

    def create_qf_ehea_level_set(self, *args, **kwargs):
        return(QfEheaLevelSet(self, *args, **kwargs))

    def create_institution(self, *args, **kwargs):
        return(NewInstitution(self, *args, **kwargs))

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
                    print('  [{}] => {}/{}'.format(l, level['id'], level['level']))
            elif match and match.group(1) == 'cycle':
                pass
            elif strict:
                raise(DataError('  [{}] : QF-EHEA level not recognised, ignored.'.format(l)))
            elif verbose:
                print('  [{}] : QF-EHEA level not recognised, ignored.'.format(l))

        for i in recognised:
            self.append({ 'qf_ehea_level': i })

    def __str__(self):
        return("QF-EHEA: {}".format("-".join([ str(level['qf_ehea_level'] + 4) for level in self ])))

class NewInstitution:

    Countries = None   # this is a class property and will be filled when the first object is made

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

        # check if name and website present
        if not ( csv_coalesce('name_official') and csv_coalesce('website_link') ):
            raise DataError("Institution must have an official name and a website.")

        # determine primary name
        name_primary = csv_coalesce('name_english', 'name_official')

        if verbose:
            print('* {}:'.format(name_primary))
            if csv_coalesce('name_english'):
                print('  - English name given, used as primary')
            else:
                print('  - No English name, used official name as primary')
            print('  - webiste={}'.format(csv_coalesce('website_link')))

        # resolve country ISO to ID if needed
        if csv_coalesce('country_id', 'country_iso', 'country'):
            which = csv_coalesce('country_id', 'country_iso', 'country')
            country = self.Countries.get(which)
            if not country:
                raise DataError("Unknown country [{}]".format(which))
            elif verbose:
                print('  - country [{}] resolved to {} (ID {})'.format(which,country['name_english'],country['id']))
        else:
            raise DataError("Country needs to be specified")

        # basic record
        self.institution = dict(
            name_primary=name_primary,
            website_link=csv_coalesce('website_link'),
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

        # add optional attributes
        if csv_coalesce('name_english'):
            self.institution['names'][0]['name_english'] = csv_coalesce('name_english')
        if csv_coalesce('name_official_transliterated'):
            if data['name_official_transliterated'][0] == '*':
                self.institution['names'][0]['name_official_transliterated'] = translit(csv_coalesce('name_official'), data['name_official_transliterated'][1:3], reversed=True)
                if verbose:
                    print("  - transliterated '{}' -> '{}'".format(csv_coalesce('name_official'), self.institution['names'][0]['name_official_transliterated']))
            else:
                self.institution['names'][0]['name_official_transliterated'] = csv_coalesce('name_official_transliterated')
        if csv_coalesce('name_version'):
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
            if 'resource' not in data and 'agency_id' not in data:
                raise(DataError("Identifier needs to have an agency ID or a resource."))
            if 'resource' in data:
                self.institution['identifiers'][0]['resource'] = csv_coalesce('resource')
                if verbose:
                    print('  - identifier [{}] with resource [{}]'.format(csv_coalesce('identifier'), csv_coalesce('resource')))
            else:
                self.institution['identifiers'][0]['resource'] = 'local identifier'
                if verbose:
                    print('  - identifier [{}] as local identifier'.format(csv_coalesce('identifier')))
            if 'agency_id' in data:
                self.institution['identifiers'][0]['agency'] = csv_coalesce('agency_id')
                if verbose:
                    print('  linked to agency ID [{}]'.format(csv_coalesce('agency_id')))

        # process parent institution
        if csv_coalesce('parent_id', 'parent_deqar_id'):
            match = re.match('\s*(DEQARINST)?([0-9]+)\s*', str(csv_coalesce('parent_id', 'parent_deqar_id')).upper())
            if match:
                self.institution['hierarchical_parent'] = [ { 'institution': int(match.group(2)) } ]
                if verbose:
                    print('  - hierarchical parent ID [{}] (source: [{}])'.format(int(match.group(2)), csv_coalesce('parent_id', 'parent_deqar_id')))
            else:
                raise DataError('Malformed parent_id: [{}]'.format(csv_coalesce('parent_id', 'parent_deqar_id')))

        # process QF levels
        if csv_coalesce('qf_ehea_levels'):
            self.institution['qf_ehea_levels'] = api.create_qf_ehea_level_set(data['qf_ehea_levels'], verbose=verbose)
        else:
            self.institution['qf_ehea_levels'] = list()

    def post(self, verbose=False):
        response = self.api.post('/adminapi/v1/institutions/', self.institution)
        self.institution['deqar_id'] = "DEQARINST{:04d}".format(response['id'])
        if verbose:
            print(str(self))
        return(self.institution['deqar_id'])

    def __str__(self):
        if 'deqar_id' in self.institution:
            return("{0[deqar_id]}: {0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.Countries.get(self.institution['countries'][0]['country'])))
        else:
            return("{0[name_primary]} ({0[website_link]}, {1[name_english]}, {0[qf_ehea_levels]})".format(self.institution, self.Countries.get(self.institution['countries'][0]['country'])))

