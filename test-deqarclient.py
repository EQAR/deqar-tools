#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import re
import unittest
import coloredlogs

from deqarclient.api import EqarApi
from deqarclient.auth import EqarApiTokenAuth
from deqarclient.csv import NestedDictReader
from deqarclient.errors import *

class DeqarClientTestCase(unittest.TestCase):

    """
    Test cases for the DEQAR client
    """

    def setUp(self):
        """ we basically need a HTTP server with test data """
        coloredlogs.install(level='DEBUG')
        print('Starting httpd ...', end='')
        self.httpd = ThreadingHTTPServer( ('localhost', 0), DeqarTestServerHandler)
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.start()
        print('done, port {}.'.format(self.httpd.server_address[1]))
        self.api_url = 'http://localhost:{}/'.format(self.httpd.server_address[1])
        self.api = EqarApi(self.api_url, authclass=EqarApiTokenAuth, token='TEST-TEST')
        print('Test server base URL: {}'.format(self.api_url))

    def tearDown(self):
        """ on tear down, we should shutdown the server """
        print('Shutting down httpd...', end='')
        self.httpd.shutdown()
        self.httpd.server_close()
        print('done.')

    def test_countries(self):
        countries = self.api.Countries
        for test in self.Tests['countries']:
            if test[1]:
                self.assertEqual(countries.get(test[0])[test[1]], test[2])
            else:
                self.assertIsNone(countries.get(test[0]))

    def test_hierarchical_types(self):
        types = self.api.HierarchicalTypes
        for test in self.Tests['hierarchical_types']:
            if test[1]:
                self.assertEqual(types.get(test[0])[test[1]], test[2])
            else:
                self.assertIsNone(types.get(test[0]))

    def test_qf_levels(self):
        levels = self.api.QfEheaLevels
        for test in self.Tests['qf_levels']:
            if test[1]:
                self.assertEqual(levels.get(test[0])[test[1]], test[2])
            else:
                self.assertIsNone(levels.get(test[0]))

    def test_level_sets(self):
        for test in self.Tests['level_sets']:
            test_set = self.api.create_qf_ehea_level_set(test[0], strict=test[3])
            self.assertEqual(test_set, test[1])
            self.assertEqual(str(test_set), test[2])
            if not test[3]:
                self.assertRaises(DataError, self.api.create_qf_ehea_level_set, test[0], strict=True)

    def test_helpers(self):
        checker = self.api.DomainChecker
        for test in self.Tests['core_domains']:
            self.assertEqual(checker.core_domain(test[1]), test[0])

    def test_csv_reader(self):
        for test in self.Tests['csv_reader']['good']:
            inreader = NestedDictReader(test[0])
            self.assertEqual(next(inreader), test[1])
        for test in self.Tests['csv_reader']['bad']:
            inreader = NestedDictReader(test)
            self.assertRaises(DataError, next, inreader)

    def test_institution_creator(self):
        self.maxDiff = None
        for test in self.Tests['institution_creator']['good']:
            self.assertEqual(self.api.create_institution(test[0]).institution, test[1])
        for test in self.Tests['institution_creator']['good_ap']:
            self.assertEqual(self.api.create_institution(test[0], other_provider=True).institution, test[1])
        for test in self.Tests['institution_creator']['bad']:
            try:
                self.assertRaisesRegex(DataError, test[1], self.api.create_institution, test[0])
            except (AssertionError):
                print("Input:\n------\n", test[0], "\n\nOutput:\n-------\n", self.api.create_institution(test[0]).institution)
                raise
        for test in self.Tests['institution_creator']['warn']:
            try:
                self.assertWarnsRegex(DataWarning, test[1], self.api.create_institution, test[0])
            except (AssertionError):
                print("Input:\n------\n", test[0], "\n\nOutput:\n-------\n", self.api.create_institution(test[0]).institution)
                raise


    """
    The following dict defines test sets. This is just to keep them separate from methods above.
    """

    Tests = dict(
        core_domains=[
            ('educon-university.de',                'www.Educon-University.de'),
            ('esak.de',                             'WWW.Esak.de'),
            ('awm-korntal.eu',                      'https://www.awm-korntal.eu/en/index'),
            ('wikipedia.org',                       'https://de.wikipedia.org/wiki/Fachhochschule_im_Deutschen_Roten_Kreuz'),
            ('wikipedia.org',                       'https://de.wikipedia.org/wiki/FH_KUNST_Arnstadt'),
            ('hfg-gmuend.de',                       'http://WWW.hfg-gmuend.de'),
            ('gisma.com',                           'HttpS://www.gisma.com/de'),
            ('hanse-college.de',                    'https://WWW.hanse-college.de/'),
            ('health-and-medical-university.de',    'https://www.health-and-medical-university.de'),
            ('hessische-ba.de',                     'Www.Hessische-BA.de'),
            ('wikipedia.org',                       'https://de.wikipedia.org/wiki/Akademie_f%C3%BCr_digitale_Medienproduktion'),
            ('heidi.github.io',                     'http://www2.heidi.github.io/'),
            ('w00940ec.kasserver.com',              'https://xyz.web.w00940ec.kasserver.com/path/to/file.html'),
            ('tuwien.ac.at',                        'https://www.tuwien.ac.at'),
            ('open.ac.uk',                          'http://www.open.ac.uk/about/main/'),
        ],
        countries=[
            ('AT', 'id', 10),
            ('SI', 'iso_3166_alpha3', 'SLO'),
            ('XY', None),
            (999, None)
        ],
        qf_levels=[
            (0, 'level', 'short cycle'),
            ('first cycle', 'code', 1),
            ('second cycle', 'id', 3),
            (999, None),
            ('unknown level', None)
        ],
        hierarchical_types=[
            (1, 'type', 'consortium'),
            ('2', 'type', 'faculty'),
            ('faculty', 'id', 2),
            ('independent faculty or school', 'type', 'independent faculty or school'),
            ('4', None),
            (None, None),
            (99, None),
        ],
        level_sets=[
            (
                [ 'first', 'secound cycle' ],
                [ { 'id': 2, 'code': 1, 'level': 'first cycle' }, { 'id': 3, 'code': 2, 'level': 'second cycle' } ],
                'QF-EHEA: 6-7',
                True
            ),(
                [ 'short cycle', 'third cycle' ],
                [ { 'id': 1, 'code': 0, 'level': 'short cycle' }, { 'id': 4, 'code': 3, 'level': 'third cycle' } ],
                'QF-EHEA: 5-8',
                True
            ),(
                [ 'EQF 5 (short cycle)', '6', '7' ],
                [ { 'id': 1, 'code': 0, 'level': 'short cycle' }, { 'id': 2, 'code': 1, 'level': 'first cycle' }, { 'id': 3, 'code': 2, 'level': 'second cycle' } ],
                'QF-EHEA: 5-6-7',
                True
            ),(
                [ 'something about spam' ],
                [ ],
                'QF-EHEA: ',
                False
            )
        ],
        institution_creator=dict(
            good=[
                ( dict( country_id='AT',
                        agency_id=11,
                        name_official='  Chinesisch-Deutsche Hochschule für Angewandte Wissenschaften an der Tongji-Universität  ',
                        name_english='Chinese-German University  ',
                        name_version='testname ',
                        acronym='ABC123',
                        website_link=' cloud.eqar.eu',
                        city='  Shanghai ',
                        founding_date=' 2000-01-01',
                        closing_date='1970 ',
                        identifier='X-CN-0012 ',
                        identifier_resource='CN national  ',
                        parent_id='DeqarINST0987  ',
                        parent_type='faculty',
                        qf_ehea_levels='short cycle, 6,7, 8  ' ),
                    {
                        'is_other_provider': False,
                        'name_primary': 'Chinese-German University',
                        'names': [ {
                            'name_official': 'Chinesisch-Deutsche Hochschule für Angewandte Wissenschaften an der Tongji-Universität',
                            'name_english': 'Chinese-German University',
                            'acronym': 'ABC123',
                            'alternative_names': [ {
                                'name': 'testname'
                            } ]
                        } ],
                        'countries': [ {
                            'country': 10,
                            'city': 'Shanghai',
                            'country_verified': True
                        } ],
                        'identifiers': [ {
                            'identifier': 'X-CN-0012',
                            'resource': 'CN national'
                        } ],
                        'hierarchical_parent': [ {
                            'institution': 987,
                            'relationship_type': 2,
                        } ],
                        'qf_ehea_levels': [
                            { 'id': 1, 'code': 0, 'level': 'short cycle' },
                            { 'id': 2, 'code': 1, 'level': 'first cycle' },
                            { 'id': 3, 'code': 2, 'level': 'second cycle' },
                            { 'id': 4, 'code': 3, 'level': 'third cycle' }
                        ],
                        'flags': [ ],
                        'website_link': 'https://cloud.eqar.eu/login',
                        'founding_date': '2000-01-01',
                        'closing_date': '1970-12-31'
                    }
                ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        acronym='LKK ',
                        website_link='http://cloud.eqar.eu/  ',
                        city='Klagenfurt / Celovec',
                        founding_date='2003 ',
                        closing_date='2089-5-11 ',
                        identifier='  4711 ',
                        agency_id=11,
                        parent_id='  ',
                        qf_ehea_level=[ '1', '2' ] ),
                    {
                        'is_other_provider': False,
                        'name_primary': 'Landeskonservatorium Kärnten',
                        'names': [ {
                            'name_official': 'Landeskonservatorium Kärnten',
                            'acronym': 'LKK'
                        } ],
                        'countries': [ {
                            'country': 17,
                            'city': 'Klagenfurt / Celovec',
                            'country_verified': True
                        } ],
                        'identifiers': [ {
                            'identifier': '4711',
                            'resource': 'local identifier',
                            'agency': 11
                        } ],
                        'qf_ehea_levels': [
                            { 'id': 2, 'code': 1, 'level': 'first cycle' },
                            { 'id': 3, 'code': 2, 'level': 'second cycle' },
                        ],
                        'flags': [ ],
                        'website_link': 'https://cloud.eqar.eu/login',
                        'founding_date': '2003-01-01',
                        'closing_date': '2089-5-11'
                    }
                )
            ],
            good_ap=[
                ( dict( country_id='AT',
                        name_official='Alternative Uni Wien',
                        name_english='Vienna Alternative "University"',
                        acronym='AUW',
                        website_link=' cloud.eqar.eu',
                        city='Vienna',
                        latitude=48.1951219,
                        longitude=16.3716378,
                        other_location=[ dict(country='BE', city='Brussels'), dict(country='SI', city='Ljubljana', latitude=46.0513491, longitude=14.5090710) ],
                        founding_date='2000-01-01',
                        identifier='X-AT-4711',
                        identifier_resource='EU-VAT',
                        type_provider='non governmental organisation',
                        qf_ehea_level=[ 'short cycle', 'EQF 6' ],
                        source_information='Austrian chamber of commerce' ),
                    {
                        'is_other_provider': True,
                        'name_primary': 'Vienna Alternative "University"',
                        'names': [ {
                            'name_official': 'Alternative Uni Wien',
                            'name_english': 'Vienna Alternative "University"',
                            'acronym': 'AUW'
                        } ],
                        'countries': [ {
                            'country': 10,
                            'city': 'Vienna',
                            'lat': 48.1951219,
                            'long': 16.3716378,
                            'country_verified': True
                        }, {
                            'country': 17,
                            'city': 'Brussels',
                            'country_verified': False
                        }, {
                            'country': 157,
                            'city': 'Ljubljana',
                            'lat': 46.0513491,
                            'long': 14.5090710,
                            'country_verified': False
                        } ],
                        'identifiers': [ {
                            'identifier': 'X-AT-4711',
                            'resource': 'EU-VAT'
                        } ],
                        'qf_ehea_levels': [
                            { 'id': 1, 'code': 0, 'level': 'short cycle' },
                            { 'id': 2, 'code': 1, 'level': 'first cycle' },
                        ],
                        'flags': [ ],
                        'organization_type': 2,
                        'website_link': 'https://cloud.eqar.eu/login',
                        'founding_date': '2000-01-01',
                        'source_of_information': 'Austrian chamber of commerce'
                    }
                ),
            ],
            bad=[
                ( dict( country='BEL',
                        name_english='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        qf_ehea_levels='1&2' ),
                    r'Institution must have an official name and a website' ),
                ( dict( country_iso='XKX',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        qf_ehea_levels='1&2' ),
                    r'Unknown country \[XKX\]' ),
                ( dict( name_official='Landeskonservatorium Kärnten',
                        name_english='Carinthian Conservatory',
                        website_link='http://www.deqar.eu/',
                        qf_ehea_levels='1&2' ),
                    r'Country needs to be specified' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        name_english='Carinthian Conservatory',
                        website_link='http://www.deqar.eu/',
                        identifier='4711' ),
                    r'Identifier needs to have an agency ID or a resource' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        parent_deqar_id='DEQARINST-4711' ),
                    r'Malformed parent_id: \[DEQARINST-4711\]' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        parent_deqar_id='DEQARINST4711',
                        parent_type='notafaculty' ),
                    r'Unknown parent_type: \[notafaculty\]' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        founding_date='2019-13-12' ),
                    r'Malformed founding_date' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        founding_date='2019-13-12' ),
                    r'Malformed founding_date' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        founding_date='12/01/1970' ),
                    r'Malformed founding_date' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        founding_date='2019-11-32' ),
                    r'Malformed founding_date' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        closing_date='2019-10' ),
                    r'Malformed closing_date' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/',
                        closing_date='11/11/2011' ),
                    r'Malformed closing_date' ),
                ( dict( country='AT',
                        name_official='Alternative Uni Wien',
                        website_link=' cloud.eqar.eu/foobar',
                        other_location=[ dict(country='XY', city='Brussels') ],
                        identifier='X-AT-4711',
                        identifier_resource='EU-VAT',
                        type_provider='private company' ),
                    r'Unknown country \[XY\]' ),
                ( dict( country_id='AT',
                        name_official='Alternative Uni Wien',
                        website_link=' cloud.eqar.eu/foobar',
                        other_location=[ dict(city='Brussels') ],
                        identifier='X-AT-4711',
                        identifier_resource='EU-VAT',
                        type_provider='private company' ),
                    r'Country needs to be specified for each location' ),
                ( dict( country_id='AT',
                        name_official='Alternative Uni Wien',
                        website_link=' cloud.eqar.eu/foobar',
                        identifier='X-AT-4711',
                        identifier_resource='EU-VAT',
                        type_provider='NGO/institute' ),
                    r'Unknown type of provider \[NGO/institute\]' ),
            ],
            warn = [
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        name_english='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/' ),
                    r' - !!! DUPLICATE NAME' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        name_english='Carinthian Conservatory',
                        name_version='Landeskonservatorium Kärnten',
                        website_link='http://www.deqar.eu/' ),
                    r' - !!! DUPLICATE NAME' ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        name_english='Carinthian Conservatory',
                        name_version='Carinthian Conservatory',
                        website_link='http://www.deqar.eu/' ),
                    r' - !!! DUPLICATE NAME' )
            ]
        ), # institutions
        csv_reader=dict(
            good=[
                (
                    [
                        "name,location[1].city,location[1].country,location[2].country,location[3].city",
                        "HEI,Aachen,DE,SI,Zagreb",
                    ],
                    {
                        "name": "HEI",
                        "location": [
                            {
                                "country": "DE",
                                "city": "Aachen"
                            },
                            {
                                "country": "SI"
                            },
                            {
                                "city": "Zagreb"
                            },
                        ]
                    }
                ),
                (
                    [
                        "name,qf_ehea_level[0],qf_ehea_level[7],obj.list[1].sublist[2],obj.list[2],obj.list[1].sublist[1],obj.list[3].a,obj.list[3].b",
                        "HEI, EQF 5,           EQF 6,           A,                     B,          C,                     D,            E",
                    ],
                    {
                        "name": "HEI",
                        "qf_ehea_level": [
                            "EQF 5",
                            "EQF 6"
                        ],
                        "obj": {
                            "list": [
                                {
                                    "sublist": [
                                        "C",
                                        "A"
                                    ]
                                },
                                "B",
                                {
                                    "a": "D",
                                    "b": "E"
                                }
                            ]
                        }
                    }
                ),
            ],
            bad=[
                [
                    "name,qf_ehea_level[0],qf_ehea_level[7],obj.list[1].sublist[2],obj.list[2],obj.list[2].sublist[1],obj.list[3].a,obj.list[3].b",
                    "HEI, EQF 5,           EQF 6,           A,                     B,          C,                     D,            E",
                ],
                [
                    "name,qf_ehea_level,qf_ehea_level[7]",
                    "HEI, EQF 5,        EQF 6",
                ],
                [
                    "name,qf_ehea_level[0],qf_ehea_level[7],obj.list[1].sublist[2],obj.list[1],obj.list[2].sublist[1],obj.list[3].a,obj.list[3].b",
                    "HEI, EQF 5,           EQF 6,           A,                     B,          C,                     D,            E",
                ]
            ]
        )
    ) # Tests

# ----------------------------------------------------------------------------------------- #

class DeqarTestServerHandler(BaseHTTPRequestHandler):

    """
    Mock DEQAR API server. Required to run tests that make API calls.
    """

    TestData = {
        r'^/adminapi/v1/select/country/?$': [
            {"id":10,"iso_3166_alpha2":"AT","iso_3166_alpha3":"AUT","name_english":"Austria"},
            {"id":17,"iso_3166_alpha2":"BE","iso_3166_alpha3":"BEL","name_english":"Belgium"},
            {"id":64,"iso_3166_alpha2":"DE","iso_3166_alpha3":"DEU","name_english":"Germany"},
            {"id":157,"iso_3166_alpha2":"SI","iso_3166_alpha3":"SLO","name_english":"Slovenia"}
        ],
        r'^/adminapi/v1/select/qf_ehea_level/?$': [
            {"id":1,"code":0,"level":"short cycle"},
            {"id":2,"code":1,"level":"first cycle"},
            {"id":3,"code":2,"level":"second cycle"},
            {"id":4,"code":3,"level":"third cycle"}
        ],
        r'^/connectapi/v1/institutions/?.*$': {
            "count":0,
            "results":[
            ]
        },
        r'^/adminapi/v1/select/institution_hierarchical_relationship_types/?$': [
            { "id": 1, "type": "consortium" },
            { "id": 2, "type": "faculty" },
            { "id": 3, "type": "independent faculty or school" },
        ],
        r'^/adminapi/v1/select/institutions/organization_type/?$': [
            { "id": 1, "type": "private company" },
            { "id": 2, "type": "non governmental organisation" },
            { "id": 3, "type": "public – private partnership" },
        ],
    }

    def _headers_ok(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def _headers_notfound(self):
        self.send_response(404)
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        for pattern in self.TestData.keys():
            if re.match(pattern, self.path):
                self._headers_ok()
                self.wfile.write(json.dumps(self.TestData[pattern]).encode())
                return
        self._headers_notfound()

    def do_POST(self):
        if re.match(r'^/accounts/get_token/?$', self.path):
            self._headers_ok()
            self.wfile.write(json.dumps({'token':'TEST-TEST-TEST'}).encode())
        else:
            self._headers_notfound()

# ----------------------------------------------------------------------------------------- #

if __name__ == "__main__":
    unittest.main()

