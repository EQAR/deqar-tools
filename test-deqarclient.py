#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import re
import unittest
import deqarclient

class DeqarClientTestCase(unittest.TestCase):

    """
    Test cases for the DEQAR client
    """

    def setUp(self):
        """ we basically need a HTTP server with test data """
        print('Starting httpd ...', end='')
        self.httpd = ThreadingHTTPServer( ('localhost', 0), DeqarTestServerHandler)
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.start()
        print('done, port {}.'.format(self.httpd.server_address[1]))
        self.api_url = 'http://localhost:{}/'.format(self.httpd.server_address[1])
        self.api = deqarclient.EqarApi(self.api_url, 'TEST-TEST')
        print('Test server base URL: {}'.format(self.api_url))

    def tearDown(self):
        """ on tear down, we should shutdown the server """
        print('Shutting down httpd...', end='')
        self.httpd.shutdown()
        self.httpd.server_close()
        print('done.')

    def test_countries(self):
        countries = self.api.get_countries()
        for test in self.Tests['countries']:
            if test[1]:
                self.assertEqual(countries.get(test[0])[test[1]], test[2])
            else:
                self.assertIsNone(countries.get(test[0]))

    def test_qf_levels(self):
        levels = self.api.get_qf_ehea_levels()
        for test in self.Tests['qf_levels']:
            if test[1]:
                self.assertEqual(levels.get(test[0])[test[1]], test[2])
            else:
                self.assertIsNone(levels.get(test[0]))

    def test_level_sets(self):
        for test in self.Tests['level_sets']:
            test_set = self.api.create_qf_ehea_level_set(test[0], verbose=True, strict=test[3])
            self.assertEqual(test_set, test[1])
            self.assertEqual(str(test_set), test[2])
            if not test[3]:
                self.assertRaises(deqarclient.DataError, self.api.create_qf_ehea_level_set, test[0], verbose=True, strict=True)

    def test_institution_creator(self):
        self.maxDiff = None
        for test in self.Tests['institution_creator']['good']:
            self.assertEqual(self.api.create_institution(test[0]).institution, test[1])
        for test in self.Tests['institution_creator']['bad']:
            try:
                self.assertRaisesRegex(deqarclient.DataError, test[1], self.api.create_institution, test[0])
            except (AssertionError):
                print("Input:\n------\n", test[0], "\n\nOutput:\n-------\n", self.api.create_institution(test[0]).institution)
                raise
        for test in self.Tests['institution_creator']['warn']:
            try:
                self.assertWarnsRegex(deqarclient.DataWarning, test[1], self.api.create_institution, test[0])
            except (AssertionError):
                print("Input:\n------\n", test[0], "\n\nOutput:\n-------\n", self.api.create_institution(test[0]).institution)
                raise

    """
    The following dict defines test sets. This is just to keep them separate from methods above.
    """

    Tests = dict(
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
        level_sets=[
            ('first, second cycle',         [ { 'qf_ehea_level': 2 }, { 'qf_ehea_level': 3 } ],                         'QF-EHEA: 6-7',     True),
            ('short cycle, third cycle',    [ { 'qf_ehea_level': 1 }, { 'qf_ehea_level': 4 } ],                         'QF-EHEA: 5-8',     True),
            ('EQF 5 (short cycle), 6, 7',   [ { 'qf_ehea_level': 1 }, { 'qf_ehea_level': 2 }, { 'qf_ehea_level': 3 } ], 'QF-EHEA: 5-6-7',   False),
            ('something about spam',        [ ],                                                                        'QF-EHEA: ',        False)
        ],
        institution_creator=dict(
            good=[
                ( dict( country_id='AT',
                        name_official='  Chinesisch-Deutsche Hochschule für Angewandte Wissenschaften an der Tongji-Universität  ',
                        name_english='Chinese-German University  ',
                        name_version='testname ',
                        acronym='ABC123',
                        website_link='https://cdhaw.tongji.edu.cn/ ',
                        city='  Shanghai ',
                        founding_date=' 2000-01-01',
                        closing_date='1970 ',
                        identifier='X-CN-0012 ',
                        resource='CN national  ',
                        agency_id=37,
                        parent_id='DeqarINST0987  ',
                        qf_ehea_levels='short cycle, 6, 7, 8  ' ),
                    {
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
                            'city': 'Shanghai'
                        } ],
                        'identifiers': [ {
                            'identifier': 'X-CN-0012',
                            'resource': 'CN national',
                            'agency': 37
                        } ],
                        'hierarchical_parent': [ {
                            'institution': 987
                        } ],
                        'qf_ehea_levels': [
                            { 'qf_ehea_level': 1 },
                            { 'qf_ehea_level': 2 },
                            { 'qf_ehea_level': 3 },
                            { 'qf_ehea_level': 4 }
                        ],
                        'flags': [ ],
                        'website_link': 'https://cdhaw.tongji.edu.cn/',
                        'founding_date': '2000-01-01',
                        'closing_date': '1970-12-31'
                    }
                ),
                ( dict( country='BEL',
                        name_official='Landeskonservatorium Kärnten',
                        acronym='LKK ',
                        website_link='http://www.deqar.eu/  ',
                        city='Klagenfurt / Celovec',
                        founding_date='2003 ',
                        closing_date='2089-5-11 ',
                        identifier='  4711 ',
                        agency_id=11,
                        parent_id='  ',
                        qf_ehea_levels='1&2' ),
                    {
                        'name_primary': 'Landeskonservatorium Kärnten',
                        'names': [ {
                            'name_official': 'Landeskonservatorium Kärnten',
                            'acronym': 'LKK'
                        } ],
                        'countries': [ {
                            'country': 17,
                            'city': 'Klagenfurt / Celovec'
                        } ],
                        'identifiers': [ {
                            'identifier': '4711',
                            'resource': 'local identifier',
                            'agency': 11
                        } ],
                        'qf_ehea_levels': [
                            { 'qf_ehea_level': 2 },
                            { 'qf_ehea_level': 3 },
                        ],
                        'flags': [ ],
                        'website_link': 'http://www.deqar.eu/',
                        'founding_date': '2003-01-01',
                        'closing_date': '2089-5-11'
                    }
                )
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
        ) # institutions
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
        ]
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

