#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import click
import os
from getpass import getpass

class EqarApi:

    def __init__(self, base, token=None):
        self.session            = requests.Session()
        self.base               = base.rstrip('/')
        self.webapi             = '/webapi/v2'
        self.request_timeout    = 5

        self.session.headers.update({
            'user-agent': 'deqar-api-client/0.1 ' + self.session.headers['User-Agent'],
            'accept': 'application/json'
        })

        if token:
            self.session.headers.update({ 'authorization': 'Bearer ' + token })
        elif 'DEQAR_TOKEN' in os.environ and os.environ['DEQAR_TOKEN']:
            click.secho("DEQAR_TOKEN variable set", bold=True)
            self.session.headers.update({ 'authorization': 'Bearer ' + os.environ['DEQAR_TOKEN'] })
        else:
            self.session.headers.update({ 'authorization': 'Bearer ' + self.login() })

    def login(self):
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

        return(self.get(self.webapi + "/browse/institutions/", kwargs))

    def get_institution(self, id):

        return(self.get(self.webapi + "/browse/institutions/{:d}".format(id), None))

class Countries:

    def __init__(self, api):
        self.countries = api.get("/adminapi/v1/select/country/")

    def get(self, which):
        if type(which) == str and which.isdigit():
            which = int(which)
        for c in self.countries:
            if which in [ c['id'], c['iso_3166_alpha2'], c['iso_3166_alpha3'] ]:
                return c

class QfEheaLevels:

    def __init__(self, api):
        self.levels = api.get("/adminapi/v1/select/qf_ehea_level/")

    def get(self, which):
        if type(which) == str and which.isdigit():
            which = int(which)
        for l in self.levels:
            if which in [ l['code'], l['level'] ]:
                return l

