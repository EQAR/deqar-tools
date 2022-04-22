# -*- coding: utf-8 -*-

import os
import logging
import requests

from getpass import getpass

class EqarApiTokenAuth:
    """ basic authorisation class """

    def __init__(self, token=None, **kwargs):
        """ instantiate with a constant token as parameter """
        self._token = token

    @property
    def token(self):
        return(self._token)

class EqarApiLoginAuth (EqarApiTokenAuth):
    """ get token with username and password """

    def __init__(self, username=None, password=None, api=None, **kwargs):
        self._token = self._login(api, username, password)

    def _login(self, api, username, password):
        r = api.session.post(api.base + '/accounts/get_token/', data={ 'username': username, 'password': password }, timeout=api.request_timeout)
        if r.status_code == requests.codes.ok:
            api.logger.debug("Login successful: {} {}".format(r.status_code, r.reason))
            return(r.json()['token'])
        else:
            api.logger.error("Error: {} {}".format(r.status_code, r.reason))
            raise Exception("DEQAR login failed.")

class EqarApiEnvAuth (EqarApiLoginAuth):
    """ use token or username/password from environment variables """

    def __init__(self, api=None, **kwargs):
        if os.getenv('DEQAR_TOKEN'):
            api.logger.debug("DEQAR_TOKEN variable set")
            self._token = os.getenv('DEQAR_TOKEN')
        elif os.getenv('DEQAR_USER') and os.getenv('DEQAR_PASSWORD'):
            api.logger.debug("Username [{}] and password from environment variable".format(os.getenv('DEQAR_USER')))
            self._token = self._login(api, os.getenv('DEQAR_USER'), os.getenv('DEQAR_PASSWORD'))
        else:
            self._token = None

class EqarApiInteractiveAuth (EqarApiEnvAuth):
    """ display an interactive login prompt if no environment vars are set """

    def __init__(self, api=None, **kwargs):
        super().__init__(api=api)
        if self._token:
            pass
        else:
            print("DEQAR login required:")
            if os.getenv('DEQAR_USER'):
                username = os.getenv('DEQAR_USER')
                print("Username [{}] from environment variable".format(os.getenv('DEQAR_USER')))
            else:
                username = input("DEQAR username: ")
            if os.getenv('DEQAR_PASSWORD'):
                password = os.getenv('DEQAR_PASSWORD')
                print("Password [***] from environment variable")
            else:
                password = getpass("DEQAR password: ")
            self._token = self._login(api, username, password)

