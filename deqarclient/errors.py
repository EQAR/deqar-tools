# -*- coding: utf-8 -*-

class HttpError (Exception):

    """ Raised when API returns a non-2xx code """

    pass

class DataError (Exception):

    """ Raised when input data is malformed etc. """

    pass

class DataWarning (Warning):

    """ Warnings triggered by input data issues """

    pass

