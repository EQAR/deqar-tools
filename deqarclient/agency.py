# -*- coding: utf-8 -*-

from .errors import *

class Agency:

    """
    load and modify an existing agency record
    """

    def __init__(self, api, pk):
        # save api for later use
        self.api = api

        # load data
        self.data = self.api.get(f'/adminapi/v1/agencies/{pk}/')
        self.id = self.data['id']

        # adjust to structure expected for PUT
        def replace_dict_by_pk(array, item, pk = 'id'):
            for i in array:
                if type(i.get(item)) == dict:
                    i[item] = i[item].get(pk)

        replace_dict_by_pk(self.data['activities'], 'activity_type')
        replace_dict_by_pk(self.data['decisions'], 'decision_type')
        replace_dict_by_pk(self.data['focus_countries'], 'country')
        replace_dict_by_pk(self.data['memberships'], 'association')
        self.data['country'] = self.data['country']['id']
        self.data['names'] = self.data['names_actual'] + self.data['names_former']
        del self.data['names_actual']
        del self.data['names_former']

        # determine acronym_primary and name_primary
        for name in self.data['names']:
            if name['name_valid_to'] == None:
                for version in name['agency_name_versions']:
                    if version['acronym_is_primary']:
                        self.acronym_primary = version['acronym']
                    if version['name_is_primary']:
                        self.name_primary = version['name']


    def save(self, comment='changed by deqarclient.py'):
        """
        PUT the prepared institution object
        """
        data = self.data.copy()
        data['submit_comment'] = comment
        response = self.api.put(f'/adminapi/v1/agencies/{self.id}/', data)
        self.api.logger.info(str(self))
        return(response)

    def __str__(self):
        return(f"{self.id}: {self.name_primary} ({self.data['website_link']})")

