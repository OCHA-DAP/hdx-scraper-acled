#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
RESOURCE:
--------

Resource item; defines all logic for creating,
updating, and checking resources.

'''
import json
import logging

import requests

from hdx.configuration import Configuration
from hdx.utilities.loader import load_data_into_existing_dict

logger = logging.getLogger(__name__)


class Resource:
    '''
    Resource class.

    '''

    def __init__(self, configuration: Configuration):
        self.data = dict()
        base_url = configuration.get_hdx_site()
        self.url = {
            'base': base_url,
            'show': base_url + '/api/action/resource_show?id=',
            'update': base_url + '/api/3/action/resource_update?id=',
            'create': base_url + '/api/action/resource_create?id='
        }
        self.headers = {
            'X-CKAN-API-Key': configuration.get_api_key(),
            'content-type': 'application/json'
        }

    def clear_data(self):
        self.data = dict()

    def add_data(self, input_type: str, input_data):
        self.data = load_data_into_existing_dict(self.data, input_type, input_data)

    def add_static_data(self, input_type: str = 'yaml', static_data='config/hdx_resource_static.yml'):
        self.add_data(input_type, static_data)

    def _check(self):
        '''
        Checks if the resource exists in HDX.

        '''
        check = requests.get(
            self.url['show'] + self.data['name'],
            headers=self.headers, auth=('dataproject', 'humdata')).json()

        if check['success'] is True and len(check['result']['resources']) > 0:
            return {
                'exists': True,
                'state': check['result']['state']
            }
        else:
            return {
                'exists': False,
                'resources': [],
                'state': 'Nonexistent'
            }

    def update(self, state=None):
        '''
        Updates a resource on HDX.

        '''
        if not state:
            state = self._check()

        for field in self.data:
            state[field] = self.data[field]

        r = requests.post(
            self.url['update'], data=json.dumps(state),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code // 100 == 2:
            logger.info("updated successfully %s" % self.data['name'])
        else:
            logger.error('failed to update %s\n%s' % (self.data['name'], r.text))

    def create(self):
        '''
        Creates a resource on HDX.

        '''
        state = self._check()
        if state['exists'] is True:
            logger.warning("Dataset exists. Updating. %s" % self.data['name'])
            self.update()
            return

        r = requests.post(
            self.url['create'], data=json.dumps(self.data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code // 100 == 2:
            logger.info("created successfully %s" % self.data['name'])
        else:
            logger.error('failed to create %s\n%s' % (self.data['name'], r.text))
