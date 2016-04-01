#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
DATASET:
-------

Dataset class; contains all logic for creating,
checking, and updating datasets.

'''
import json

import requests

from collector.utilities.item import item


class Dataset:
    '''
    Dataset class.

    '''

    def __init__(self, dataset_object, base_url, apikey):
        if apikey is None:
            raise ValueError('Please provide API key.')

        if base_url is None:
            raise ValueError('Plase provide a CKAN base URL.')

        if dataset_object is None:
            raise ValueError('Dataset object not provided.')

        self.apikey = apikey
        self.data = dataset_object
        self.url = {
            'base': base_url,
            'show': base_url + '/api/3/action/package_show?id=',
            'update': base_url + '/api/3/action/package_update?id=',
            'create': base_url + '/api/3/action/package_create?id='
        }
        self.headers = {
            'X-CKAN-API-Key': apikey,
            'content-type': 'application/json'
        }
        self.state = self._check()

    def _check(self):
        '''
        Checks if the dataset exists in HDX.

        '''
        check = requests.get(
            self.url['show'] + self.data['name'],
            headers=self.headers, auth=('dataproject', 'humdata')).json()

        if check["success"] is True:
            return {'exists': True, 'state': check['result']['state']}

        else:
            return {'exists': False, 'state': 'Nonexistent'}

    def update(self):
        '''
        Updates a dataset on HDX.

        '''
        r = requests.post(
            self.url['update'], data=json.dumps(self.data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code != 200:
            print("%s failed to create %s" % (item('error'), self.data['name']))
            print(r.text)

        else:
            print("%s updated successfully %s" % (item('success'), self.data['name']))

    def create(self):
        '''
        Creates a dataset on HDX.

        '''
        if self.state['exists'] is True:
            print("%s Dataset exists. Updating. %s" % (item('warn'), self.data['name']))
            self.update()
            return

        r = requests.post(
            self.url['create'], data=json.dumps(self.data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code != 200:
            print("%s failed to create %s" % (item('error'), self.data['name']))
            print(r.text)

        else:
            print("%s created successfully %s" % (item('success'), self.data['name']))
