# -*- coding: utf-8 -*-
'''
DATASET:
-------

Dataset class; contains all logic for creating,
checking, and updating datasets.

'''
import collections
import json
import logging

import requests

from hdx.configuration import Configuration
from hdx.utilities.loader import load_data_into_existing_dict

logger = logging.getLogger(__name__)


class DatasetError(Exception):
    pass


class Dataset(collections.UserDict):
    '''
    Dataset class.

    '''

    def __init__(self, configuration: Configuration, initial_data=dict()):
        super(Dataset, self).__init__(initial_data)
        self.configuration = configuration
        base_url = configuration.get_hdx_site()
        self.url = {
            'base': base_url,
            'show': base_url + '/api/3/action/package_show?id=',
            'update': base_url + '/api/3/action/package_update',
            'create': base_url + '/api/3/action/package_create'
        }
        self.headers = {
            'X-CKAN-API-Key': configuration.get_api_key(),
            'content-type': 'application/json'
        }

    def load(self, input_type: str, input_data):
        self.data = load_data_into_existing_dict(self.data, input_type, input_data)

    def load_static(self, input_type: str = 'yaml', static_data='config/hdx_dataset_static.yml'):
        self.load(input_type, static_data)

    def _check(self):
        '''
        Checks if the dataset exists in HDX.

        '''
        if not self.data:
            raise DatasetError("No data in dataset!")
        if 'name' not in self.data:
            raise DatasetError("No name field (mandatory) in dataset!")

        check = requests.get(
            self.url['show'] + self.data['name'],
            headers=self.headers, auth=('dataproject', 'humdata')).json()

        if check['success'] is True:
            return check['result']
        else:
            return None

    @staticmethod
    def simple_comparator(a, b):
        return a['name'] == b['name']

    def update_in_hdx(self, update_resources=True, comparator=simple_comparator, existing_dataset=None):
        '''
        Updates a dataset in HDX.

        '''
        if not existing_dataset:
            existing_dataset = self._check()
        if not existing_dataset:
            raise DatasetError("No existing dataset to update!")

        for field in self.data:
            if field == 'resources':
                if update_resources:
                    for existing_resource in existing_dataset['resources']:
                        for resource in self.data['resources']:
                            if comparator(existing_resource, resource):
                                for res_field in resource:
                                    existing_resource[res_field] = resource[res_field]
            else:
                existing_dataset[field] = self.data[field]

        r = requests.post(
            self.url['update'], data=json.dumps(existing_dataset),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code // 100 == 2:
            logger.info('updated successfully %s' % self.data['name'])
        else:
            logger.error('failed to update %s\n%s' % (self.data['name'], r.text))

    def create_in_hdx(self, comparator=simple_comparator):
        '''
        Creates a dataset in HDX.

        '''
        existing_dataset = self._check()
        if existing_dataset:
            logger.warning('Dataset exists. Updating. %s' % self.data['name'])
            self.update_in_hdx(True, comparator, existing_dataset)
            return

        for field in self.configuration['dataset']['required_fields']:
            if not field in self.data:
                raise DatasetError("Field %s is missing in dataset!" % field)
        if 'resources' in self.data:
            for resource in self.data['resources']:
                for field in self.configuration['resource']['required_fields']:
                    if not field in resource:
                        raise DatasetError("Field %s is missing in resource!" % field)

        r = requests.post(
            self.url['create'], data=json.dumps(self.data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code // 100 == 2:
            logger.info('created successfully %s' % self.data['name'])
        else:
            logger.error('failed to create %s\n%s' % (self.data['name'], r.text))
