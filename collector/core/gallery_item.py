#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
GALLERY ITEM:
------------

Gallery item; defines all logic for creating,
updating, and checking gallery items.

'''
import json

import requests

from collector.utilities.item import item


class GalleryItem:
    '''
    Gallery item class.

    '''

    def __init__(self, gallery_item_object, base_url, apikey):
        if apikey is None:
            raise ValueError('Please provide API key.')

        if base_url is None:
            raise ValueError('Plase provide a CKAN base URL.')

        if gallery_item_object is None:
            raise ValueError('Gallery item object not provided.')

        self.apikey = apikey
        self.data = gallery_item_object
        self.url = {
            'base': base_url,
            'show': base_url + '/api/3/action/related_list?id=',
            'create': base_url + '/api/3/action/related_create?id=',
            'delete': base_url + '/api/3/action/related_delete?id='
        }
        headers = {'X-CKAN-API-Key': apikey, 'content-type': 'application/json'}
        self.headers = {
            'X-CKAN-API-Key': apikey,
            'content-type': 'application/json'
        }
        self.state = self._check()

    def _check(self):
        '''
        Checks if a gallery item exists in HDX.

        '''
        check = requests.get(
            self.url['show'] + self.data['dataset_id'],
            headers=self.headers, auth=('dataproject', 'humdata')).json()

        if check["success"] is True and len(check['result']) > 0:
            return {
                'exists': True,
                'package_id': self.data['dataset_id'],
                'items': check['result']
            }

        else:
            return {'exists': False, 'items': 0}

    def delete(self, gallery_item_data):
        '''
        Deletes a gallery item on HDX.

        '''
        r = requests.post(
            self.url['delete'], data=json.dumps(gallery_item_data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code != 200:
            print("%s failed to delete %s" % (item('error'), self.data['dataset_id']))
            print(r.text)

        else:
            print("%s deleted successfully %s" % (item('success'), self.data['dataset_id']))

    def create(self):
        '''
        Creates a gallery item on HDX.

        '''
        if self.state['exists'] is True:
            print("%s Gallery item exists (%s). Updating. %s" % (
                item('warn'), len(self.state['items']), self.data['dataset_id']))
            for object in self.state['items']:
                self.delete(gallery_item_data=object)

        r = requests.post(
            self.url['create'], data=json.dumps(self.data),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if r.status_code != 200:
            print("%s failed to create %s" % (item('error'), self.data['dataset_id']))
            print(r.text)

        else:
            print("%s gallery item created %s" % (item('success'), self.data['dataset_id']))
