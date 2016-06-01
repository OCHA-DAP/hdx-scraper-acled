#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the WorldPop class.

'''
import unittest
from datetime import datetime

from hdx.configuration import Configuration

from acled_africa import generate_dataset


class TestAcledAfrica(unittest.TestCase):
    '''
    Tests for url generation.

    '''

    def setUp(self):
        self.configuration = Configuration()

    def test_generate_dataset(self):
        today = datetime.strptime('01062016', "%d%m%Y").date()
        expected_dataset = {'name': 'acled-conflict-data-for-africa-realtime-2016',
                           'title': 'ACLED Conflict Data for Africa (Realtime - 2016)',
                            'dataset_date': '05/28/2016',
                            'groups': 'country'
                            }

        actual_dataset = generate_dataset(self.configuration, today, "country")
        self.assertEquals(expected_dataset, actual_dataset)

        base_url = self.configuration['base_url']

        expected_resources = [{'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
                               'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
                               'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx' % base_url,
                               'format': 'xlsx'},
                              {
                                  'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
                                  'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
                                  'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip' % base_url,
                                  'format': 'zipped csv'}]
        self.assertEquals(expected_resources, actual_dataset.get_resources())

        expected_gallery = []
        self.assertEquals(expected_gallery, actual_dataset.get_gallery())


def test_generate_countries(self):
    today = datetime.strptime('01062016', "%d%m%Y").date()
    actual_result = generate_dataset(self.configuration, today)

    self.assertEquals(58, len(actual_result['groups']))
