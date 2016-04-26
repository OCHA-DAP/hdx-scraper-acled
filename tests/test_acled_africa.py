#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the WorldPop class.

'''
import unittest
from datetime import datetime

from collector.acled_africa import generate_basedata


class TestAcledAfrica(unittest.TestCase):
    '''
    Tests for url generation.

    '''

    def test_generate_dataset(self):
        today = datetime.strptime('01042016', "%d%m%Y").date()
        expected_result = {'name': 'Africa (Realtime - 2016)', 'dataset_date': '03/26/2016', 'iso': 'country',
                           'resources': [{'url': 'http://a.com/2016/03/ACLED-All-Africa-File_20160101-to-20160326.xlsx',
                                          'format': 'xlsx'},
                                         {
                                             'url': 'http://a.com/2016/03/ACLED-All-Africa-File_20160101-to-20160326_csv.zip',
                                             'format': 'zipped csv'}]}

        actual_result = generate_basedata('http://a.com/', today, "country")

        self.assertEquals(expected_result, actual_result)

    def test_generate_countries(self):
        today = datetime.strptime('01042016', "%d%m%Y").date()
        actual_result = generate_basedata('http://a.com/', today)

        self.assertEquals(58, len(actual_result['iso']))
