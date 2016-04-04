#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the WorldPop class.

'''
import unittest
from datetime import datetime

from collector.acled_africa import generate_urls


class TestAcledAfrica(unittest.TestCase):
    '''
    Tests for url generation.

    '''

    def test_generate_urls(self):
        today = datetime.strptime('01042016', "%d%m%Y").date()
        expected_result = [{'name': 'Africa (Realtime - 2016)',
                            'url': 'http://a.com/2016/03/ACLED-All-Africa-File_20160101-to-20160326.xlsx',
                            'dataset_date': '03/26/2016', 'format': 'xlsx', 'iso': 'country'},
                           {'name': 'Africa (Realtime - 2016)',
                            'url': 'http://a.com/2016/03/ACLED-All-Africa-File_20160101-to-20160326_csv.zip',
                            'dataset_date': '03/26/2016', 'format': 'zip', 'iso': 'country'}]
        actual_result = generate_urls('http://a.com/', today, "country")

        self.assertEquals(expected_result, actual_result)

    def test_generate_countries(self):
        today = datetime.strptime('01042016', "%d%m%Y").date()
        actual_result = generate_urls('http://a.com/', today)

        self.assertEquals(58, len(actual_result[0]['iso']))
