#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the WorldPop class.

'''
from datetime import datetime
from os.path import join

import pytest
from hdx.configuration import Configuration

from acled_africa import generate_dataset


class TestAcledAfrica():
    '''
    Tests for url generation.

    '''

    @pytest.fixture(scope="session")
    def configuration(self):
        return Configuration(hdx_key_file=join('fixtures', '.hdxkey'))

    def test_generate_dataset(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        expected_dataset = {'name': 'acled-conflict-data-for-africa-realtime-2016',
                           'title': 'ACLED Conflict Data for Africa (Realtime - 2016)',
                            'dataset_date': '05/28/2016',
                            'groups': 'country'
                            }

        actual_dataset = generate_dataset(configuration, today, "country")
        assert expected_dataset == actual_dataset

        base_url = configuration['base_url']

        expected_resources = [{'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
                               'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
                               'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx' % base_url,
                               'format': 'xlsx'},
                              {
                                  'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
                                  'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
                                  'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip' % base_url,
                                  'format': 'zipped csv'}]
        assert expected_resources == actual_dataset.get_resources()

        expected_gallery = []
        assert expected_gallery == actual_dataset.get_gallery()

    def test_generate_countries(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        actual_result = generate_dataset(configuration, today)

        assert len(actual_result['groups']) == 58
