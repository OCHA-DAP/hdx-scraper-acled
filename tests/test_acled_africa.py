#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the acled_africa class.

'''
from datetime import datetime
from os.path import join

import pytest
from hdx.configuration import Configuration

from acled_africa import generate_dataset


class TestAcledAfrica():
    @pytest.fixture(scope="class")
    def configuration(self):
        return Configuration(hdx_key_file=join('fixtures', '.hdxkey'))

    def test_generate_dataset(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        expected_dataset = {
            'name': 'acled-conflict-data-for-africa-realtime-2016',
            'title': 'ACLED Conflict Data for Africa (Realtime - 2016)',
            'dataset_date': '05/28/2016',
            'data_update_frequency': '7',
            'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}],
            'groups': [{'id': 'ago'}, {'id': 'bdi'}, {'id': 'ben'}, {'id': 'bfa'}, {'id': 'bwa'}, {'id': 'caf'},
                       {'id': 'civ'}, {'id': 'cmr'}, {'id': 'cod'}, {'id': 'cog'}, {'id': 'com'}, {'id': 'cpv'},
                       {'id': 'dji'}, {'id': 'dza'}, {'id': 'egy'}, {'id': 'eri'}, {'id': 'esh'}, {'id': 'eth'},
                       {'id': 'gab'}, {'id': 'gha'}, {'id': 'gin'}, {'id': 'gmb'}, {'id': 'gnb'}, {'id': 'gnq'},
                       {'id': 'ken'}, {'id': 'lbr'}, {'id': 'lby'}, {'id': 'lso'}, {'id': 'mar'}, {'id': 'mdg'},
                       {'id': 'mli'}, {'id': 'moz'}, {'id': 'mrt'}, {'id': 'mus'}, {'id': 'mwi'}, {'id': 'myt'},
                       {'id': 'nam'}, {'id': 'ner'}, {'id': 'nga'}, {'id': 'reu'}, {'id': 'rwa'}, {'id': 'sdn'},
                       {'id': 'sen'}, {'id': 'shn'}, {'id': 'sle'}, {'id': 'som'}, {'id': 'ssd'}, {'id': 'stp'},
                       {'id': 'swz'}, {'id': 'syc'}, {'id': 'tcd'}, {'id': 'tgo'}, {'id': 'tun'}, {'id': 'tza'},
                       {'id': 'uga'}, {'id': 'zaf'}, {'id': 'zmb'}, {'id': 'zwe'}],
        }

        actual_dataset = generate_dataset(configuration, today)
        assert expected_dataset == actual_dataset

        base_url = configuration['base_url']

        expected_resources = [{
            'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
            'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
            'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx' % base_url,
            'format': 'xlsx',
            'url_type': 'api',
            'resource_type': 'api'
        }, {
            'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
            'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
            'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip' % base_url,
            'format': 'zipped csv',
            'url_type': 'api',
            'resource_type': 'api'
        }]
        assert expected_resources == actual_dataset.get_resources()

        expected_gallery = []
        assert expected_gallery == actual_dataset.get_gallery()

    def test_generate_countries(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        actual_result = generate_dataset(configuration, today)

        assert len(actual_result['groups']) == 58
