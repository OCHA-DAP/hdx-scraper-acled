#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for acled_africa.

"""
from datetime import datetime
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration

from acled_africa import generate_dataset_showcase


class TestAcledAfrica():
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_key_file=join('tests', 'fixtures', '.hdxkey'),
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    def test_generate_dataset_showcase(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        expected_dataset = {
            'name': 'acled-conflict-data-for-africa-realtime-2016',
            'title': 'ACLED Conflict Data for Africa (Realtime - 2016)',
            'dataset_date': '05/28/2016',
            'data_update_frequency': '7',
            'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}],
            'groups': [{'name': 'ago'}, {'name': 'bdi'}, {'name': 'ben'}, {'name': 'bfa'}, {'name': 'bwa'},
                       {'name': 'caf'},
                       {'name': 'civ'}, {'name': 'cmr'}, {'name': 'cod'}, {'name': 'cog'}, {'name': 'com'},
                       {'name': 'cpv'},
                       {'name': 'dji'}, {'name': 'dza'}, {'name': 'egy'}, {'name': 'eri'}, {'name': 'esh'},
                       {'name': 'eth'},
                       {'name': 'gab'}, {'name': 'gha'}, {'name': 'gin'}, {'name': 'gmb'}, {'name': 'gnb'},
                       {'name': 'gnq'},
                       {'name': 'ken'}, {'name': 'lbr'}, {'name': 'lby'}, {'name': 'lso'}, {'name': 'mar'},
                       {'name': 'mdg'},
                       {'name': 'mli'}, {'name': 'moz'}, {'name': 'mrt'}, {'name': 'mus'}, {'name': 'mwi'},
                       {'name': 'myt'},
                       {'name': 'nam'}, {'name': 'ner'}, {'name': 'nga'}, {'name': 'reu'}, {'name': 'rwa'},
                       {'name': 'sdn'},
                       {'name': 'sen'}, {'name': 'shn'}, {'name': 'sle'}, {'name': 'som'}, {'name': 'ssd'},
                       {'name': 'stp'},
                       {'name': 'swz'}, {'name': 'syc'}, {'name': 'tcd'}, {'name': 'tgo'}, {'name': 'tun'},
                       {'name': 'tza'},
                       {'name': 'uga'}, {'name': 'zaf'}, {'name': 'zmb'}, {'name': 'zwe'}],
        }
        expected_resources = [{'format': 'xlsx', 'url': 'https://github.com/mcarans/hdxscraper-acled-africa/raw/0f674693f0542afd5681fa77f8f5077c708c549b/tests/fixtures/2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx',
                               'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
                               'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx'},
                              {'format': 'zipped csv', 'url': 'https://github.com/mcarans/hdxscraper-acled-africa/raw/0f674693f0542afd5681fa77f8f5077c708c549b/tests/fixtures/2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
                               'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
                               'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip'}]
        expected_showcase = {
            'name': 'acled-conflict-data-for-africa-realtime-2016-showcase',
            'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}]
        }
        actual_dataset, actual_showcase = generate_dataset_showcase(today)
        assert actual_dataset == expected_dataset
        actual_resources = actual_dataset.get_resources()
        assert actual_resources == expected_resources
        assert actual_showcase == expected_showcase

        base_url = Configuration.read()['base_url']

        expected_resources = [{
            'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
            'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
            'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx' % base_url,
            'format': 'xlsx',
        }, {
            'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
            'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
            'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip' % base_url,
            'format': 'zipped csv',
        }]
        assert expected_resources == actual_dataset.get_resources()

    def test_generate_countries(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        actual_result, _ = generate_dataset_showcase(today)
        assert len(actual_result['groups']) == 58
