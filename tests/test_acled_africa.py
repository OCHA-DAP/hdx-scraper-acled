#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for acled_africa.

"""
from datetime import datetime
from os.path import join

import pytest
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations

from acled_africa import generate_dataset_showcase


class TestAcledAfrica():
    @pytest.fixture(scope='function')
    def africa(self):
        return [{'name': 'ago'}, {'name': 'bdi'}, {'name': 'ben'}, {'name': 'bfa'}, {'name': 'bwa'}, {'name': 'caf'},
                {'name': 'civ'}, {'name': 'cmr'}, {'name': 'cod'}, {'name': 'cog'}, {'name': 'com'}, {'name': 'cpv'},
                {'name': 'dji'}, {'name': 'dza'}, {'name': 'egy'}, {'name': 'eri'}, {'name': 'esh'}, {'name': 'eth'},
                {'name': 'gab'}, {'name': 'gha'}, {'name': 'gin'}, {'name': 'gmb'}, {'name': 'gnb'}, {'name': 'gnq'},
                {'name': 'ken'}, {'name': 'lbr'}, {'name': 'lby'}, {'name': 'lso'}, {'name': 'mar'}, {'name': 'mdg'},
                {'name': 'mli'}, {'name': 'moz'}, {'name': 'mrt'}, {'name': 'mus'}, {'name': 'mwi'}, {'name': 'myt'},
                {'name': 'nam'}, {'name': 'ner'}, {'name': 'nga'}, {'name': 'reu'}, {'name': 'rwa'}, {'name': 'sdn'},
                {'name': 'sen'}, {'name': 'shn'}, {'name': 'sle'}, {'name': 'som'}, {'name': 'ssd'}, {'name': 'stp'},
                {'name': 'swz'}, {'name': 'syc'}, {'name': 'tcd'}, {'name': 'tgo'}, {'name': 'tun'}, {'name': 'tza'},
                {'name': 'uga'}, {'name': 'zaf'}, {'name': 'zmb'}, {'name': 'zwe'}]

    @pytest.fixture(scope='function')
    def configuration(self, africa):
        Configuration._create(hdx_key_file=join('tests', 'fixtures', '.hdxkey'),
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations(africa)

    def test_generate_dataset_showcase(self, configuration, africa):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        dataset, showcase, xlsx_url = generate_dataset_showcase(today)
        assert dataset == {
            'name': 'acled-conflict-data-for-africa-realtime-2016',
            'title': 'Africa - Conflict Data',
            'dataset_date': '05/28/2016',
            'data_update_frequency': '7',
            'maintainer': '8b84230c-e04a-43ec-99e5-41307a203a2f',
            'owner_org': 'b67e6c74-c185-4f43-b561-0e114a736f19',
            'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}],
            'groups': africa,
        }
        resources = dataset.get_resources()
        base_url = Configuration.read()['base_url']
        xlsx_url_expected = '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528.xlsx' % base_url
        assert resources == [{
            'description': 'ACLED-All-Africa-File_20160101-to-20160528.xlsx',
            'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
            'url': xlsx_url_expected,
            'format': 'xlsx',
        }, {
            'description': 'ACLED-All-Africa-File_20160101-to-20160528_csv.zip',
            'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
            'url': '%s2016/06/ACLED-All-Africa-File_20160101-to-20160528_csv.zip' % base_url,
            'format': 'zipped csv',
        }]
        assert showcase == {
            'name': 'acled-conflict-data-for-africa-realtime-2016-showcase',
            'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}]
        }
        assert xlsx_url == xlsx_url_expected

    def test_generate_countries(self, configuration):
        today = datetime.strptime('01062016', '%d%m%Y').date()
        actual_result, _, _ = generate_dataset_showcase(today)
        assert len(actual_result['groups']) == 58
