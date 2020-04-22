#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for acled.

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.path import temp_dir

from acled import generate_dataset_and_showcase, get_countries


class TestAcled():
    countrydata = {'m49': 120, 'iso3': 'CMR', 'countryname': 'Cameroon'}
    dataset = {'maintainer': '8b84230c-e04a-43ec-99e5-41307a203a2f', 'name': 'acled-data-for-cameroon',
               'dataset_date': '01/01/1997-12/31/2018', 'groups': [{'name': 'cmr'}],
               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'violence and conflict', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'protests', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'security incidents', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'owner_org': 'b67e6c74-c185-4f43-b561-0e114a736f19', 'data_update_frequency': '7',
               'title': 'Cameroon - Conflict Data', 'subnational': '1'}
    resources = [{'description': 'Conflict data with HXL tags', 'name': 'Conflict Data for Cameroon',
                  'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                 {'name': 'QuickCharts-Conflict Data for Cameroon', 'description': 'Cut down data for QuickCharts',
                  'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'cmr', 'title': 'Cameroon'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'violence and conflict'}, {'name': 'protests'}, {'name': 'security incidents'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def get_tabular_rows(url, **kwargs):
                if url == 'http://haha':
                    return ['Name', 'ACLED country-code', 'ISO Code', 'Region-code'], \
                           [{'Name': 'Cameroon', 'ACLED country-code': 'CMR', 'ISO Code': 120, 'Region-code': 'Middle Africa'}]
                elif url == 'http://lala?iso=120':
                    return ['year'], [{'year': '1997'}, {'year': '2018'}]
                elif url == 'http://lala?iso=4':
                    return None, list()

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countries('http://haha', downloader)
        assert countriesdata == [TestAcled.countrydata]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('ACLED') as folder:
            dataset, showcase = generate_dataset_and_showcase('http://lala?', downloader, folder, TestAcled.countrydata)
            assert dataset == TestAcled.dataset

            resources = dataset.get_resources()
            assert resources == TestAcled.resources

            assert showcase == {'name': 'acled-data-for-cameroon-showcase', 'notes': 'Conflict Data Dashboard for Cameroon',
                                'url': 'https://www.acleddata.com/dashboard/#120',
                                'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'violence and conflict', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'protests', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'security incidents', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                                'title': 'Dashboard for Cameroon', 'image_url': 'https://www.acleddata.com/wp-content/uploads/2018/01/dash.png'}

            dataset, showcase = generate_dataset_and_showcase('http://lala?', downloader, folder, {'m49': 4, 'iso3': 'AFG', 'countryname': 'Afghanistan'})
            assert dataset is None
