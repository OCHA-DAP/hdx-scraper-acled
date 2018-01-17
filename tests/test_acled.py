#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for acled_africa.

"""
import datetime
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from acled import generate_dataset_and_showcase, get_countriesdata


class TestAcledAfrica():
    countrydata = {'m49': 4, 'iso3': 'AFG', 'countryname': 'Afghanistan'}

    @pytest.fixture(scope='function')
    def configuration(self, africa):
        Configuration._create(hdx_key_file=join('tests', 'fixtures', '.hdxkey'),
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations(africa)
        Country.countriesdata(use_live=False)

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/GHO?format=json':
                    def fn():
                        return {'dimension': [{'code': [{'display': 'Life expectancy at birth (years)',
                                                         'url': 'http://apps.who.int/gho/indicatorregistry/App_Main/view_indicator.aspx?iid=65',
                                                         'attr': [{'category': 'DISPLAY_FR', 'value': 'Esperance de vie a la naissance (ans)'},
                                                                  {'category': 'DISPLAY_ES', 'value': 'Esperanza de vida al nacer'},
                                                                  {'category': 'DEFINITION_XML', 'value': 'http://apps.who.int/gho/indicatorregistryservice/publicapiservice.asmx/IndicatorGetAsXml?profileCode=WHO&applicationCode=System&languageAlpha2=en&indicatorId=65'},
                                                                  {'category': 'CATEGORY', 'value': 'Sustainable development goals'},
                                                                  {'category': 'CATEGORY', 'value': 'something and another'},
                                                                  {'category': 'RENDERER_ID', 'value': 'RENDER_2'}],
                                                         'display_sequence': 10, 'label': 'WHOSIS_000001'}]}]}
                    response.json = fn
                return response

            @staticmethod
            def get_tabular_rows(url, dict_rows, headers):
                if url == 'http://haha':
                    return [{'End Date': 'Real Time Coding', 'Updated and Backdated': None, 'Country': 'Afghanistan',
                             'Start Date': datetime.datetime(2017, 1, 1, 0, 0), 'Region': 'Southern Asia'}]

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://haha', downloader)
        assert countriesdata == [TestAcledAfrica.countrydata]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        acled_url = Configuration.read()['acled_url']
        hxlproxy_url = Configuration.read()['hxlproxy_url']
        dataset, showcase = generate_dataset_and_showcase(acled_url, hxlproxy_url, downloader, TestAcledAfrica.countrydata)
        assert dataset == {'groups': [{'name': 'afg'}], 'title': 'Afghanistan - Health Indicators',
                           'tags': [{'name': 'indicators'}, {'name': 'World Health Organization'}],
                           'data_update_frequency': '365', 'dataset_date': '01/01/1992-12/31/2015',
                           'name': 'who-data-for-afghanistan', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'owner_org': 'hdx'}

        resources = dataset.get_resources()
        assert resources == [{'format': 'csv', 'name': 'Life expectancy at birth (years)',
                              'description': '[Indicator metadata](http://apps.who.int/gho/indicatorregistry/App_Main/view_indicator.aspx?iid=65)',
                              'url': 'http://papa/GHO/WHOSIS_000001.csv?filter=COUNTRY:AFG&profile=verbose'}]
        assert showcase == {'image_url': 'http://www.who.int/sysmedia/images/countries/afg.gif',
                            'url': 'http://www.who.int/countries/afg/en/',
                            'tags': [{'name': 'indicators'}, {'name': 'World Health Organization'}],
                            'notes': 'Health indicators for Afghanistan', 'name': 'who-data-for-afghanistan-showcase',
                            'title': 'Indicators for Afghanistan'}
