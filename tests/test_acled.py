#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for acled_africa.

"""
import copy
import datetime
from os.path import join

import pytest
from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from acled import generate_dataset_and_showcase, get_countriesdata, generate_resource_view


class TestAcled():
    countrydata = {'m49': 120, 'iso3': 'CMR', 'countryname': 'Cameroon'}
    dataset = {'maintainer': '8b84230c-e04a-43ec-99e5-41307a203a2f', 'name': 'acled-data-for-cameroon',
               'dataset_date': '01/01/1997-12/31/2018', 'groups': [{'name': 'cmr'}],
               'tags': [{'name': 'HXL'}, {'name': 'conflicts'}, {'name': 'political violence'}, {'name': 'protests'}],
               'owner_org': 'b67e6c74-c185-4f43-b561-0e114a736f19', 'data_update_frequency': '0',
               'title': 'Cameroon - Conflict Data', 'subnational': '1'}
    resource = {'description': 'Conflict data with HXL tags', 'name': 'Conflict Data for Cameroon',
                'format': 'csv', 'resource_type': 'api', 'url_type': 'api',
                'url': 'https://data.humdata.org/hxlproxy/data.csv?url=http%3A%2F%2Flala%3Fiso%3D120&name=ACLEDHXL&tagger-match-all=on&tagger-02-header=iso&tagger-02-tag=%23country%2Bcode&tagger-03-header=event_id_cnty&tagger-03-tag=%23event%2Bcode&tagger-05-header=event_date&tagger-05-tag=%23date%2Boccurred+&tagger-08-header=event_type&tagger-08-tag=%23event%2Btype&tagger-09-header=actor1&tagger-09-tag=%23group%2Bname%2Bfirst&tagger-10-header=assoc_actor_1&tagger-10-tag=%23group%2Bname%2Bfirst%2Bassoc&tagger-12-header=actor2&tagger-12-tag=%23group%2Bname%2Bsecond&tagger-13-header=assoc_actor_2&tagger-13-tag=%23group%2Bname%2Bsecond%2Bassoc&tagger-16-header=region&tagger-16-tag=%23region%2Bname&tagger-17-header=country&tagger-17-tag=%23country%2Bname&tagger-18-header=admin1&tagger-18-tag=%23adm1%2Bname&tagger-19-header=admin2&tagger-19-tag=%23adm2%2Bname&tagger-20-header=admin3&tagger-20-tag=%23adm3%2Bname&tagger-21-header=location&tagger-21-tag=%23loc%2Bname&tagger-22-header=latitude&tagger-22-tag=%23geo%2Blat&tagger-23-header=longitude&tagger-23-tag=%23geo%2Blon&tagger-25-header=source&tagger-25-tag=%23meta%2Bsource&tagger-27-header=notes&tagger-27-tag=%23description&tagger-28-header=fatalities&tagger-28-tag=%23affected%2Bkilled&header-row=1'}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'cmr', 'title': 'Cameroon'}])
        Country.countriesdata(use_live=False)

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def get_tabular_rows(url, dict_rows, headers, format=None):
                if url == 'http://haha':
                    return [{'Name': 'Cameroon', 'ACLED country-code': 'CMR', 'ISO Code': 120, 'Region-code': 'Middle Africa'}]
                elif url == 'http://lala?iso=120':
                    return [{'year': '1997'}, {'year': '2018'}]
                elif url == 'http://lala?iso=4':
                    return list()

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://haha', downloader)
        assert countriesdata == [TestAcled.countrydata]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        hxlproxy_url = Configuration.read()['hxlproxy_url']
        dataset, showcase = generate_dataset_and_showcase('http://lala?', hxlproxy_url, downloader, TestAcled.countrydata)
        assert dataset == TestAcled.dataset

        resources = dataset.get_resources()
        assert resources == [TestAcled.resource]

        assert showcase == {'name': 'acled-data-for-cameroon-showcase', 'notes': 'Conflict Data Dashboard for Cameroon',
                            'url': 'https://www.acleddata.com/dashboard/#120',
                            'tags': [{'name': 'HXL'}, {'name': 'conflicts'}, {'name': 'political violence'}, {'name': 'protests'}],
                            'title': 'Dashboard for Cameroon', 'image_url': 'https://www.acleddata.com/wp-content/uploads/2018/01/dash.png'}

        dataset, showcase = generate_dataset_and_showcase('http://lala?', hxlproxy_url, downloader, {'m49': 4, 'iso3': 'AFG', 'countryname': 'Afghanistan'})
        assert dataset is None

    def test_generate_resource_view(self):
        dataset = Dataset(TestAcled.dataset)
        resource = copy.deepcopy(TestAcled.resource)
        resource['id'] = '123'
        dataset.add_update_resource(resource)
        result = generate_resource_view(dataset)
        assert result == {'resource_id': '123', 'description': '', 'title': 'Quick Charts', 'view_type': 'hdx_hxl_preview',
                          'hxl_preview_config': '{"configVersion":4,"bites":[{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":null,"valueColumn":"#affected+killed","aggregateFunction":"sum","dateColumn":null,"comparisonValueColumn":null,"comparisonOperator":null,"filters":{},"description":""},"type":"key figure","errorMsg":null,"computedProperties":{"title":"Sum of fatalities","dataTitle":"fatalities","unit":"k"},"uiProperties":{"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Total Fatalities","postText":"deaths","numberFormat":"us","unit":"none"},"dataProperties":{},"displayCategory":"Key Figures","hashCode":-1955043658},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":"#adm1+name","valueColumn":"#affected+killed","aggregateFunction":"sum","dateColumn":null,"comparisonValueColumn":null,"comparisonOperator":null,"filters":{},"description":""},"type":"chart","errorMsg":null,"computedProperties":{"pieChart":false,"title":"Sum of fatalities by admin1","dataTitle":"fatalities"},"uiProperties":{"swapAxis":true,"showGrid":false,"color":"#1ebfb3","sortingByValue1":"DESC","sortingByCategory1":null,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Top 5 Regions for Fatalities","dataTitle":"deaths","limit":5},"dataProperties":{},"displayCategory":"Charts","hashCode":738289179},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":null,"valueColumn":"#affected+killed","aggregateFunction":"sum","dateColumn":"#date+occurred","comparisonValueColumn":null,"comparisonOperator":null,"filters":{},"description":""},"type":"timeseries","errorMsg":null,"computedProperties":{"pieChart":false,"filters":{"filterWith":[{"#affected+killed":"is not empty"}],"filterWithout":[]},"title":"Sum of fatalities by event_date","dataTitle":"fatalities"},"uiProperties":{"swapAxis":true,"showGrid":true,"color":"#0077ce","sortingByValue1":"DESC","sortingByCategory1":null,"showPoints":false,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Fatalities over Time","dataTitle":"deaths"},"dataProperties":{},"displayCategory":"Timeseries","hashCode":2126517972}],"cookbookName":"generic"}'}
