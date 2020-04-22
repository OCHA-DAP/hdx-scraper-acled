#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
ACLED:
-----

Generates HXlated API urls from the ACLED website.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'event_id_cnty': '#event+code', 'event_date': '#date+occurred', 'year': '#date+year',
           'event_type': '#event+type', 'actor1': '#group+name+first', 'assoc_actor_1': '#group+name+first+assoc',
           'actor2': '#group+name+second', 'assoc_actor_2': '#group+name+second+assoc', 'region': '#region+name',
           'country': '#country+name', 'admin1': '#adm1+name', 'admin2': '#adm2+name', 'admin3': '#adm3+name',
           'location': '#loc+name', 'latitude': '#geo+lat', 'longitude': '#geo+lon', 'source': '#meta+source',
           'notes': '#description', 'fatalities': '#affected+killed', 'iso3': '#country+code'}


def get_countries(countries_url, downloader):
    countries = list()
    headers, iterator = downloader.get_tabular_rows(countries_url, headers=1, dict_form=True, format='xlsx')
    for row in iterator:
        m49 = row['ISO Code']
        if not m49:
            continue
        iso3 = Country.get_iso3_from_m49(m49)
        countryname = Country.get_country_name_from_iso3(iso3)
        countries.append({'m49': m49, 'iso3': iso3, 'countryname': countryname})
    return countries


def generate_dataset_and_showcase(base_url, downloader, folder, country):
    countryname = country['countryname']
    title = '%s - Conflict Data' % countryname
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify('ACLED Data for %s' % countryname).lower()
    countryiso = country['iso3']
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('8b84230c-e04a-43ec-99e5-41307a203a2f')
    dataset.set_organization('b67e6c74-c185-4f43-b561-0e114a736f19')
    dataset.set_expected_update_frequency('Every week')
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ['hxl', 'violence and conflict', 'protests', 'security incidents']
    dataset.add_tags(tags)

    url = '%siso=%d' % (base_url, country['m49'])
    filename = 'conflict_data_%s.csv' % countryiso
    resourcedata = {
        'name': 'Conflict Data for %s' % countryname,
        'description': 'Conflict data with HXL tags'
    }
    quickcharts = {'cutdown': 2, 'cutdownhashtags': ['#date+year', '#adm1+name', '#affected+killed']}
    success, results = dataset.download_and_generate_resource(downloader, url, hxltags, folder, filename, resourcedata,
                                                              yearcol='year', quickcharts=quickcharts)
    if success is False:
        logger.warning('%s has no data!' % countryname)
        return None, None

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Dashboard for %s' % country['countryname'],
        'notes': 'Conflict Data Dashboard for %s' % country['countryname'],
        'url': 'https://www.acleddata.com/dashboard/#%03d' % country['m49'],
        'image_url': 'https://www.acleddata.com/wp-content/uploads/2018/01/dash.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase
