#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
ACLED:
-----

Generates HXlated API urls from the ACLED website.

"""
import logging
from urllib.parse import quote_plus

from hdx.data.dataset import Dataset
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)

hxlate = '&name=ACLEDHXL&tagger-match-all=on&tagger-02-header=iso&tagger-02-tag=%23country%2Bcode&tagger-03-header=event_id_cnty&tagger-03-tag=%23event%2Bcode&tagger-05-header=event_date&tagger-05-tag=%23date%2Boccurred+&tagger-08-header=event_type&tagger-08-tag=%23event%2Btype&tagger-09-header=actor1&tagger-09-tag=%23group%2Bname%2Bfirst&tagger-10-header=assoc_actor_1&tagger-10-tag=%23group%2Bname%2Bfirst%2Bassoc&tagger-12-header=actor2&tagger-12-tag=%23group%2Bname%2Bsecond&tagger-13-header=assoc_actor_2&tagger-13-tag=%23group%2Bname%2Bsecond%2Bassoc&tagger-16-header=region&tagger-16-tag=%23region%2Bname&tagger-17-header=country&tagger-17-tag=%23country%2Bname&tagger-18-header=admin1&tagger-18-tag=%23adm1%2Bname&tagger-19-header=admin2&tagger-19-tag=%23adm2%2Bname&tagger-20-header=admin3&tagger-20-tag=%23adm3%2Bname&tagger-21-header=location&tagger-21-tag=%23loc%2Bname&tagger-22-header=latitude&tagger-22-tag=%23geo%2Blat&tagger-23-header=longitude&tagger-23-tag=%23geo%2Blon&tagger-25-header=source&tagger-25-tag=%23meta%2Bsource&tagger-27-header=notes&tagger-27-tag=%23description&tagger-28-header=fatalities&tagger-28-tag=%23affected%2Bkilled&header-row=1'


def get_countriesdata(countries_url, downloader):
    countries = list()
    for row in downloader.get_tabular_rows(countries_url, dict_rows=True, headers=1, format='xlsx'):
        # country = row['Name']
        # iso3, _ = Country.get_iso3_country_code_fuzzy(country, exception=ValueError)
        # m49 = Country.get_m49_from_iso3(iso3)
        m49 = row['ISO Country Number']
        if not m49:
            continue
        iso3 = Country.get_iso3_from_m49(m49)
        countryname = Country.get_country_name_from_iso3(iso3)
        countries.append({'m49': m49, 'iso3': iso3, 'countryname': countryname})
    return countries


def generate_dataset_and_showcase(acled_url, hxlproxy_url, downloader, countrydata):
    """
      Create HXLated URLs to ACLED API
      eg. https://data.humdata.org/hxlproxy/data.csv?name=ACLEDHXL&url=https%3A//api.acleddata.com/acled/read.csv%3Flimit%3D0%26iso%3D120&tagger-match-all=on&tagger-02-header=iso&tagger-02-tag=%23country%2Bcode&tagger-03-header=event_id_cnty&tagger-03-tag=%23event%2Bcode&tagger-05-header=event_date&tagger-05-tag=%23date%2Boccurred+&tagger-08-header=event_type&tagger-08-tag=%23event%2Btype&tagger-09-header=actor1&tagger-09-tag=%23group%2Bname%2Bfirst&tagger-10-header=assoc_actor_1&tagger-10-tag=%23group%2Bname%2Bfirst%2Bassoc&tagger-12-header=actor2&tagger-12-tag=%23group%2Bname%2Bsecond&tagger-13-header=assoc_actor_2&tagger-13-tag=%23group%2Bname%2Bsecond%2Bassoc&tagger-16-header=region&tagger-16-tag=%23region%2Bname&tagger-17-header=country&tagger-17-tag=%23country%2Bname&tagger-18-header=admin1&tagger-18-tag=%23adm1%2Bname&tagger-19-header=admin2&tagger-19-tag=%23adm2%2Bname&tagger-20-header=admin3&tagger-20-tag=%23adm3%2Bname&tagger-21-header=location&tagger-21-tag=%23loc%2Bname&tagger-22-header=latitude&tagger-22-tag=%23geo%2Blat&tagger-23-header=longitude&tagger-23-tag=%23geo%2Blon&tagger-25-header=source&tagger-25-tag=%23meta%2Bsource&tagger-27-header=notes&tagger-27-tag=%23description&tagger-28-header=fatalities&tagger-28-tag=%23affected%2Bkilled&header-row=1
    """
    countryname = countrydata['countryname']
    title = '%s - Conflict Data' % countryname
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify('ACLED Data for %s' % countryname).lower()
    countryiso = countrydata['iso3']
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('8b84230c-e04a-43ec-99e5-41307a203a2f')
    dataset.set_organization('b67e6c74-c185-4f43-b561-0e114a736f19')
    dataset.set_expected_update_frequency('Live')
    dataset.add_country_location(countryiso)
    tags = ['conflict', 'political violence', 'protests', 'war', 'HXL']
    dataset.add_tags(tags)

    acled_country_url = '%siso=%d' % (acled_url, countrydata['m49'])
    url = '%surl=%s%s' % (hxlproxy_url, quote_plus(acled_country_url), hxlate)
    earliest_year = 10000
    latest_year = 0
    for row in downloader.get_tabular_rows(acled_country_url, dict_rows=True, headers=1):
        year = int(row['year'])
        if year < earliest_year:
            earliest_year = year
        if year > latest_year:
            latest_year = year

    if latest_year == 0:
        logger.warning('%s has no data!' % countryname)
        return None, None

    resource = {
        'name': 'Conflict Data for %s' % countryname,
        'description': 'Conflict data with HXL tags',
        'format': 'csv',
        'url': url
    }
    dataset.add_update_resource(resource)
    dataset.set_dataset_year_range(earliest_year, latest_year)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Dashboard for %s' % countrydata['countryname'],
        'notes': 'Conflict Data Dashboard for %s' % countrydata['countryname'],
        'url': 'https://www.acleddata.com/dashboard/#%03d' % countrydata['m49'],
        'image_url': 'https://www.acleddata.com/wp-content/uploads/2018/01/dash.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase


def generate_resource_view(dataset):
    resourceview = ResourceView({'resource_id': dataset.get_resource()['id']})
    resourceview.update_from_yaml()
    return resourceview
