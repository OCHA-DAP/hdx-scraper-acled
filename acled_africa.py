#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
ACLED AFRICA:
------------

Generates Africa csv and xls from the ACLED website.

'''

from datetime import timedelta

import geonamescache
import requests
from hdx.data.dataset import Dataset
from slugify import slugify


def generate_dataset(configuration, today, iso=None):
    '''Parse urls of the form
      CSV: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319_csv.zip
      XLSX: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319.xlsx
      and create basic data for constructing a basedata
    '''

    start_week = today - timedelta(days=(today.weekday() + 2) % 7)
    dataset_date = start_week.strftime('%m/%d/%Y')  # has to be MM/DD/YYYY
    start_week_url = start_week.strftime('%Y/%m')
    year = start_week_url[:4]
    year_start_url = '%s0101' % year
    year_to_start_week_url = start_week.strftime('%Y%m%d')

    filenamestart = 'ACLED-All-Africa-File_%s-to-' % year_start_url
    filename = '%s%s' % (filenamestart, year_to_start_week_url)
    resourcename = '%sdate' % filenamestart
    url_minus_extension = '%s%s/%s' % (configuration['base_url'], start_week_url, filename)
    csv_url = '%s_csv.zip' % url_minus_extension
    csv_resourcename = '%s_csv.zip' % resourcename
    response = requests.head(csv_url)
    if response.status_code == requests.codes.NOT_FOUND:
        start_week_url = today.strftime('%Y/%m')
        url_minus_extension = '%s%s/%s' % (configuration['base_url'], start_week_url, filename)
        csv_url = '%s_csv.zip' % url_minus_extension
    response = requests.head(csv_url)
    if response.status_code == requests.codes.NOT_FOUND:
        response.raise_for_status()
    xlsx_url = '%s.xlsx' % url_minus_extension
    xlsx_resourcename = '%s.xlsx' % resourcename
    response = requests.head(xlsx_url)
    if response.status_code == requests.codes.NOT_FOUND:
        response.raise_for_status()
    name = 'Africa (Realtime - %s)' % year
    title = 'ACLED Conflict Data for %s' % name
    slugified_name = slugify(title).lower()
    gc = geonamescache.GeonamesCache()
    if not iso:
        iso = list()
        for country in gc.get_countries().values():
            if country.get('continentcode') == 'AF':
                iso.append({'id': country.get('iso3').lower()})

    dataset = Dataset(configuration, {
        'name': slugified_name,
        'title': title,
        'dataset_date': dataset_date,  # has to be MM/DD/YYYY
        'groups': iso
    })
    resources = [{
        'name': xlsx_resourcename,
        'format': 'xlsx',
        'url': xlsx_url
    }, {
        'name': csv_resourcename,
        'format': 'zipped csv',
        'url': csv_url
    }]
    for resource in resources:
        resource['description'] = resource['url'].rsplit('/', 1)[-1]
        resource['url_type'] = 'api'
        resource['resource_type'] = 'api'

    dataset.add_update_resources(resources)
    return dataset
