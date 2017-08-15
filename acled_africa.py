#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
ACLED AFRICA:
------------

Generates Africa csv and xls from the ACLED website.

"""
from datetime import timedelta

import requests
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.hdx_configuration import Configuration
from slugify import slugify

def generate_dataset_showcase(today):
    """Parse urls of the form
      CSV: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319_csv.zip
      XLSX: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319.xlsx
      and create basic data for constructing a basedata
    """

    dataset_date = today - timedelta(days=(today.weekday() + 2) % 7)
    start_week_url = dataset_date.strftime('%Y/%m')
    year = start_week_url[:4]
    year_start_url = '%s0101' % year
    year_to_start_week_url = dataset_date.strftime('%Y%m%d')

    filenamestart = 'ACLED-All-Africa-File_%s-to-' % year_start_url
    filename = '%s%s' % (filenamestart, year_to_start_week_url)
    resourcename = '%sdate' % filenamestart
    url_minus_extension = '%s%s/%s' % (Configuration.read()['base_url'], start_week_url, filename)
    csv_url = '%s_csv.zip' % url_minus_extension
    csv_resourcename = '%s_csv.zip' % resourcename
    response = requests.head(csv_url)
    if response.status_code != requests.codes.OK:
        start_week_url = today.strftime('%Y/%m')
        url_minus_extension = '%s%s/%s' % (Configuration.read()['base_url'], start_week_url, filename)
        csv_url = '%s_csv.zip' % url_minus_extension
        response = requests.head(csv_url)
        if response.status_code != requests.codes.OK:
            response.raise_for_status()
    xlsx_url = '%s.xlsx' % url_minus_extension
    xlsx_resourcename = '%s.xlsx' % resourcename
    response = requests.head(xlsx_url)
    if response.status_code != requests.codes.OK:
        response.raise_for_status()
    name = 'Africa (Realtime - %s)' % year
    title = 'ACLED Conflict Data for %s' % name
    slugified_name = slugify(title).lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('8b84230c-e04a-43ec-99e5-41307a203a2f')
    dataset.set_organization('b67e6c74-c185-4f43-b561-0e114a736f19')
    dataset.set_dataset_date_from_datetime(dataset_date)
    dataset.set_expected_update_frequency('Every week')
    dataset.add_continent_location('Africa')
    dataset.add_tags(['conflict', 'political violence', 'protests', 'war'])

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

    dataset.add_update_resources(resources)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name
    })
    showcase.add_tags(['conflict', 'political violence', 'protests', 'war'])
    return dataset, showcase, xlsx_url
