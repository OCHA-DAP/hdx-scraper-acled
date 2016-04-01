#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
ACLED AFRICA:
------------

Generates Africa csv and xls from the ACLED website.

'''

from datetime import timedelta

import geonamescache


def generate_urls(base_url, today, iso=None):
    '''Parse urls of the form
      CSV: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319_csv.zip
      XLSX: http://www.acleddata.com/wp-content/uploads/2016/03/ACLED-All-Africa-File_20160101-to-20160319.xlsx
    '''

    start_week = today - timedelta(days=(today.weekday() + 2) % 7)
    dataset_date = start_week.strftime('%m/%d/%Y')  # has to be MM/DD/YYYY
    start_week_url = start_week.strftime('%Y/%m')
    year = start_week_url[:4]
    year_start_url = '%s0101' % year
    year_to_start_week_url = start_week.strftime('%Y%m%d')

    filename = 'ACLED-All-Africa-File_%s-to-%s' % (year_start_url, year_to_start_week_url)
    url_minus_extension = '%s%s/%s' % (base_url, start_week_url, filename)
    csv_url = '%s_csv.zip' % url_minus_extension
    xlsx_url = '%s.xlsx' % url_minus_extension
    name = 'Africa (Realtime - %s)' % year
    gc = geonamescache.GeonamesCache()
    countries = gc.get_countries()
    if not iso:
        iso = list()
        for country in countries.values():
            if country.get('continentcode') == 'AF':
                iso.append({'id': country.get('iso3').lower()})

    objects = []
    objects.append({
        'name': name,
        'url': xlsx_url,
        'iso': iso,
        'format': 'xlsx',
        'dataset_date': dataset_date
    })
    objects.append({
        'name': name,
        'url': csv_url,
        'iso': iso,
        'format': 'zip',
        'dataset_date': dataset_date
    })
    return objects
