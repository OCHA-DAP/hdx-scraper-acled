#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
PARSER:
------

Parser of scraper data in to HDX-ready data.

'''

from slugify import slugify


def enrich(basedata):
    '''
    This adds HDX required fields to the basic data.

    '''
    title = 'ACLED Conflict Data for %s' % basedata['name']
    slugified_name = slugify('ACLED Conflict Data for %s' % basedata['name']).lower()

    dataset = {
        'name': slugified_name,
        'title': title,
        'owner_org': 'acled',
        'author': 'acled',
        'author_email': 'c.raleigh@acleddata.com',
        'maintainer': 'acled',
        'maintainer_email': 'c.raleigh@acleddata.com',
        'license_id': 'cc-by-sa',
        'dataset_date': basedata['dataset_date'],  # has to be MM/DD/YYYY
        'subnational': 1,  # has to be 0 or 1. Default 1 for ACLED.
        'notes': """ACLED conflict and protest data for African states from 1997 – December 2015 is available in Version 6 of the the ACLED dataset. Realtime data for 2016 is collected and published on a weekly basis, and will continue to be made available through the Climate Change and African Political Stability (CCAPS) website and on this page.

Due to the realtime nature of data collection which results in occasional reporting lags, and/or insufficient detail in early event reports for inclusion in the dataset, a small number of events in the 2016 data pre-date this period. These have been coded and published for the first time in 2016 and do not duplicate any events found in the full published dataset.

Data files are updated each Monday, containing data from the previous week. The data files below include a single running file for all 2016 data, with monthly files updated on an ongoing basis.""",
        'caveats': "Due to the realtime nature of data collection which results in occasional reporting lags, and/or insufficient detail in early event reports for inclusion in the dataset, a small number of events in the 2016 data pre-date this period. These have been coded and published for the first time in 2016 and do not duplicate any events found in the full published dataset.",
        'data_update_frequency': '7',
        'methodology': 'Other',
        'methodology_other': "This page contains information about how the ACLED team collects, cleans, reviews and checks event data, with a focus on what makes ACLED unique and compatible with other data. The process of ACLED coding assures that it is accurate, comprehensive, transparent and regularly updated. ACLED-Africa data is available from 1997 and into real time. ACLED-Asia produces publicly available real-time data and continues to backdate for all states. Data will be posted as it is complete.  For more information about its methodology, please consult [ACLED's Methodology page](http://www.acleddata.com/methodology/).",
        'dataset_source': 'ACLED',
        'package_creator': 'mcarans',
        'private': False,  # has to be True or False
        'url': None,
        'state': 'active',  # always "active".
        'tags': [{'name': 'conflict'}, {'name': 'political violence'}, {'name': 'protests'}, {'name': 'war'}],
        'groups': basedata['iso']  # has to be ISO-3-letter-code. { 'id': None }
    }

    gallery_item = {
        'title': 'Dynamic Map: Political Conflict in Africa',
        'type': 'visualization',
        'description': 'The dynamic maps below have been drawn from ACLED Version 6. They illustrate key dynamics in event types, reported fatalities, and actor categories. Clicking on the maps, and selecting or de-selecting options in the legends, allows users to interactively edit and manipulate the visualisations, and export or share the finished visuals. The maps are visualised using Tableau Public.',
        'url': 'http://www.acleddata.com/visuals/maps/dynamic-maps/',
        'image_url': 'http://docs.hdx.rwlabs.org/wp-content/uploads/acled_visual.png',
        'dataset_id': slugified_name
    }

    resources = basedata['resources']
    for resource in resources:
        resource['package_id'] = slugified_name
        resource['name'] = resource['url'].rsplit('/', 1)[-1]
        resource['description'] = '%s (%s)' % (title, resource['format'])

    dataset['resources'] = resources
    return dataset, gallery_item
