#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
PARSER:
------

Parser of scraper data in to HDX-ready data.

'''

import logging

from hdx.utilities.loader import load_data

logger = logging.getLogger(__name__)


def enrich_dataset(dataset):
    '''
    This adds HDX required fields to the basic data.

    '''

    dataset.load_static()
    return dataset


def enrich_gallery_item(gallery_item):
    static_gallery = load_data('yaml', 'config/hdx_gallery_static.yml')
    gallery_item.update(static_gallery)
    return gallery_item
