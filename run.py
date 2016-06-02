#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import logging
from datetime import datetime

from hdx.collector.scraperwiki import wrapper

from acled_africa import generate_dataset

logger = logging.getLogger(__name__)


def main(configuration):
    '''Wrapper'''

    dataset = generate_dataset(configuration, datetime.now())
    dataset.update_yaml()
    dataset.create_in_hdx()

if __name__ == '__main__':
    wrapper(main)
