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

from acled_africa import generate_dataset
from hdx.collector.scraperwiki import wrapper

logger = logging.getLogger(__name__)


def main(configuration):
    '''Wrapper'''

    dataset = generate_dataset(configuration, datetime.now())
    dataset.load_static()
    dataset.create_in_hdx()

if __name__ == '__main__':
    wrapper(main)
