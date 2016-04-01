#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

from collector.acled_africa import generate_urls
from collector.parser import parse
from collector.register import create_datasets, create_resources, create_gallery_items


def main():
    '''
    Wrapper.

    '''
    server = 'http://test-data.hdx.rwlabs.org'
    objects = generate_urls()
    parsed_data = parse(objects)

    create_datasets(datasets=parsed_data['datasets'],
                    hdx_site=server, apikey=os.getenv('HDX_KEY'))

    create_resources(resources=parsed_data['resources'],
                     hdx_site=server, apikey=os.getenv('HDX_KEY'))

    create_gallery_items(gallery_items=parsed_data['gallery_items'],
                         hdx_site=server, apikey=os.getenv('HDX_KEY'))


if __name__ == '__main__':
    main()
