#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import os
from datetime import datetime
from os.path import expanduser

import scraperwiki

from collector.acled_africa import generate_urls
from collector.parser import parse
from collector.register import create_datasets, create_resources, create_gallery_items
from collector.utilities.item import item as I
from collector.utilities.load import load_config


def main():
    '''Wrapper'''

    try:
        #
        # Setting up configuration
        #
        home = expanduser("~")
        with open('%s/.hdxkey' % home, 'r') as f:
            apikey = f.read().replace('\n', '')

            p = load_config('config/config.json')
            if p:
                objects = generate_urls(p['base_url'], datetime.now())
                parsed_data = parse(objects)

                print('--------------------------------------------------')
                print('%s HDX Site: %s' % (I('bullet'), p['hdx_site']))

                #
                # Create datasets, resources, and gallery items.
                #
                create_datasets(datasets=parsed_data['datasets'],
                                hdx_site=p['hdx_site'], apikey=apikey)
                create_gallery_items(gallery_items=parsed_data['gallery_items'],
                                     hdx_site=p['hdx_site'], apikey=apikey)
                create_resources(resources=parsed_data['resources'],
                                 hdx_site=p['hdx_site'], apikey=apikey)

    except Exception as e:
        print(e)
        return False


if __name__ == '__main__':

    if main() != False:
        print('%s acled-africa scraper finished successfully.\n' % I('success'))
        scraperwiki.status('ok')

    else:
        scraperwiki.status('error', 'Failed to register resources.')
        os.system("mail -s 'acled-africa scraper collector failed' rans@email.com")
