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

from collector.acled_africa_dynamic import generate_dataset, generate_gallery_item
from collector.acled_africa_static import enrich_dataset, enrich_gallery_item
from collector.register import create_datasets, create_gallery_items
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

            configuration = load_config('config/config.json')
            if configuration:
                dataset = generate_dataset(configuration['base_url'], datetime.now())
                gallery_item = generate_gallery_item(dataset)
                enrich_dataset(dataset)
                enrich_gallery_item(gallery_item)

                hdx_site = configuration['hdx_site']
                print('--------------------------------------------------')
                print('%s HDX Site: %s' % (I('bullet'), hdx_site))

                #
                # Create datasets, resources, and gallery items.
                #
                create_datasets(datasets=[dataset], hdx_site=hdx_site, apikey=apikey,
                                comparator=lambda a, b: all(i[0] == i[1] for i in zip(a, b) if not i[0].isdigit()))
                #                create_resources(resources=dataset['resources'], hdx_site=hdx_site, apikey=apikey,
                #                                 comparator=lambda a, b: all(i[0] == i[1] for i in zip(a, b) if not i[0].isdigit()))
                #                we deal with resources when creating/updating the dataset
                create_gallery_items(gallery_items=[gallery_item], hdx_site=hdx_site, apikey=apikey)
            else:
                return False

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
