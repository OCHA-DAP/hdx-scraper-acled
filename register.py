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

import scraperwiki

from hdx.acled_africa_dynamic import generate_dataset, generate_gallery_item
from hdx.acled_africa_static import enrich_dataset, enrich_gallery_item
from hdx.configuration import Configuration
from hdx.logging import setup_logging
from hdx.register import create_gallery_items

logger = logging.getLogger(__name__)
setup_logging()

def main():
    '''Wrapper'''

    try:
        #
        # Setting up configuration
        #
        configuration = Configuration(scraper_config_file='config/scraper_configuration.yml', )
        apikey = configuration.get_api_key()

        dataset = generate_dataset(configuration, datetime.now())
        gallery_item = generate_gallery_item(dataset)
        enrich_dataset(dataset)
        enrich_gallery_item(gallery_item)

        hdx_site = configuration['hdx_site']
        logger.info('--------------------------------------------------')
        logger.info('> HDX Site: %s' % hdx_site)

        #
        # Create datasets, resources, and gallery items.
        #
        dataset.create_in_hdx(lambda a, b: all(i[0] == i[1] for i in zip(a, b) if not i[0].isdigit()))
        #                create_resources(resources=dataset['resources'], hdx_site=hdx_site, apikey=apikey,
        #                                 comparator=lambda a, b: all(i[0] == i[1] for i in zip(a, b) if not i[0].isdigit()))
        #                we deal with resources when creating/updating the dataset
        create_gallery_items(gallery_items=[gallery_item], hdx_site=hdx_site, apikey=apikey)

    except Exception as e:
        logger.critical(e, exc_info=True)
        return False


if __name__ == '__main__':

    if main() != False:
        logger.info('%s acled-africa scraper finished successfully.\n')
        scraperwiki.status('ok')

    else:
        scraperwiki.status('error', 'Failed to register resources.')
# os.system("mail -s 'acled-africa scraper collector failed' rans@email.com")
