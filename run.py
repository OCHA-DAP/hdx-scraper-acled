#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser
from time import sleep

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from acled import get_countriesdata, generate_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-acled'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    base_url = configuration['base_url']
    countries_url = configuration['countries_url']
    hxlproxy_url = configuration['hxlproxy_url']
    with Download() as downloader:
        countriesdata = get_countriesdata(countries_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in sorted(countriesdata, key=lambda x: x['iso3']):
            dataset, showcase = generate_dataset_and_showcase(base_url, hxlproxy_url, downloader, countrydata)
            if dataset:
                dataset.update_from_yaml()
                dataset['license_other'] = dataset['license_other'].replace('\n', '  \n')  # ensure markdown has line breaks
                dataset.create_in_hdx(hxl_update=False)
                dataset.generate_resource_view()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)
                sleep(1)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
