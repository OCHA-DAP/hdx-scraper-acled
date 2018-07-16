#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from acled import get_countriesdata, generate_dataset_and_showcase, generate_resource_view

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    acled_url = Configuration.read()['acled_url']
    countries_url = Configuration.read()['countries_url']
    hxlproxy_url = Configuration.read()['hxlproxy_url']
    with Download() as downloader:
        countriesdata = get_countriesdata(countries_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in sorted(countriesdata, key=lambda x: x['iso3']):
            dataset, showcase = generate_dataset_and_showcase(acled_url, hxlproxy_url, downloader, countrydata)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                resource_view = generate_resource_view(dataset)
                resource_view.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', user_agent_config_yaml=join(expanduser('~'), '.acleduseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))
