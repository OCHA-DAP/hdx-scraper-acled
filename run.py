#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from acled import get_countries, generate_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-acled'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    base_url = configuration['base_url']
    countries_url = configuration['countries_url']
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        countries = get_countries(countries_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countries))
        for info, country in progress_storing_tempdir('ACLED', sorted(countries, key=lambda x: x['iso3']), 'iso3'):
            folder = info['folder']
            dataset, showcase = generate_dataset_and_showcase(base_url, downloader, folder, country)
            if dataset:
                dataset.update_from_yaml()
                dataset['license_other'] = dataset['license_other'].replace('\n', '  \n')  # ensure markdown has line breaks
                dataset.generate_resource_view(1)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: ACLED', batch=info['batch'])
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
