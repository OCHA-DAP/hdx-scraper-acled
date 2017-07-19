#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from datetime import datetime
from os.path import join

from hdx.facades.hdx_scraperwiki import facade

from acled_africa import generate_dataset_showcase

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    dataset, showcase = generate_dataset_showcase(datetime.now())
    dataset.update_from_yaml()
    dataset.create_in_hdx()
    for resource in dataset.get_resources():
        resource.update_datastore()
    showcase.update_from_yaml()
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'),
           smtp_config_yaml=join('config', 'smtp_configuration.yml'))
