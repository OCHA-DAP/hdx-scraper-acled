#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from datetime import datetime
from os import unlink
from os.path import join

from hdx.data.user import User
from hdx.utilities.downloader import Download

from acled_africa import generate_dataset_showcase

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')
from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    dataset, showcase, xlsx_url = generate_dataset_showcase(datetime.now(), User.read_from_hdx('acled'))
    dataset.update_from_yaml()
    dataset.create_in_hdx()
    downloader = Download()
    path = downloader.download_file(xlsx_url)
    for resource in dataset.get_resources():
        resource.update_datastore(path=path)
    unlink(path)
    showcase.update_from_yaml()
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
