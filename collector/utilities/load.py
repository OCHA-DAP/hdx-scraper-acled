#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
LOAD CONFIGURATION:
------------------

Script designed to load the configuration file
from disk.

'''
import json

from collector.utilities.item import item


def load_config(config_path='config/config.json', verbose=True):
    '''
    Load configuration parameters.

    '''
    try:
        with open(config_path) as json_file:
            config = json.load(json_file)

    except Exception as e:
        print("%s Couldn't load configuration." % item('error'))
        if verbose:
            print(e)
        return False

    return config
