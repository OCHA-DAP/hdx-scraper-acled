#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Registers datasets on HDX.

'''

from tqdm import tqdm

from hdx.data.upload.dataset import Dataset
from hdx.data.upload.gallery_item import GalleryItem
from hdx.data.upload.resource import Resource


def create_datasets(datasets, hdx_site, apikey, comparator):
    '''
    Create datasets on an HDX site..

    '''
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")
    print("////////////// CREATING DATASETS /////////////////")
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")

    if isinstance(datasets, list) is False:
        raise ValueError('Please provide list of dictionaries.')

    for dataset in tqdm(datasets):
        d = Dataset(dataset_object=dataset, base_url=hdx_site, apikey=apikey, comparator=comparator)
        d.create()

    print("--------------------------------------------------")


def create_resources(resources, hdx_site, apikey, comparator):
    '''
    Create resources based in an HDX site.

    '''
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")
    print("///////////// CREATING RESOURCES /////////////////")
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")

    if isinstance(resources, list) is False:
        raise ValueError('Please provide list of dictionaries.')

    for resource in tqdm(resources):
        r = Resource(resource_object=resource, base_url=hdx_site, apikey=apikey, comparator=comparator)
        r.create()

    print("--------------------------------------------------")


def create_gallery_items(gallery_items, hdx_site, apikey):
    '''
    Create gallery items based in an HDX site.

    '''
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")
    print("///////////// CREATING GALLERY ITEMS /////////////")
    print("--------------------------------------------------")
    print("//////////////////////////////////////////////////")
    print("--------------------------------------------------")

    if isinstance(gallery_items, list) is False:
        raise ValueError('Please provide list of dictionaries.')

    for gallery_item in tqdm(gallery_items):
        r = GalleryItem(gallery_item_object=gallery_item, base_url=hdx_site, apikey=apikey)
        r.create()

    print("--------------------------------------------------")
