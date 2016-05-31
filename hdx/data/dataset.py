# -*- coding: utf-8 -*-
'''
DATASET:
-------

Dataset class; contains all logic for creating,
checking, and updating datasets.

'''
import copy
import logging

from hdx.configuration import Configuration
from hdx.utilities.dictionary import merge_two_dictionaries
from .galleryitem import GalleryItem
from .hdxobject import HDXObject, HDXError
from .resource import Resource

logger = logging.getLogger(__name__)


class Dataset(HDXObject):
    '''
    Dataset class.

    '''
    action_url = {
        'show': 'package_show?id=',
        'update': 'package_update',
        'create': 'package_create'
    }

    def __init__(self, configuration: Configuration, initial_data=None, include_gallery=True):
        if not initial_data:
            initial_data = dict()
        super(Dataset, self).__init__(configuration, self.action_url, initial_data)
        self.include_gallery = include_gallery
        self.init_resources()
        self.init_gallery()

    def __setitem__(self, key, value):
        if key == 'resources':
            raise HDXError('Add resource using add_resource or resources using add_resources!')
        if key == 'gallery':
            raise HDXError('Add gallery item using add_galleryitem or gallery using add_gallery!')
        super(Dataset, self).__setitem__(key, value)

    def separate_resources(self):
        self._separate_hdxobjects(self.resources, 'resources', 'name', Resource)

    def separate_gallery(self):
        self._separate_hdxobjects(self.gallery, 'gallery', 'title', GalleryItem)

    def init_resources(self):
        self.resources = list()

    def add_resource(self, resource: dict):
        self._addupdate_hdxobject(self.resources, 'name', Resource, resource)

    def add_resources(self, resources: list):
        if not isinstance(resources, list):
            raise HDXError('Resources should be a list!')
        for resource in resources:
            self.add_resource(resource)

    def get_resources(self):
        return self.resources

    def init_gallery(self):
        self.gallery = list()

    def add_galleryitem(self, galleryitem: dict):
        self._addupdate_hdxobject(self.gallery, 'title', GalleryItem, galleryitem)

    def add_gallery(self, gallery: list):
        if not isinstance(gallery, list):
            raise HDXError('Gallery should be a list!')
        for galleryitem in gallery:
            self.add_galleryitem(galleryitem)

    def get_gallery(self):
        return self.gallery

    def load_static(self, input_type: str = 'yaml', static_data='config/hdx_dataset_static.yml'):
        self.load(input_type, static_data)
        self.separate_resources()
        self.separate_gallery()

    def load_from_hdx(self, id_or_name) -> bool:
        '''
        Load the dataset from HDX.

        '''

        if not self._load_from_hdx('dataset', id_or_name):
            return False
        if self.data["resources"]:
            self.old_data["resources"] = copy.deepcopy(self.resources)
            self.separate_resources()
        if self.include_gallery:
            gallery = self._get_from_hdx('gallery', self.data['id'], '%s%s' % (self.base_url,
                                                                               GalleryItem.action_url['list']))
            if gallery:
                self.data['gallery'] = gallery
                self.old_data['gallery'] = copy.deepcopy(self.gallery)
                self.separate_gallery()
        return True

    def check_required_fields(self, ignore_fields=list()):
        for field in self.configuration['dataset']['required_fields']:
            if not field in ignore_fields and not field in self.data:
                raise HDXError("Field %s is missing in dataset!" % field)

        for resource in self.resources:
            resource.check_required_fields([self.configuration['resource']['dataset_id']])
        for galleryitem in self.gallery:
            galleryitem.check_required_fields([self.configuration['galleryitem']['dataset_id']])

    def _merge_hdx_update(self, update_resources, update_gallery):
        merge_two_dictionaries(self.data, self.old_data)
        del self.data['resources']
        del self.data['gallery']
        old_resources = self.old_data.get('resources', None)
        if update_resources and old_resources:
            resource_names = set()
            for resource in self.resources:
                resource_name = resource['name']
                resource_names.add(resource_name)
                for old_resource in old_resources:
                    if resource_name == old_resource['name']:
                        logger.warning('Resource exists. Updating. %s' % resource_name)
                        merge_two_dictionaries(resource, old_resource)
                        break
            for old_resource in old_resources:
                if not old_resource['name'] in resource_names:
                    self.resources.append(old_resource)
        old_gallery = self.old_data.get('gallery', None)
        if self.resources:
            self.data['resources'] = self.resources
        self._save_to_hdx('update', 'id')
        self.init_resources()
        self.separate_resources()
        if self.include_gallery and update_gallery and old_gallery:
            self.old_data['gallery'] = copy.deepcopy(self.gallery)
            galleryitem_titles = set()
            for i, galleryitem in enumerate(self.gallery):
                galleryitem_title = galleryitem['title']
                galleryitem_titles.add(galleryitem_title)
                for new_galleryitem in old_gallery:
                    if galleryitem_title == new_galleryitem['title']:
                        logger.warning('Gallery item exists. Updating. %s' % galleryitem_title)
                        merge_two_dictionaries(galleryitem, new_galleryitem)
                        galleryitem.update_in_hdx()
            for old_galleryitem in old_gallery:
                old_galleryitem['dataset_id'] = self.data['id']
                if not old_galleryitem['title'] in galleryitem_titles:
                    old_galleryitem.create_in_hdx()
                    self.gallery.append(old_galleryitem)

    def update_in_hdx(self, update_resources=True, update_gallery=True):
        '''
        Updates a dataset in HDX.

        '''
        self._check_load_existing_object('dataset', 'name')
        self._merge_hdx_update(update_resources, update_gallery)

    def create_in_hdx(self):
        '''
        Creates a dataset in HDX.

        '''
        self.check_required_fields()
        if self._load_existing_object('dataset', 'name'):
            logger.warning('Dataset exists. Updating. %s' % self.data['name'])
            self._merge_hdx_update(True, True)
            return

        if self.resources:
            self.data['resources'] = self.resources
        self._save_to_hdx('create', 'name')
        self.init_resources()
        self.separate_resources()

        if self.include_gallery:
            self.old_data['gallery'] = copy.deepcopy(self.gallery)
            for i, galleryitem in enumerate(self.gallery):
                galleryitem['dataset_id'] = self.data['id']
                galleryitem.create_in_hdx()

    def delete_from_hdx(self):
        '''
        Deletes a dataset from HDX.

        '''
        self._delete_from_hdx('dataset', 'id')
