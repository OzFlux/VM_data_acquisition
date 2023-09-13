# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pathlib

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

REMOTE_ALIAS_DICT = {'AliceSpringsMulga': 'AliceMulga'}

#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
class paths():

    def __init__(self):

        self._config = ConfigParser()
        self._config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
        self._placeholder = '<site>'
        self._base_locs = ['LOCAL_PATH', 'REMOTE_PATH', 'APPLICATION_PATH']
        self._stream_dict = {
            'LOCAL_PATH': 'LOCAL_DATA_STREAM',
            'REMOTE_PATH': 'REMOTE_DATA_STREAM'
            }

    #--------------------------------------------------------------------------
    ### PUBLIC METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_application_path(self, application, **kwargs):

        return self._get_path(
            base_location='APPLICATIONS', resource=application, **kwargs
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_local_path(self, **kwargs):

        return self._get_path(base_location='LOCAL_PATH', **kwargs)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_local_resource_list(self):

        return [x for x in self._config['LOCAL_PATH']]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_local_stream_list(self):

        return [x for x in self._config['LOCAL_DATA_STREAM']]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_remote_path(self, **kwargs):

        return self._get_path(base_location='REMOTE_PATH', **kwargs)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_remote_resource_list(self):

        return [x for x in self._config['REMOTE_PATH']]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_remote_stream_list(self):

        return [x for x in self._config['REMOTE_DATA_STREAM']]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_image(self, img_type, **kwargs):

        img_dict = {'tower': '<site>_tower.jpg', 'contour': '<site>_contour.png'}
        return self._get_path(
                base_location='LOCAL_PATH', resource='site_images',
                file_name=img_dict[img_type],
                **kwargs
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_stream_list(self, location='local'):

        location_dict = {
            'local': 'LOCAL_DATA_STREAM', 'remote': 'REMOTE_DATA_STREAM'
            }
        stream = location_dict[location]
        return [x for x in self._config[stream]]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### PRIVATE METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _add_subdirs(self, path, subdirs_list):

        for subdir in subdirs_list:
            path = path / subdir
        return path
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_exists(self, path):

        if not path.exists():
            raise FileNotFoundError('No such path!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_path(self, base_location, resource, stream=None, site=None,
                  subdirs=[], file_name=None, check_exists=False, as_str=False):

        if not isinstance(subdirs, list):
            if not isinstance(subdirs, str):
                raise TypeError('kwarg "subdirs" must be of type list or str!')
            subdirs = [subdirs]
        path = pathlib.Path(self._config[base_location][resource])
        elements = [] + subdirs
        if file_name:
            elements.append(file_name)
        if stream:
            stream_header = self._stream_dict[base_location]
            path = path / self._config[stream_header][stream]
        if elements:
            path = self._add_subdirs(path=path, subdirs_list=elements)
        if site:
            if base_location == 'REMOTE_PATH':
                try:
                    site = REMOTE_ALIAS_DICT[site]
                except KeyError:
                    pass
            path = pathlib.Path(self._insert_site_str(target_obj=path, site=site))
        if check_exists:
            self._check_exists(path=path)
        if as_str:
            return str(path)
        return path
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _insert_site_str(self, target_obj, site):

        if isinstance(target_obj, pathlib.Path):
            return pathlib.Path(str(target_obj).replace(self._placeholder, site))
        return target_obj.replace(self._placeholder, site)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------