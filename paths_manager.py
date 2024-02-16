# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pandas as pd
import pathlib

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

REMOTE_ALIAS_DICT = {
    'AliceSpringsMulga': 'AliceMulga', 'Longreach': 'MitchellGrassRangeland'
    }
BASE_LOCS = ['LOCAL_PATH', 'REMOTE_PATH', 'APPLICATION_PATH']
STREAM_DICT = {
    'LOCAL_PATH': 'LOCAL_DATA_STREAM', 'REMOTE_PATH': 'REMOTE_DATA_STREAM'
    }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class GenericPaths():

    def __init__(self):

        self._Paths = paths()
        self.local_resources = self._get_generic_resources()
        self.applications = self._get_applications()

        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_generic_resources(self):

        return (
            pd.Series({
                resource: self._Paths.get_local_path(resource=resource)
                for resource in self._Paths.get_local_resource_list()
                })
            .drop(['data', 'logs'])
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_applications(self):

        return pd.Series({
            app: self._Paths.get_application_path(application=app) for app in
            self._Paths._config['APPLICATIONS']}
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SitePaths(GenericPaths):

    def __init__(self, site):

        super().__init__()
        self.site = site
        self.local_data = self._get_local_data_paths()
        self.remote_data = self._get_remote_data_paths()
        self.local_logs = self._get_site_log_paths()


    def _get_local_data_paths(self):

        return pd.Series({
            stream: self._Paths.get_local_path(
                resource='data', stream=stream, site=self.site
                )
            for stream in self._Paths.get_local_stream_list()
            })

    #--------------------------------------------------------------------------
    def _get_remote_data_paths(self):

        return pd.Series({
            resource: self._Paths.get_remote_path(
                resource=resource, site=self.site
                )
            for resource in self._Paths.get_remote_resource_list()
            })
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_log_paths(self):

        return self._Paths.get_local_path(resource='logs', site='Calperum')
    #--------------------------------------------------------------------------

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
    def get_local_resource_paths(self, site, incl_all=False):

        return {
            resource: self.get_local_path(resource=resource)
            for resource in self.get_local_resource_list()
            }
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