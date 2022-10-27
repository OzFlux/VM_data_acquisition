# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pathlib
import pdb

#------------------------------------------------------------------------------
class paths():
    
    def __init__(self):

        self._config = ConfigParser()
        self._config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
        self._placeholder = '<site>'
        
    def variable_map(self, check_exists=False):
        
        path = get_path(base_path='xl_variable_map')
        if check_exists:
            self._check_exists(path=path)
        return path

    def get_application_path(self, application, check_exists=True, as_str=False):
        
        path = pathlib.Path(self._config['APPLICATIONS'][application])
        if check_exists:
            self._check_exists(path=path)
        if as_str:
            return str(path)
        return path
    
    def get_remote_path(self, resource, stream=None, site=None):
        
        path = self._config['REMOTE_PATH'][resource]
        if site:
            path = self._insert_site_str(path, site=site)
        if stream:
            stream_path = self._config['REMOTE_PATH'][stream]
            return pathlib.Path(path) / stream_path
        return pathlib.Path(path)
    
    def slow_fluxes(self, site=None, check_exists=False):
        
        path = get_path(
            base_path='data', data_stream='flux_slow', site=site
            )
        if check_exists:
            self._check_exists(path=path)
        return path
    
    def get_local_path(self, resource, stream=None, site=None, subdirs=[],
                       check_exists=True, as_str=False):
        
        path = self._config['BASE_PATH'][resource]
        if site:
            path = pathlib.Path(self._insert_site_str(path, site=site))
        path = pathlib.Path(path)
        if stream:
            path = path / self._config['DATA_STREAM'][stream]
        if subdirs:
            path = _add_subdirs(path=path, subdirs_list=subdirs)
        if as_str:
            return str(path)
        return path
    
    def RTMC_site_images(self, img_type=None, site=None, check_exists=False):
        
        img_dict = {'tower': '{}_tower.jpg', 'contour': '{}_contour.png'}
        base_image = get_path(base_path='site_images')
        if not img_type:
            return base_image
        if not site:
            return base_image / img_dict[img_type]
        else:
            path = base_image / img_dict[img_type].format(site)
            if check_exists:
                self._check_exists(path=path)
            return path
        
    def RTMC_snapshot_directory(self, site=None, check_exists=False):
        
        path = get_path(
            base_path='data', data_stream='flux_RTMC', site=site
            )
        if check_exists:
            self._check_exists(path=path)
        return path

        
    def RTMC_template(self, check_exists=False):
        
        return get_path(base_path='RTMC_project_template')
    
    def RTMC_data_file(self, site=None, check_exists=False):
        
        path = get_path(
            base_path='data', data_stream='flux_slow', site=site
            )
        file = '{}_merged_std.dat'
        if site:
            file = file.format(site)
            path = path / file
            if check_exists:
                try:
                    self._check_exists(path=path)
                except FileNotFoundError:
                    pdb.set_trace()
            return path
        return path / file

    def RTMC_site_details_file(self, site=None, check_exists=False):
        
        path = get_path(base_path='site_details')
        file = '{}_details.dat'
        if site:
            file = file.format(site)
            path = path / file
            if check_exists:
                self._check_exists(path=path)
            return path
        return path
        
    def _check_exists(self, path):
        
        if not path.exists():
            raise FileNotFoundError('No such path!')
            
    def _insert_site_str(self, target_str, site):
        
        return target_str.replace(self._placeholder, site)
            
#------------------------------------------------------------------------------
def _add_subdirs(path, subdirs_list):

    for subdir in subdirs_list:
        path = path / subdir
    return path


def get_path(base_path, data_stream=None, sub_dirs=None, site=None,
             check_exists=False):

    """Use initialisation file to extract data path for site and data type"""

    replacer = '<site>'
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
    if not base_path in config['BASE_PATH']:
        raise KeyError(
            'base_path arg must be one of: {}'
            .format(', '.join(config['BASE_PATH']))
            )
    out_path = pathlib.Path(config['BASE_PATH'][base_path])
    if base_path == 'data':
        if data_stream:
            if not data_stream in config['DATA_STREAM']:
                raise KeyError('data_stream arg must be one of: {}'
                               .format(', '.join(config['DATA_STREAM'])))
            out_path = out_path / config['DATA_STREAM'][data_stream]
    if sub_dirs:
        out_path = out_path / sub_dirs
    if site:
        if replacer in str(out_path):
            out_path = (
                pathlib.Path(str(out_path).replace(replacer, site))
                )
    if check_exists:
        if not out_path.exists():
            raise FileNotFoundError('path does not exist')
    return out_path
#------------------------------------------------------------------------------        
        
            
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_path(base_path, data_stream=None, sub_dirs=None, site=None,
             check_exists=False):

    """Use initialisation file to extract data path for site and data type"""

    replacer = '<site>'
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
    if not base_path in config['BASE_PATH']:
        raise KeyError(
            'base_path arg must be one of: {}'
            .format(', '.join(config['BASE_PATH']))
            )
    out_path = pathlib.Path(config['BASE_PATH'][base_path])
    if base_path == 'data':
        if data_stream:
            if not data_stream in config['DATA_STREAM']:
                raise KeyError('data_stream arg must be one of: {}'
                               .format(', '.join(config['DATA_STREAM'])))
            out_path = out_path / config['DATA_STREAM'][data_stream]
    if sub_dirs:
        out_path = out_path / sub_dirs
    if site:
        if replacer in str(out_path):
            out_path = (
                pathlib.Path(str(out_path).replace(replacer, site))
                )
    if check_exists:
        if not out_path.exists():
            raise FileNotFoundError('path does not exist')
    return out_path
#------------------------------------------------------------------------------