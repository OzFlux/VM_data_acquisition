# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pathlib

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class paths_2():
    
    def __init__(self, placeholder='<site>', site=None):

        self._config = ConfigParser()
        self._config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
        self.site = site
        self.xl_variable_map = get_path(base_path='xl_variable_map')
        self.slow_fluxes = get_path(
            base_path='data', data_stream='flux_slow', site=self.site
            )
        base_image = get_path(base_path='site_images')
        if site:
            self.tower_image = base_image / '{}_tower.jpg'.format(site)
            self.contour_image = base_image / '{}_contour.png'.format(site)
        else:
            self.tower_image = base_image
            self.contour_image = base_image
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class paths():
    
    def __init__(self):

        self._config = ConfigParser()
        self._config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
        
    def variable_map(self, check_exists=False):
        
        path = get_path(base_path='xl_variable_map')
        if check_exists:
            self._check_exists(path=path)
        return path
    
    def slow_fluxes(self, site=None, check_exists=False):
        
        path = get_path(
            base_path='data', data_stream='flux_slow', site=site
            )
        if check_exists:
            self._check_exists(path=path)
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
    
    def _check_exists(self, path):
        
        if not path.exists():
            raise FileNotFoundError('No such path!')
            
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