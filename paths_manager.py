# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pathlib

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class paths():
    
    def __init__(self, site=None):

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