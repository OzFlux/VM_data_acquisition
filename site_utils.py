#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 13:37:22 2021

@author: imchugh
"""

import pandas as pd
import pathlib
import pdb
from configparser import ConfigParser

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------
BASE_LIST = ['site_details', 'base_data_path', 'base_log_path',
             'generic_RTMC_file_path']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_details(site):

    return get_site_list().loc[site]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_list():

    import_list = ['Site', 'Start year', 'Latitude', 'Longitude', 'Altitude',
                   'Time zone', 'Time step']
    convert_dict = {'Site': lambda x: x.replace(' ', '')}
    path = get_base_path(to='site_details')
    df = pd.read_excel(path, header=9, usecols=import_list,
                       converters=convert_dict)
    df.Latitude = df.Latitude.round(6)
    df.Longitude = df.Longitude.round(6)
    df.index = df.Site
    return df.drop('Site', axis=1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_path(site=None, data=None, check_exists=False):

    """Use initialisation file to extract data path for site and data type"""    

    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths.ini')
    base_data_path = config['BASE_PATH']['data']
    if site:
        base_data_path = base_data_path.replace('<site>', site)
    out_path = pathlib.Path(base_data_path)
    if data:
        if not data in config['DATA_PATH']:
            raise KeyError('data arg must be one of: {}'
                           .format(', '.join(config['DATA_PATH'])))
        data_path = config['DATA_PATH'][data]
        out_path = out_path / data_path
    if not check_exists: return out_path
    if not out_path.exists():
        raise FileNotFoundError('path does not exist')
    else:
        return out_path
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_base_path(to):
    
    """Use initialisation file to extract base paths"""
    
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths.ini')
    if not to in config['BASE_PATH']:
        raise KeyError('to arg must be one of {}'
                       .format(', '.join(config['BASE_PATH'])))
    return config['BASE_PATH'][to]
#------------------------------------------------------------------------------