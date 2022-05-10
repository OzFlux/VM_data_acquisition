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
    df.rename({'Altitude': 'Elevation'}, axis=1, inplace=True)
    df.index = df.Site
    return df.drop('Site', axis=1)
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_data_path(site=None, data=None, sub_dirs=None, check_exists=False):

#     """Use initialisation file to extract data path for site and data type"""    

#     config = ConfigParser()
#     config.read(pathlib.Path(__file__).parent / 'paths.ini')
#     base_data_path = config['BASE_PATH']['data']
#     if site:
#         base_data_path = base_data_path.replace('<site>', site)
#     out_path = pathlib.Path(base_data_path)
#     if data:
#         if not data in config['DATA_PATH']:
#             raise KeyError('data arg must be one of: {}'
#                            .format(', '.join(config['DATA_PATH'])))
#         data_path = config['DATA_PATH'][data]
#         out_path = out_path / data_path
#     if sub_dirs:
#         out_path = out_path / sub_dirs
#     if not check_exists: return out_path
#     if not out_path.exists():
#         raise FileNotFoundError('path does not exist')
#     else:
#         return out_path
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_path(base_path, data_stream=None, sub_dirs=None, site=None,
             check_exists=False):

    """Use initialisation file to extract data path for site and data type"""    

    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths_new.ini')
    if not base_path in config['BASE_PATH']:
        raise KeyError('base_path arg must be one of: {}'
                       .format(', '.join(config['BASE_PATH'])))
    out_path = pathlib.Path(config['BASE_PATH'][base_path])
    if base_path == 'data':
        if site:
            out_path = pathlib.Path(str(out_path).replace('<site>', site))
    else:
        if not out_path.exists():
            raise FileNotFoundError('path does not exist')
        return out_path
    if data_stream:
        if not data_stream in config['DATA_STREAM']:
            pdb.set_trace()
            raise KeyError('data_stream arg must be one of: {}'
                           .format(', '.join(config['DATA_STREAM'])))
        out_path = out_path / config['DATA_STREAM'][data_stream]
        if sub_dirs:
            out_path = out_path / sub_dirs
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
    return pathlib.Path(config['BASE_PATH'][to])
#------------------------------------------------------------------------------