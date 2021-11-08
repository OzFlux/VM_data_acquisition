#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 13:37:22 2021

@author: imchugh
"""

import pandas as pd

path = '/home/unimelb.edu.au/imchugh/Temp/site_master.xls'

def get_site_details(site):

    return get_site_list().loc[site]

def get_site_list():

    import_list = ['Site', 'Start year', 'Latitude', 'Longitude', 'Altitude',
                   'Time zone', 'Time step']
    convert_dict = {'Site': lambda x: x.replace(' ', '')}
    df = pd.read_excel(path, header=9, usecols=import_list,
                       converters=convert_dict)
    df.Latitude = df.Latitude.round(6)
    df.Longitude = df.Longitude.round(6)
    df.index = df.Site
    return df.drop('Site', axis=1)