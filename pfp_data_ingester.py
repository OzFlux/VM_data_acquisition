# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 12:32:42 2024

@author: jcutern-imchugh
"""

import json
import pandas as pd
import pathlib
import yaml
from configobj import ConfigObj

from paths_manager import Paths
from sparql_site_details import site_details

GLOBAL_ATTRS = [
    'Conventions', 'acknowledgement', 'altitude',  'canopy_height', 'comment',
    'contact', 'data_link', 'featureType', 'fluxnet_id', 'history',
    'institution', 'latitude', 'license', 'license_name', 'longitude',
    'metadata_link', 'ozflux_link', 'publisher_name', 'references',
    'site_name', 'site_pi', 'soil', 'source', 'time_step', 'time_zone',
    'title', 'tower_height', 'vegetation'
    ]

GENERIC_GLOBAL_ATTRS = {
    'Conventions': 'CF-1.8',
    'acknowledgement': 'This work used eddy covariance data collected by '
    'the TERN Ecosystem \nProcesses facility. Ecosystem Processes would '
    'like to acknowledge the financial support of the \nAustralian Federal '
    'Government via the National Collaborative Research Infrastructure '
    'Scheme \nand the Education Investment Fund.',
    'comment': 'CF metadataOzFlux standard variable names',
    'featureType': 'timeSeries',
    'license': 'https://creativecommons.org/licenses/by/4.0/',
    'license_name': 'CC BY 4.0',
    'metadata_link': 'http://www.ozflux.org.au/monitoringsites/<site>/index.html',
    'ozflux_link': 'http://ozflux.org.au/',
    'publisher_name': 'TERN Ecosystem ProcessesOzFlux',
    }

ALIAS_DICT = {'elevation': 'altitude'}

PathsManager = Paths()
Details = site_details()

class PFPControlFileHandler():

    def __init__(self, filename):

        self.config=ConfigObj(filename)
        self.site = self.config['Global']['site_name']

    def get_variable_table(self):

        return (
            pd.concat(
                [
                    pd.DataFrame(
                        [self.config['Variables'][key]['Attr'] for key in
                         self.config['Variables'].keys()]
                        ),
                    pd.DataFrame(
                        [self.config['Variables'][key]['xl'] for key in
                         self.config['Variables'].keys()]
                        )
                    ],
                axis=1
                )
            .set_index(key for key in self.config['Variables'].keys())
            .rename({'sheet': 'table'}, axis=1)
            )

    def get_globals_series(self):

        return pd.Series(
            dict(zip(
                self.config['Global'].keys(),
                [''.join(x) for x in self.config['Global'].values()]
                ))
            )

    def write_variables_to_excel(self, xl_write_path):

        vars_df = self.get_variable_table()
        globals_series = self.get_globals_series()
        with pd.ExcelWriter(path=xl_write_path) as writer:
            globals_series.to_excel(writer, sheet_name='Global_attrs')
            vars_df.to_excel(writer, sheet_name='Variable_attrs')

    def write_variables_to_yaml(self, file_name: str=None):

        output_dir = PathsManager.get_local_resource_path(
            'L1_config_files', subdirs=['site_configs']
            )
        if file_name is None:
            full_path = output_dir / f'{self.site}_variables.yaml'
        else:
            full_path = output_dir / file_name
        data = (
            self.get_variable_table()
            .drop(['long_name', 'standard_name'], axis=1)
            .fillna('')
            .T
            .to_dict()
            )
        with open(full_path, 'w') as f:
            yaml.dump(data=data, stream=f, sort_keys=False)

class VarAttrBuilder():

    def __init__(self, site):

        pass

class L1XrConstructor():

    def __init__(self, site):

        self.global_attrs = (
            {'site_name': site} |
            get_L1_generic_global_attrs(site=site) |
            get_L1_site_global_attrs(site=site)
            )

def get_L1_site_global_attrs(site):

    subset = [
        'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step',
        'time_zone'
        ]
    return (
        Details.get_single_site_details(site=site)
        [subset]
        .rename(ALIAS_DICT)
        .to_dict()
        )

def get_L1_generic_global_attrs(site: str=None) -> dict:
    """
    Retrieve the attributes that are either not site-specific, or are
    predictably so.

    Args:
        site (optional): name of site for which to get attrs. Defaults to None.

    Returns:
        dict: the attrs.

    """

    with open(file=_get_generic_globals_file()) as f:
        rslt = json.load(f)
    if site is None:
        return rslt
    rslt['metadata_link'] = rslt['metadata_link'].replace('<site>', site)
    return rslt

def write_L1_generic_global_attrs():
    """
    Write the generic global attributes dictionary to a file in json format.

    Returns:
        None.

    """

    with open(file=_get_generic_globals_file(), mode='w', encoding='utf-8') as f:
        json.dump(GENERIC_GLOBAL_ATTRS, f, indent=4)

def _get_generic_globals_file() -> str | pathlib.Path:
    """
    Return the absolute path of the global generic attributes file.

    Returns:
        The path.

    """

    return (
        PathsManager.get_local_resource_path(resource='L1_config_files') /
        'generic_global_attrs.json'
        )
