#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:05:35 2022

@author: imchugh
"""

import numpy as np
import pandas as pd
import pathlib

PATH_TO_XL = '/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'
PATH_TO_DATA = 'E:/Sites/{}/Flux/Slow'

class translator():

    def __init__(self, site):

        self.master_df = make_master_df(path=PATH_TO_XL)
        self.site_df = make_site_df(path=PATH_TO_XL, site=site)
        self.site = site
        unique_names = list(
            set(self.site_df.index.tolist()) -
            set(self.master_df.index.tolist())
            )
        if unique_names:
            raise IndexError('The following non-standard names are contained '
                             'in the site spreadsheet: {}'
                             .format(', '.join(unique_names)))

    #--------------------------------------------------------------------------
    ### METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_map(self, long_name):

        return pd.concat(
            [self.master_df.loc[long_name], self.site_df.loc[long_name]]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_list(self):

        return list(self.site_df.table_name.dropna().unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_list(self, by_table=None):

        if by_table:
            return (
                self.site_df.loc[self.site_df.table_name==by_table]
                .index
                .tolist()
                )
        return self.site_df.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_data_path(self, table):

        if not table in self.get_table_list():
            raise KeyError('Table "{}" not found!'.format(table))
        return (
            pathlib.Path(PATH_TO_DATA.format(self.site)) / '{}.dat'
            .format(table)
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------



#------------------------------------------------------------------------------
def make_master_df(path):

    def converter(val, default_val='None'):
        if len(val) > 0:
            return val
        return default_val

    df = pd.read_excel(
        path, sheet_name='master_variables', index_col='Long name',
        converters={'Variable units': converter}
    )
    df.index.name = 'long_name'
    df.rename({'Variable name': 'standard_name',
               'Variable units': 'standard_units'}, axis=1, inplace=True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_df(path, site):
    """


    Parameters
    ----------
    path : TYPE
        DESCRIPTION.
    site : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    def converter(val, default_val='None'):
        if len(val) > 0:
            return val
        return default_val

    IMPORT_LIST = ['Label', 'Variable name', 'Variable units', 'Table name',
                   'Logger name', 'Disable', 'Default value', 'Long name']

    # Create the site dataframe
    site_df = pd.read_excel(path, sheet_name=site, usecols=IMPORT_LIST,
                            converters={'Variable units': converter},
                            index_col='Long name')
    site_df = site_df.loc[np.isnan(site_df.Disable)]
    site_df.rename({'Label': 'site_label',
                    'Variable name': 'site_name',
                    'Variable units': 'site_units',
                    'Table name': 'table_name',
                    'Logger name': 'logger_name'},
                    axis=1, inplace=True)

    return site_df


    # # Write variable standard names and unit strings
    # master_df = _make_master_df(path=path)


    # site_df = (
    #     site_df.assign(
    #         variable_name=[master_df.loc[x, 'variable_name'] for x in
    #                        site_df.index.get_level_values(level='long_name')]
    #         )
    #     )

    # site_df = (
    #     site_df.assign(
    #         variable_units=[master_df.loc[x, 'variable_units'] for x in
    #                         site_df.index.get_level_values(level='long_name')]
    #         )
    #     )

    # # Write flag to determine whether raw string can be used in rtmc output
    # # strings (for example, raw variable string should not be used if
    # # conversion of units is required)
    # site_df = site_df.assign(
    #     parse_as_raw=lambda x: x.site_variable_units==x.variable_units
    #     )

    # # Write rtmc variable name for each variable (special table variable for
    # # connection status - no variable name)
    # site_df = site_df.assign(
    #     rtmc_name=lambda x: x.apply(_get_rtmc_variable_name, axis=1)
    #     )

    # # Write alias string for each variable
    # site_df = site_df.assign(
    #     alias_name=lambda x: x.apply(_make_alias, axis=1)
    #     )

    # # Write alias conversion string for each variable
    # site_df = site_df.assign(
    #     eval_string=lambda x: x.site_variable_units.map(units_dict).fillna(
    #         x.alias_name
    #         )
    #     )

    # return site_df
#------------------------------------------------------------------------------