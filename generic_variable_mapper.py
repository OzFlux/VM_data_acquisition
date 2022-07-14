#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:05:35 2022

@author: imchugh
"""

import numpy as np
import pandas as pd
import pathlib
import pdb

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
PATH_TO_XL = (
    'E:\\Cloudstor\\Network_documents\\Site_documentation\\'
    'site_variable_map_alt.xlsx'
    )
PATH_TO_DATA = 'E:\\Sites\\{}\\Flux\\Slow'
IMPORT_LIST = ['Label', 'Variable name', 'Variable units', 'Table name',
               'Logger name', 'Disable', 'Default value', 'Long name']
RENAME_DICT = {'Label': 'site_label',
               'Variable name': 'site_name',
               'Variable units': 'site_units',
               'Table name': 'table_name',
               'Logger name': 'logger_name'}
USE_LOGGER_NAME = True
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class translator():

    def __init__(self, site):

        master_df = make_master_df(path=PATH_TO_XL)
        site_df = make_site_df(path=PATH_TO_XL, site=site)
        self.site_df = site_df.join(master_df.reindex(site_df.index))
        self.site = site

    #--------------------------------------------------------------------------
    ### METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_map(self, long_name):
        """
        Returns the details of a given long name defined in the site
        spreadsheet.

        Parameters
        ----------
        long_name : str
            Standard long name for the requested variable.

        Returns
        -------
        pd.Series
            pandas series containing the following details:
                blah \
                blah \
                blah

        """

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
    def get_variable_translation(self):
        
        return (
            dict(zip(self.site_df.site_name.tolist(), 
                     self.site_df.standard_name.tolist()))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_data_path(self, table):

        if not table in self.get_table_list():
            raise KeyError('Table "{}" not found!'.format(table))
        table_to_file = self.map_table_to_source()
        file = table_to_file[table]
        return pathlib.Path(PATH_TO_DATA.format(self.site)) / file
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_table_data(self, table, standardise_vars=False):
        
        path = self.get_table_data_path(table=table)
        df = pd.read_csv(path, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                         index_col=['TIMESTAMP'], na_values='NAN', sep=',', 
                         engine='c', on_bad_lines='warn')
        df.drop_duplicates(inplace=True)
        freq = self._get_table_freq(df)
        new_index = (
            pd.date_range(start=df.index[0], end=df.index[-1], freq=freq)
            )
        df.drop('RECORD', axis=1, inplace=True)
        return df.reindex(new_index)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_table_to_source(self):
        
        return dict(zip(
            pd.unique(self.site_df['table_name']).tolist(), 
            pd.unique(self.site_df['source']).tolist()
            ))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def merge_tables(self, standardise_vars=False):
        
        tables = self.get_table_list()
        try:
            tables.remove('site_details')
        except ValueError:
            pass
        df_list, date_list, freq_list = [], [], []
        for table in tables:
            this_df = self.get_table_data(
                table=table, standardise_vars=standardise_vars
                )
            df_list.append(this_df)
            date_list += [this_df.index[0], this_df.index[-1]]
            freq_list.append(this_df.index.freq)
        if not all(x == freq_list[0] for x in freq_list):
            raise NotImplementedError(
                'Data tables have different intervals; '
                'resampling not implemented yet'
                )
        new_index = pd.date_range(
            start=np.array(date_list).min(), 
            end=np.array(date_list).max(), 
            freq=freq_list[0]
            )
        for this_df in df_list:
            this_df = this_df.reindex(new_index)
        return pd.concat(df_list)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def read_table_header(self, table):
        
        table_path = self.get_table_data_path(table=table)
        labels_list = ['Prog_info', 'var_name', 'units', 'statistic']
        output_dict = {}
        with open(table_path) as f:
            for i in range(4):
                label_name = labels_list[i]
                output_dict[label_name] = f.readline()
        return output_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_var_names(self):
        
        return dict(zip(
            self.site_df.site_name.values, self.site_df.standard_name.values
            )
            )
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def check_for_variable_columns(self, table):
        
    #     table_path = self.get_table_data_path(table=table)
    #     date_list, n_list = [], []
    #     skip_bool = True
    #     with open(table_path) as f:
    #         for line in f:
    #             if not skip_bool:
    #                 line_list = line.split(',')
    #                 n_list.append(len(line_list))
    #                 date_list.append(line_list[0])
    #             skip_bool = False
    #     return pd.DataFrame(data={'col_count': n_list}, index=date_list)
    # #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _get_table_freq(self, df):
        
        new_df = df.RECORD.reset_index()
        new_df.index = new_df.TIMESTAMP
        new_df = new_df-new_df.shift()
        new_df['minutes'] = new_df.TIMESTAMP.dt.components.minutes
        new_df = new_df.loc[(new_df.RECORD==1) & (new_df.minutes!=0)]
        interval_list = new_df.minutes.unique().tolist()
        if not len(interval_list) == 1:
            raise RuntimeError('Inconsistent interval between records!')
        return '{}T'.format(str(interval_list[0]))
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
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
def make_site_df(path, site, logger_name_in_source=USE_LOGGER_NAME):
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

    # Concatenate the logger_name and the table_name to make the file source 
    # name    
    def converter(val, default_val='None'):
        if len(val) > 0:
            return val
        return default_val

    # Concatenate the logger_name and the table_name to make the file source 
    # name (only applied if 'logger_name_in_source' arg is True)
    def func(s):
       return '{}.dat'.format('_'.join(s.tolist()))

    # Create the site dataframe
    site_df = pd.read_excel(path, sheet_name=site, usecols=IMPORT_LIST,
                            converters={'Variable units': converter},
                            index_col='Long name')
    site_df = site_df.loc[np.isnan(site_df.Disable)]
    site_df.rename(RENAME_DICT, axis=1, inplace=True)
    if logger_name_in_source:
        site_df = site_df.assign(
            source=site_df[['logger_name', 'table_name']]
            .apply(func, axis=1)
            )
    else:
        site_df = site_df.assign(
            source=site_df['table_name'].apply('{}.dat'.format, axis=1)
            )
    return site_df
#------------------------------------------------------------------------------