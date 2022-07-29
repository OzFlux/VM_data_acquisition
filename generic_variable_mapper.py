#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:05:35 2022

Issues:
    - currently the units in the raw file are overriden by what is contained 
      in the site spreadsheet; maybe need to reconcile these / allow choice 
      about which are used
    - need to do a comprehensive check of the files to be concatenated before
      doing the concat - pandas is good at reconciling different files but this
      could have unintended consequences

@author: imchugh
"""

import csv
import datetime as dt
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
PROG_INFO_LIST = ['format', 'station_name', 'logger_type', 'serial_num', 
                  'OS_version', 'program_name', 'program_sig', 'table_name']
SITE_PROC_LIST = ['Calperum', 'Gingin']
USE_LOGGER_NAME = True
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class var_mapper():
    
    def __init__(self):
        
        
        pass
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_grouper():
    
    def __init__(self, site, search_str):
        
        self.file_path = pathlib.Path(PATH_TO_DATA.format(site))
        if not self.file_path.exists(): 
            raise FileNotFoundError('Path does not exist!')
        self.search_str = search_str
        
    def get_file_list(self, no_dir=False):
        
        if not no_dir:
            return list(self.file_path.glob('*{}*'.format(self.search_str)))
        return [x.name for x in self.file_path.glob('*{}*'.format(self.search_str))]        
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_parser():

    def __init__(self, site):

        self._conversion_dict = {'Fco2': {'mg/m^2/s': _convert_co2},
                                 'RH': {'frac': _convert_RH}}
        self.site_df = make_site_df(path=PATH_TO_XL, site=site)
        self.site = site

    #--------------------------------------------------------------------------
    ### METHODS (PUBLIC) ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_data(self, table, standardise_vars=False, 
                       convert_units=False, apply_limits=False, 
                       pass_all_vars=False):
        
        files_to_concat = self.get_file_list(table)
        df = (pd.concat(
            [pd.read_csv(file, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                         index_col=['TIMESTAMP'], #usecols=variables, 
                         na_values='NAN', sep=',', engine='c',
                         on_bad_lines='warn')
            for file in files_to_concat]
            )
            .drop_duplicates()
            .sort_index()
            )
        if not pass_all_vars:
            variables = (
                ['RECORD'] +
                self.get_variable_list(by_table=table, returns='site_name')
                )
            df = df[variables]
        freq = self._get_table_freq(df)
        new_index = (
            pd.date_range(start=df.index[0], end=df.index[-1], freq=freq)
            )
        df.drop('RECORD', axis=1, inplace=True)
        if standardise_vars:
            rename_dict = dict(zip(
                self.get_variable_list(
                    by_table=table, returns='site_name'
                    ),
                self.get_variable_list(
                    by_table=table, returns='translation_name'
                    )
                ))
            df.rename(rename_dict, axis=1, inplace=True)
            if convert_units:
                self._do_unit_conversion(df=df)
            if apply_limits:
                self._apply_limits(df=df)
        try:
            return df.reindex(new_index)
        except ValueError:
            pdb.set_trace()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_data_path(self, table=None, file_only=False):
        """
        Get the path on disk for either a single table or all tables.

        Parameters
        ----------
        table : str, optional
            The table for which to return the path. The default is None.
        file_only : Bool, optional
            Return only the filename (as opposed to the path). 
            The default is False.

        Raises
        ------
        KeyError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        if table:        
            if not table in self.get_table_list():
                raise KeyError('Table "{}" not found!'.format(table))
        table_list = self.site_df.table_name.unique().tolist()
        file_list = self.site_df.file_name.unique().tolist()
        if not file_only:
            file_list = [
                pathlib.Path(PATH_TO_DATA.format(self.site)) / file 
                for file in file_list
                ]
        files_dict = dict(zip(table_list, file_list))
        if table:
            return files_dict[table]
        return files_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_list(self, search_str):
        
        file_path = pathlib.Path(PATH_TO_DATA.format(self.site))
        return list(file_path.glob('*{}.dat'.format(search_str)))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_list(self):
        """
        List all the unique tables for the relevant site in the variable map
        spreadsheet

        Returns
        -------
        list
            The list of tables.
        """

        return list(self.site_df.table_name.dropna().unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_list(self, by_table=None, returns='long_name'):
        """
        Get a list of the variables mapped in the variable map spreasheet.

        Parameters
        ----------
        by_table : str, optional
            The table for which to return variables. The default is None 
            (returns all variables).
        returns : str, optional
            The variable column to return (can be 'site name', 'standard_name', 
            or 'long name'). The default is 'long_name'.

        Raises
        ------
        KeyError
            Raised if either table name or returns arg is invalid.

        Returns
        -------
        list
            List of the variables that meet the criteria.

        """

        table_list = self.get_table_list()
        if not by_table in table_list:
            raise KeyError('Table not found!')
        return_list = [
            'long_name', 'site_name', 'standard_name', 'translation_name'
            ]
        if not returns in return_list:
            raise KeyError(
                '"Returns" arg must be one of the following: {}'
                .format(', '.join(return_list))
                )
        temp_df = self.site_df.reset_index()
        if by_table:
            return (
                temp_df.loc[temp_df.table_name==by_table, returns].tolist()
                )
        return temp_df[returns].tolist()
    #--------------------------------------------------------------------------
   
    #--------------------------------------------------------------------------
    def merge_tables(self, standardise_vars=False, convert_units=False, 
                     apply_limits=False, pass_all_vars=False):
        
        tables = self.get_table_list()
        try:
            tables.remove('site_details')
        except ValueError:
            pass
        df_list, date_list, freq_list = [], [], []
        for table in tables:
            print ('Getting table {}...'.format(table))
            this_df = self.get_table_data(
                table=table, standardise_vars=standardise_vars,
                convert_units=convert_units, apply_limits=apply_limits,
                pass_all_vars=pass_all_vars
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
        df = pd.concat(df_list, axis=1)
        if standardise_vars:
            df = df[self.site_df.translation_name]
        return df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def read_raw_header(self, table):
        
        table_path = self.get_table_data_path(table=table)
        header_list = []
        with open(table_path) as f:
            for i in range(4):
                header_list.append(f.readline())
        return header_list
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_program_info(self, table):
        
        header_list = self.read_raw_header(table=table)
        string_list = [x.replace('"', '') for x in 
                       header_list[0].replace('\n', '').split(',')]
        output_dict = dict(zip(PROG_INFO_LIST, string_list))
        output_dict.update({'raw_string': header_list[0]})
        return output_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_header_frame(self, table):
        
        header_list = self.read_raw_header(table=table)[1:]
        splits_list = []
        for i, line in enumerate(header_list):
            splits_list.append(
                [x.replace('"', '')for x in line.replace('\n', '').split(',')]
                )
        output_df = pd.DataFrame(
            data=zip(splits_list[1], splits_list[2]), 
            index=splits_list[0], columns=['units', 'sampling']
            )
        output_df.drop('RECORD', inplace=True)
        output_df.index.name = 'variable'
        return output_df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_header_frames(self, standardise_vars=False, convert_units=False):
        
        header_frame = (
            pd.concat([self.get_header_frame(table=table) 
                       for table in self.get_table_list()]
                      )
            )
        header_frame = header_frame[~header_frame.index.duplicated()]
        timestamp = pd.DataFrame(header_frame.loc['TIMESTAMP']).T
        header_frame = header_frame.loc[self.site_df.site_name.tolist()]
        site_name_list = header_frame.index.tolist()
        if standardise_vars:
            header_frame.index = self.site_df.loc[
                self.site_df.site_name==site_name_list,
                'translation_name'
                ]
        if convert_units:
            header_frame.loc[:, 'units'] = self.site_df.standard_units.tolist()
        else:
            header_frame.loc[:, 'units'] = self.site_df.site_units.tolist()
        return pd.concat([timestamp, header_frame])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_header(self, standardise_vars=False, convert_units=False):
        
        header_frame = self.merge_header_frames(
            standardise_vars=standardise_vars, convert_units=convert_units
            ).reset_index()
        string_list = [self._make_dummy_header()]
        for column in header_frame.columns:
            string_list.append(
                ','.join(['"{}"'.format(x) for x in header_frame[column]])
                + '\n'
                )
        return string_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_output_file(self, dest):
        
        header_lines = self.make_header(
            standardise_vars=True, convert_units=True
            )
        data = self.merge_tables(
            standardise_vars=True, convert_units=True, apply_limits=True
            )
        data = data.reset_index()
        timestamps = data['index'].apply(dt.datetime.strftime, 
                                         format='%Y-%m-%d %H:%M:%S')
        data['TIMESTAMP'] = timestamps
        data.drop('index', axis=1, inplace=True)
        column_order = ['TIMESTAMP'] + data.columns.drop('TIMESTAMP').tolist()
        # pdb.set_trace()
        # if 
        data = data[column_order]
        data.fillna('NAN', inplace=True)
        with open(dest, 'w', newline='\n') as f:
            for line in header_lines:
                f.write(line)
            data.to_csv(f, header=False, index=False, na_rep='NAN',
                        quoting=csv.QUOTE_NONNUMERIC)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### PRIVATE FUNCTIONS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, df):
        
        print ('    Parsing variable:')
        for var in df.columns:
            print ('        - {}'.format(var))
            limits = (
                self.site_df.loc[self.site_df.translation_name==var, 
                                 ['Max', 'Min']]
                .dropna()
                )
            if not len(limits):
                continue
            filter_bool = (
                (df[var]<limits.Min.item())|(df[var]>limits.Max.item())
                )
            df.loc[filter_bool, var] = np.nan
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _do_unit_conversion(self, df):
        
        conversion_df = self.site_df.loc[self.site_df.conversion]
        for var in conversion_df.translation_name:
            if not var in df.columns:
                continue
            conversion_dict = self._conversion_dict[var]
            existing_units = (
                conversion_df.loc[conversion_df.translation_name==var, 
                                  'site_units'].item()
                )
            conversion_func = conversion_dict[existing_units]
            df[var] = df[var].apply(conversion_func)
    #--------------------------------------------------------------------------

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
        return '{}T'.format(str(round(interval_list[0])))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _make_dummy_header(self):
        
        dummy_list = ['TOA5', self.site, 'CR6', '9999', 'CR6.Std.99.99', 
                      'CPU:no_prog.CR6', '9999', 'merged']
        return ','.join(['"{}"'.format(x) for x in dummy_list]) + '\n'
    #--------------------------------------------------------------------------
    
    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_df(path, site, logger_name_in_file=USE_LOGGER_NAME):
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
    if logger_name_in_file:
        file_name=site_df[['logger_name', 'table_name']].apply(func, axis=1)
    else:
        file_name=site_df['table_name'].apply('{}.dat'.format, axis=1)
    site_df = site_df.assign(file_name=file_name)
    
    # Create the master dataframe
    master_df = pd.read_excel(
        path, sheet_name='master_variables', index_col='Long name',
        converters={'Variable units': converter}
    )
    master_df.rename(
        {'Variable name': 'standard_name', 'Variable units': 'standard_units'}, 
        axis=1, inplace=True
        )

    # Join and generate variables that require input from both sources
    site_df = site_df.join(master_df)
    site_df = site_df.assign(
        conversion=site_df.site_units!=site_df.standard_units,
        translation_name=np.where(site_df.index.duplicated(keep=False),
                                  site_df.site_label, site_df.standard_name)
        )
    
    # Format and return
    site_df.index.name = 'long_name'
    return site_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _convert_co2(data):
    
    return data * 1000 / 44
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _convert_RH(data):
    
    return data * 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _calculate_es(Ta_data):
    
    return 0.6106 * np.exp(17.27 * Ta_data / (Ta_data + 237.3))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _calculate_e(Ta_data, RH_data):
    
    return Ta_data * RH_data / 100
#------------------------------------------------------------------------------

if __name__=='__main__':
    
    for site in SITE_PROC_LIST:
        print ('Parsing site {}:'.format(site))
        output_path = (
            pathlib.Path(PATH_TO_DATA.format(site)) / 
            '{}_merged_std.dat'.format(site)
            )
        parser = file_parser(site=site)
        parser.make_output_file(dest=output_path)
