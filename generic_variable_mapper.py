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
    - remove direct manipulation of site dataframe from the file parser (this 
      necessitates the use of self.map.etc... particularly in the output file
      method)

@author: imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import csv
import datetime as dt
import numpy as np
import pandas as pd
import pathlib
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import paths_manager as pm

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
PATH_TO_XL = (
    'E:\\Cloudstor\\Network_documents\\Site_documentation\\'
    'site_variable_map_alt.xlsx'
    )
PATH_TO_DATA = 'E:\\Sites\\{}\\Flux\\Slow'
PATH_TO_DETAILS = 'E:\\Cloudstor\\Network_documents\\RTMC_files\\Static_data'
PATH_TO_IMAGES = 'E:\\Cloudstor\\Network_documents\\RTMC_files\\Static_images\\Site_images'
IMPORT_LIST = ['Label', 'Variable name', 'Variable units', 'Table name',
               'Logger name', 'Disable', 'Default value', 'Long name']
RENAME_DICT = {'Label': 'site_label', 'Variable name': 'site_name', 
               'Variable units': 'site_units', 'Table name': 'table_name',
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
class mapper():
    
    def __init__(self, site):

        self.site = site        
        self.site_df = self._make_site_df()
    
    #--------------------------------------------------------------------------
    ### PUBLIC METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_conversion_data(self):
        
        return self.site_df.loc[self.site_df.conversion]
    #--------------------------------------------------------------------------   

    #--------------------------------------------------------------------------
    def get_required_variables(self, which='all'):
        
        response_list = ['missing', 'all']
        if not which in response_list:
            raise KeyError(
                'Arg "which" must be one of: {}'
                .format(', '.join(response_list))
                )
        all_df = self.site_df.loc[self.site_df.Required==True]
        if which == 'all':
            return all_df    
        return all_df.loc[pd.isnull(all_df.site_name) & (all_df.Required)]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_repeat_variables(self, names_only=False):
        
        data = self.site_df[self.site_df.index.duplicated(keep=False)]
        if len(data) == 0:
            print('No repeat variables found!')
            return
        if not names_only:
            return data            
        return data.index.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_standard_name_mapping(self, table):
        
        return dict(zip(
            self.get_variable_list(
                table=table, returns='site_name'
                ),
            self.get_variable_list(
                table=table, returns='translation_name'
                )
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_variable_limits(self, variable, by_field='translation_name'):
        
        return self.site_df.loc[
            self.site_df[by_field] == variable, ['Max', 'Min']
            ]
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
    def get_variable_list(self, table, returns='long_name'):
        """
        Get a list of the variables mapped in the variable map spreasheet.

        Parameters
        ----------
        table : str, optional
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

        if not table in self.get_table_list():
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
        if table:
            return (
                temp_df.loc[temp_df.table_name==table, returns].tolist()
                )
        return temp_df[returns].tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### PRIVATE METHODS ###
    #--------------------------------------------------------------------------   

    #--------------------------------------------------------------------------
    def _make_site_df(self, logger_name_in_file=True):
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
        # name (only applied if 'logger_name_in_source' arg is True)
        def func(s):
           return '{}.dat'.format('_'.join(s.tolist()))
    
        # Create the site dataframe
        site_df = pd.read_excel(
            PATH_TO_XL, sheet_name=self.site, usecols=IMPORT_LIST,
            converters={'Variable units': lambda x: x if len(x) > 0 else None},
            index_col='Long name'
            )
        site_df = site_df.loc[np.isnan(site_df.Disable)]
        site_df.rename(RENAME_DICT, axis=1, inplace=True)
        if logger_name_in_file:
            file_name=site_df[['logger_name', 'table_name']].apply(func, axis=1)
        else:
            file_name=site_df['table_name'].apply('{}.dat'.format, axis=1)
        site_df = site_df.assign(file_name=file_name)

        # Make the master dataframe
        master_df = pd.read_excel(
            PATH_TO_XL, sheet_name='master_variables', 
            index_col='Long name',
            converters={'Variable units': lambda x: x if len(x) > 0 else None}
        )
        master_df.rename(
            {'Variable name': 'standard_name', 
             'Variable units': 'standard_units'}, 
            axis=1, inplace=True
            )        
           
        # Join and generate variables that require input from both sources
        site_df = site_df.join(master_df)
        site_df = site_df.assign(
            conversion=site_df.site_units!=site_df.standard_units,
            translation_name=np.where(site_df.index.duplicated(keep=False),
                                      site_df.site_label, site_df.standard_name)
            )
        
        # Add critical variables that are missing from the primary datasets
        required_list = (
            master_df.loc[master_df.Required==True].index.tolist()
            )
        missing_list = [x for x in required_list if not x in site_df.index]
        site_df = pd.concat([site_df, master_df.loc[missing_list]])
        
        # Format and return
        site_df.index.name = 'long_name'
        return site_df
    #--------------------------------------------------------------------------
        
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_parser():

    def __init__(self, site, concatenate_backups=False):

        self.paths = pm.paths(site)
        self.map = mapper(site=site)
        self._conversion_dict = {'Fco2': {'mg/m^2/s': _convert_co2},
                                 'RH': {'frac': _convert_RH}}
        self.site = site
        self.concatenate_backups = concatenate_backups

    #--------------------------------------------------------------------------
    ### METHODS (PUBLIC) ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_data(self, table, standardise_vars=False, 
                       convert_units=False, apply_limits=False, 
                       pass_all_vars=False):
        
        files_to_concat = self.get_table_files(table=table)
        df = (pd.concat(
            [pd.read_csv(file, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                         index_col=['TIMESTAMP'],
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
                self.map.get_variable_list(table=table, returns='site_name')
                )
            df = df[variables]
        freq = self._get_table_freq(df)
        new_index = (
            pd.date_range(start=df.index[0], end=df.index[-1], freq=freq)
            )
        df.drop('RECORD', axis=1, inplace=True)
        if standardise_vars:
            rename_dict = self.map.get_standard_name_mapping(table=table)
            df.rename(rename_dict, axis=1, inplace=True)
            if convert_units:
                self._do_unit_conversion(df=df)
            if apply_limits:
                self._apply_limits(df=df)
        return df.reindex(new_index)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_files(self, table):

        if not table in self.map.get_table_list():
            raise KeyError('Table not found!')
        if self.concatenate_backups:
            return list(
                self.paths.slow_fluxes.glob('*{}.*'.format(table))
                )
        return list(
            self.paths.slow_fluxes.glob('*{}.dat'.format(table))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_tables(self, standardise_vars=False, convert_units=False, 
                     apply_limits=False, pass_all_vars=False):
        
        tables = self.map.get_table_list()
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
            df = df[self.map.site_df.translation_name]
        return df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def read_raw_header(self, table):
        
        table_path = self.get_table_files(table=table)[0]
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
                       for table in self.map.get_table_list()]
                      )
            )
        header_frame = header_frame[~header_frame.index.duplicated()]
        timestamp = pd.DataFrame(header_frame.loc['TIMESTAMP']).T
        try:
            header_frame = header_frame.loc[self.map.site_df.site_name.tolist()]
        except KeyError:
            pdb.set_trace()
        site_name_list = header_frame.index.tolist()
        if standardise_vars:
            header_frame.index = self.map.site_df.loc[
                self.map.site_df.site_name==site_name_list,
                'translation_name'
                ]
        if convert_units:
            header_frame.loc[:, 'units'] = self.map.site_df.standard_units.tolist()
        else:
            header_frame.loc[:, 'units'] = self.map.site_df.site_units.tolist()
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
            limits = self.map.get_variable_limits(variable=var)
            if not len(limits):
                continue
            filter_bool = (
                (df[var]<limits.Min.item())|(df[var]>limits.Max.item())
                )
            df.loc[filter_bool, var] = np.nan
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _do_unit_conversion(self, df):
        
        conversion_df = self.map.get_conversion_data()
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
    
    return _calculate_es(Ta_data=Ta_data) * RH_data / 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _calculate_AH(Ta_data, RH_data, ps_data):
    
    return (
        _calculate_e(Ta_data, RH_data) / ps_data * 
        _calc_molar_density(Ta_data, ps_data) * 18
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _calc_molar_density(Ta_data, ps_data):
    
    return ps_data * 1000 / ((Ta_data + 273.15) * 8.3143)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _calc_CO2_mole_fraction(CO2Dens_data, Ta_data, ps_data):
    
    return (
        (CO2Dens_data / 44) / 
        _calc_molar_density(Ta_data, ps_data) * 10**3
        )
    pass
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
