# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 15:08:15 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import csv
import datetime as dt
import numpy as np
import pandas as pd
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import variable_mapper as vm
import paths_manager as pm

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
PATHS = pm.paths()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_merger():

    def __init__(self, site):

        self.site = site
        self.map = vm.mapper(site=site)

    #--------------------------------------------------------------------------
    ### METHODS (PUBLIC) ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_table_data(self, table_file):

        file_name = self.get_table_file(table_file=table_file)
        df = (
            pd.read_csv(file_name, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                        index_col=['TIMESTAMP'], na_values='NAN', sep=',', 
                        engine='c', on_bad_lines='warn')
            .drop_duplicates()
            .sort_index()
            )
        variables = (
            ['RECORD'] +
            self.map.get_variable_fields(
                table_file=table_file, field='site_name'
                ).tolist()
            )
        try:
            df = df[variables]
        except KeyError:
            pdb.set_trace()
        freq = self._get_table_freq(df)
        new_index = (
            pd.date_range(start=df.index[0], end=df.index[-1], freq=freq)
            )
        df.drop('RECORD', axis=1, inplace=True)
        rename_dict = dict(zip(
            self.map.get_variable_fields(
                table_file=table_file, field='site_name'
                ).tolist(),
            self.map.get_variable_fields(
                table_file=table_file, field='translation_name'
                ).tolist()
            ))
        df.rename(rename_dict, axis=1, inplace=True)
        self._do_unit_conversion(df=df)
        self._apply_limits(df=df)
        return df.reindex(new_index)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_file(self, table_file):

        if not table_file in self.map.get_file_list():
            raise KeyError('Table not found!')
        full_path = PATHS.slow_fluxes(site=self.site) / table_file
        if not full_path.exists:
            raise FileNotFoundError('File {} not found!'.format(table_file))
        return full_path
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_table_data(self, table_file=None):
        
        if table_file:
            return self._get_table_data(table_file=table_file)
        file_list = self.map.get_file_list()
        df_list, start_list, end_list, freq_list = [], [], [], []
        for table in file_list:
            print ('Getting table {}...'.format(table))
            this_df = self._get_table_data(table_file=table)
            df_list.append(this_df)
            start_list += [this_df.index[0]]
            end_list += [this_df.index[-1]]
            freq_list.append(this_df.index.freq)
        if not all(x == freq_list[0] for x in freq_list):
            raise NotImplementedError(
                'Data tables have different intervals; '
                'resampling not implemented yet'
                )
        new_index = pd.date_range(
            start=np.array(start_list).max(), 
            end=np.array(end_list).min(), 
            freq=freq_list[0]
            )
        df = pd.concat(df_list, axis=1)
        df = df.reindex(new_index)
        self._calculate_missing_variables(df=df)
        return df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def read_raw_header(self, table_file):
        
        table_path = self.get_table_file(table_file=table_file)
        header_list = []
        with open(table_path) as f:
            for i, line in enumerate(csv.reader(f)):
                if i==4:
                    break
                header_list.append(line)
        return header_list
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_program_info(self, table_file):
        
        PROG_INFO_LIST = ['format', 'station_name', 'logger_type', 
                          'serial_num', 'OS_version', 'program_name', 
                          'program_sig', 'table_name']
        string_list = self.read_raw_header(table_file=table_file)[0]
        output_dict = dict(zip(PROG_INFO_LIST, string_list))
        raw_string = ','.join(['"{}"'.format(x) for x in string_list]) + '\n'
        output_dict.update({'raw_string': raw_string})
        return output_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_header(self, table_file=None, to_list=False, add_missing=True):
        
        if table_file:
            df = self._construct_table_header_frame(table_file=table_file)
        else:
            df = pd.concat(
                [self._construct_table_header_frame(table_file=table_file) 
                 for table_file in self.map.get_file_list()]
                )
            df = df[~df.index.duplicated()]
            if add_missing:
                missing_df = self.map.get_missing_variables()
                df_list = []
                for var in missing_df.index:
                    df_list.append(
                        {'standard_units': missing_df.loc[var, 'standard_units'],
                         'sampling': '',}
                        )
                add_df = pd.DataFrame(
                    data=df_list, index=missing_df.translation_name.tolist()
                    )
                df = pd.concat([df, add_df])
        if to_list:
            return self._construct_header_strings(header_frame=df)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _construct_table_header_frame(self, table_file):
        
        header_list = self.read_raw_header(table_file=table_file)[1:]
        header_df = pd.DataFrame(
            data=zip(header_list[1], header_list[2]), 
            index=header_list[0], columns=['standard_units', 'sampling']
            )
        timestamp = pd.DataFrame(header_df.loc['TIMESTAMP']).T
        ref_df = self.map.get_variable_fields(table_file=table_file)
        ref_df = ref_df.assign(
            sampling = header_df.loc[ref_df.site_name, 'sampling'].tolist()
            )
        output_df = ref_df[['standard_units', 'sampling']]
        output_df.index = ref_df['translation_name']
        output_df.index.name = 'variable_name'
        return pd.concat([timestamp, output_df])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _construct_header_strings(self, header_frame):
        
        string_list = [self._make_dummy_header()]
        header_frame = header_frame.reset_index()
        for column in header_frame.columns:
            string_list.append(
                ','.join(['"{}"'.format(x) for x in header_frame[column]])
                + '\n'
                )
        return string_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_output_file(self, dest):
        
        header_lines = self.construct_header(to_list=True)
        data = self.construct_table_data()
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
        
        conversion_df = self.map.get_conversion_variables()
        constructor = var_constructor(df=df)
        for var in conversion_df.index:
            name = conversion_df.loc[var, 'translation_name']
            existing_units = conversion_df.loc[var, 'site_units']
            if not name in df.columns:
                continue
            df[name] = (
                constructor.convert_variable(variable=name, 
                                             from_units=existing_units)
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_missing_variables(self, df):
        
        missing_df = self.map.get_missing_variables()
        constructor = var_constructor(df=df)
        for var in missing_df.index:
            name = missing_df.loc[var, 'translation_name']
            try:
                df[name] = constructor.construct_variable(variable=name)
            except KeyError:
                print(
                    'Warning: missing variable ({}) with no generation function!'
                    .format(var)
                    )
                continue
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
class var_constructor():
    
    def __init__(self, df):
        
        self.data = df

    #--------------------------------------------------------------------------
    def get_header_fragment(self, variable):
        
        units_dict = {'es': 'kPa', 'e': 'kPa', 'AH': 'g/m^3', 
                      'molar_density': 'mol/m^3', 
                      'CO2_mole_fraction': 'umol/mol',
                      'CO2_density': 'mg/m^3'}
        return {'standard_units': units_dict[variable], 'sampling': ''}
    #--------------------------------------------------------------------------    

    #--------------------------------------------------------------------------    
    def construct_variable(self, variable):
        
        functions_dict = {'es': self._calculate_es,
                          'e': self._calculate_e,
                          'AH_sensor': self._calculate_AH,
                          'molar_density': self._calculate_molar_density,
                          'CO2_mole_fraction': self._calculate_CO2_mole_fraction,
                          'RH': self._calculate_RH,
                          'CO2_density': self._calculate_CO2_density}
        return functions_dict[variable]()
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------    
    def convert_variable(self, variable, from_units):
        
        functions_dict = {'Fco2': self._convert_Fco2(from_units),
                          'RH': self._convert_RH(from_units)}
        return functions_dict[variable]
    #--------------------------------------------------------------------------    

    #--------------------------------------------------------------------------
    def _convert_Fco2(self, from_units='mg/m^2/s'):
        
        if from_units == 'mg/m^2/s':
            return self.data.Fco2 * 1000 / 44
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _convert_RH(self, from_units='frac'):
        
        if from_units == 'frac':
            return self.data.RH * 100
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _calculate_es(self):
        
        return 0.6106 * np.exp(17.27 * self.data.Ta / (self.data.Ta + 237.3))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _calculate_e(self):
        
        return self._calculate_es() * self.data.RH / 100
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _calculate_AH(self):
        
        return (
            self._calculate_e() / self.data.ps * 
            self._calculate_molar_density() * 18
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_RH(self):
        
        e = ((self.data.AH_sensor / 18) / self._calculate_molar_density() *
             self.data.ps)
        es = self._calculate_es()
        return e / es * 100
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _calculate_molar_density(self):
        
        return self.data.ps * 1000 / ((self.data.Ta + 273.15) * 8.3143)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _calculate_CO2_mole_fraction(self):
        
        return (
            (self.data.CO2_density / 44) / 
            self._calculate_molar_density() * 10**3
            )
        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_CO2_density(self):
        
        return (
            self.data.CO2 / 10**3 * self._calculate_molar_density() * 44
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------