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
    - mapper should be able to create an rtmc spreadsheet linking rtmc 
      components to variables

@author: imchugh
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
class mapper():
    
    def __init__(self, site):
        """
        Class to generate mapping functions from site-specific variables to 
        universal standard (where possible, nomenclature is based on the 
                            guidance in the PFP Wiki)

        Parameters
        ----------
        site : str
            The site for which to generate the map.

        Returns
        -------
        None.

        """

        self.site = site        
        self.site_df = self._make_site_df()
        self.rtmc_syntax_generator = _RTMC_syntax_generator(self.site_df)
    
    #--------------------------------------------------------------------------
    ### PUBLIC METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_conversion_variables(self):
        """
        Get the variables in the mapping spreadsheet that require conversion 
        from non-standard units

        Parameters
        ----------
        None.

        Returns
        -------
        dataframe.

        """
                
        return self.site_df.loc[~self.site_df.Missing & self.site_df.conversion]
    #--------------------------------------------------------------------------   

    #--------------------------------------------------------------------------   
    def get_logger_list(self, long_name=None):
        
        if long_name:
            return self.site_df.loc[long_name, 'logger_name']
        return list(self.site_df.logger_name.dropna().unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_variables(self):
        """
        Get the variables in the mapping spreadsheet that are required but do
        are not available in the raw data

        Parameters
        ----------
        None.

        Returns
        -------
        dataframe.

        """
        
        return self.site_df.loc[self.site_df.Missing & self.site_df.Required]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_repeat_variables(self, names_only=False):
        """
        Get the variables in the mapping spreadsheet that have multiple
        instruments (e.g. soil instruments)
        
        Parameters
        ----------
        names_only : bool
            If True, returns a list of the variable long names. If False,
        
        Returns
        -------
        dataframe or list.
        
        """
        
        data = self.site_df[self.site_df.index.duplicated(keep=False)]
        if len(data) == 0:
            print('No repeat variables found!')
            return
        if not names_only:
            return data            
        return data.index.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_soil_variables(self):
        
        return pd.concat(
            [self.site_df.loc[x] for x in self.site_df.index.unique()
             if 'Soil' in x]
            )
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------    
    def get_variable_limits(self, variable, by_field='translation_name'):
        
        return self.site_df.loc[
            self.site_df[by_field] == variable, ['Max', 'Min']
            ]
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_table_list(self, long_name=None):
        """
        List all the unique tables for the relevant site in the variable map
        spreadsheet

        Returns
        -------
        list
            The list of tables.
        """

        if long_name:
            return self.site_df.loc[long_name, 'table_name']
        return list(self.site_df.table_name.dropna().unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_fields(self, table, field='all'):
        
        if not table in self.get_table_list():
            raise KeyError('Table not found!')
        if field == 'all':
            return self.site_df.loc[self.site_df.table_name == table]
        local_df = self.site_df.reset_index()
        return local_df.loc[local_df.table_name == table, field].tolist()
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
       
        IMPORT_LIST = ['Label', 'Variable name', 'Variable units', 'Table name',
                       'Logger name', 'Disable', 'Default value', 'Long name']
        RENAME_DICT = {'Label': 'site_label', 'Variable name': 'site_name', 
                       'Variable units': 'site_units', 'Table name': 'table_name',
                       'Logger name': 'logger_name'}
       
        # Concatenate the logger_name and the table_name to make the file source 
        # name (only applied if 'logger_name_in_source' arg is True)
        def func(s):
            if len(s.dropna()) == 0:
                return np.nan
            return '{}.dat'.format('_'.join(s.tolist()))

        # Create the site dataframe
        site_df = pd.read_excel(
            PATHS.variable_map(), sheet_name=self.site, usecols=IMPORT_LIST,
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
            PATHS.variable_map(), sheet_name='master_variables', 
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
            translation_name=np.where(~pd.isnull(site_df.site_label), 
                                      site_df.site_label, site_df.standard_name)
            )
                
        # Add critical variables that are missing from the primary datasets
        required_list = (
            master_df.loc[master_df.Required==True].index.tolist()
            )
        missing_list = [x for x in required_list if not x in site_df.index]
        site_df = pd.concat([site_df, master_df.loc[missing_list]])
        site_df['Missing'] = pd.isnull(site_df.site_name)
        site_df.loc[site_df.Missing, 'translation_name'] = (
            site_df.loc[site_df.Missing, 'standard_name']
            )
        site_df.index.name = 'long_name'
        return site_df
    #--------------------------------------------------------------------------
        
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class _RTMC_syntax_generator():
    
    def __init__(self, site_df):
        
        self.site_df = site_df

    #--------------------------------------------------------------------------
    def _get_init_dict(self, start_cond):
        
        start_dict = {
            'start': 'StartRelativeToNewest({},OrderCollected);',
            'start_absolute': 'StartAtRecord(0,0,OrderCollected);'
            }
        if not start_cond:
            return {}
        return {'start_cond': start_dict[start_cond]}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_scaled_to_range(self, eval_string):
        
        return (
            '({ev} - MinRun({ev})) / (MaxRun({ev}) - MinRun({ev}))'
            .format(ev=eval_string)
            )
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_alias_string(self, long_name, **kwargs):
        
        variable = self._get_variable_frame(long_name=long_name)
        alias_list = variable.translation_name.tolist()
        source_list = [
            '"DataFile:merged.{}"'.format(x) for x in alias_list
            ]
        combined_list = [
            'Alias({});'.format(x[0] + ',' + x[1])
            for x in zip(alias_list, source_list)
            ]
        return self._str_joiner(combined_list, joiner='\r\n')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_variable_frame(self, long_name):
        
        variable = self.site_df.loc[long_name]
        if isinstance(variable, pd.core.series.Series):
            variable = variable.to_frame().T
        return variable
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_aliased_output(
            self, long_name, multiple_to_avg=True, as_str=True, 
            start_cond=None, scaled_to_range=False
            ):
        
        variable = self._get_variable_frame(long_name=long_name)
        alias_string = self.get_alias_string(long_name=long_name) 
        eval_string = '+'.join(variable.translation_name.tolist())
        if scaled_to_range:
            eval_string = self._get_scaled_to_range(eval_string=eval_string)
            start_cond = 'start_absolute'
        n = len(variable)
        if n > 1:
            if multiple_to_avg:
                eval_string = '({0})/{1}'.format(eval_string, n)
            else:
                raise NotImplementedError('Only averages implemented!')
        strings_dict = self._get_init_dict(start_cond=start_cond)
        strings_dict.update(
            {'alias_string': alias_string, 'eval_string': eval_string}
            )
        if as_str:
            return self._str_joiner(list(strings_dict.values()))            
        return strings_dict
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_soil_heat_storage(
            self, Cp=1800, seconds=1800, layer_depth=0.08, as_str=True,
            start_cond=None):
        
        avg_dict = (
            self.get_aliased_output(long_name='Soil temperature', as_str=False)
            )
        alias_string = self._str_joiner(
            [avg_dict['alias_string'], 'Alias(Cp,{});'.format(Cp)],
            joiner='\r\n'
            )
        eval_string = (
            'Cp*(({avg})-Last({avg}))/{secs}*1/{dp}'
            .format(avg=avg_dict['eval_string'], secs=seconds, dp=layer_depth)
            )
        strings_dict = self._get_init_dict(start_cond=start_cond)
        strings_dict.update(
            {'alias_string': alias_string, 'eval_string': eval_string}
            )
        if as_str:
            return self._str_joiner(list(strings_dict.values()))
        return strings_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_corrected_soil_heat_flux(
            self, Cp=1800, seconds=1800, layer_depth=0.08
            ):
        
        stor_dict = self.get_soil_heat_storage(
            Cp=Cp, seconds=seconds, layer_depth=layer_depth, as_str=False
            )
        flux_dict = self.get_aliased_output(
            long_name='Soil heat flux at depth z', as_str=False
            )
        all_alias = self._str_joiner(
            str_list=[flux_dict['alias_string'], stor_dict['alias_string']],
                      joiner='\r\n')
        output_string = (
            '{flux}+{store}'.format(flux=flux_dict['eval_string'],
                                    store=stor_dict['eval_string'])
            )
        return self._str_joiner([all_alias, output_string])
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _str_joiner(self, str_list, joiner='\r\n\r\n'):

        return joiner.join(str_list)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_merger():

    def __init__(self, site):

        self.site = site
        self.map = mapper(site=site)

    #--------------------------------------------------------------------------
    ### METHODS (PUBLIC) ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_table_data(self, table):

        file_name = self.get_table_file(table=table)
        df = (
            pd.read_csv(file_name, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                        index_col=['TIMESTAMP'], na_values='NAN', sep=',', 
                        engine='c', on_bad_lines='warn')
            .drop_duplicates()
            .sort_index()
            )
        variables = (
            ['RECORD'] +
            self.map.get_variable_fields(table=table, field='site_name')
            )        
        df = df[variables]
        freq = self._get_table_freq(df)
        new_index = (
            pd.date_range(start=df.index[0], end=df.index[-1], freq=freq)
            )
        df.drop('RECORD', axis=1, inplace=True)
        rename_dict = dict(zip(
            self.map.get_variable_fields(table=table, field='site_name'),
            self.map.get_variable_fields(table=table, field='translation_name')
            ))
        df.rename(rename_dict, axis=1, inplace=True)
        self._do_unit_conversion(df=df)
        self._apply_limits(df=df)
        return df.reindex(new_index)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_file(self, table):

        if not table in self.map.get_table_list():
            raise KeyError('Table not found!')
        gen = PATHS.slow_fluxes(site=self.site).glob('*{}.dat'.format(table))
        l = [x for x in gen]
        if not len(l) == 1:
            raise RuntimeError('More than one file with that string found!')
        return l[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_table_data(self, table=None):
        
        if table:
            return self._get_table_data(table=table)
        tables = self.map.get_table_list()
        df_list, date_list, freq_list = [], [], []
        for table in tables:
            print ('Getting table {}...'.format(table))
            this_df = self._get_table_data(table=table)
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
            end=np.array(date_list).min(), 
            freq=freq_list[0]
            )
        for this_df in df_list:
            this_df = this_df.reindex(new_index)
        df = pd.concat(df_list, axis=1)
        self._calculate_missing_variables(df=df)
        return df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def read_raw_header(self, table):
        
        table_path = self.get_table_file(table=table)
        header_list = []
        with open(table_path) as f:
            for i in range(4):
                header_list.append(f.readline())
        return header_list
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_program_info(self, table):
        
        PROG_INFO_LIST = ['format', 'station_name', 'logger_type', 
                          'serial_num', 'OS_version', 'program_name', 
                          'program_sig', 'table_name']
        header_list = self.read_raw_header(table=table)
        string_list = [x.replace('"', '') for x in 
                       header_list[0].replace('\n', '').split(',')]
        output_dict = dict(zip(PROG_INFO_LIST, string_list))
        output_dict.update({'raw_string': header_list[0]})
        return output_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_header(self, table=None, to_list=False, add_missing=True):
        
        if table:
            df = self._construct_table_header_frame(table=table)
        else:
            df = pd.concat(
                [self._construct_table_header_frame(table=table) 
                 for table in self.map.get_table_list()]
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
    def _construct_table_header_frame(self, table):
        
        header_list = self.read_raw_header(table=table)[1:]
        splits_list = []
        for i, line in enumerate(header_list):
            splits_list.append(
                [x.replace('"', '') for x in line.replace('\n', '').split(',')]
                )
        header_df = pd.DataFrame(
            data=zip(splits_list[1], splits_list[2]), 
            index=splits_list[0], columns=['standard_units', 'sampling']
            )
        timestamp = pd.DataFrame(header_df.loc['TIMESTAMP']).T
        ref_df = self.map.get_variable_fields(table=table)
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
                      'CO2_mole_fraction': 'umol/mol'}
        return {'standard_units': units_dict[variable], 'sampling': ''}
    #--------------------------------------------------------------------------    

    #--------------------------------------------------------------------------    
    def construct_variable(self, variable):
        
        functions_dict = {'es': self._calculate_es,
                          'e': self._calculate_e,
                          'AH_sensor': self._calculate_AH,
                          'molar_density': self._calculate_molar_density,
                          'CO2_mole_fraction': self._calculate_CO2_mole_fraction,
                          'RH': self._calculate_RH}
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

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN CODE ###
#------------------------------------------------------------------------------

SITE_PROC_LIST = ['Calperum', 'Gingin', 'Boyagin', 'Fletcherview', 'Litchfield']

if __name__=='__main__':
    
    for site in SITE_PROC_LIST:
        print ('Parsing site {}:'.format(site))
        output_path = (
            PATHS.slow_fluxes(site=site) / '{}_merged_std.dat'.format(site)
            )
        parser = file_merger(site=site)
        parser.make_output_file(dest=output_path)