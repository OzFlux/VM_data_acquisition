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

import datetime as dt
import os
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
    def get_file_list(self):
        """
        Returns a list of all files documented in the mapping spreadsheet.

        Returns
        -------
        list
            List of the files.

        """

        return self.site_df.file_name.dropna().unique()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_logger_list(self, long_name=None):
        """
        Returns a list of all loggers documented in the mapping spreadsheet, or
        the logger name for the variable if long_name supplied.

        Parameters
        ----------
        long_name : str, optional
            The variable for which to return the logger name. The default is None.

        Returns
        -------
        list
            List of logger names.

        """

        if long_name:
            return self.site_df.loc[long_name, 'logger_name']
        return list(self.site_df.logger_name.dropna().unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_variables(self):
        """
        Get the variables in the mapping spreadsheet that are required but do
        are not available in the raw data

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
        instruments (e.g. soil instruments).

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
        """
        Return the subframe of the complete dataframe containing only the soil
        variables.

        Returns
        -------
        pd.core.frame.DataFrame
            Frame containing the variables.

        """

        return pd.concat(
            [self.site_df.loc[x] for x in self.site_df.index.unique()
             if 'Soil' in x]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_limits(self, variable, by_field='translation_name'):
        """
        Get the plausible limits for a given variable.

        Parameters
        ----------
        variable : str
            Name of the variable for which to return the limits.
        by_field : str, optional
            The field to query for variable. The default is 'translation_name'.

        Returns
        -------
        pd.core.series.Series
            Series containing the limits.

        """

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
    def get_translation_dict(self, table_file):

        return dict(zip(
            self.get_variable_fields(
                table_file=table_file, field='site_name'
                ).tolist(),
            self.get_variable_fields(
                table_file=table_file, field='translation_name'
                ).tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_fields(self, table_file, field=None):
        """
        Return the variables associated with a particular table file (and field
        if specified)

        Parameters
        ----------
        table_file : str
            The name of the table file for which to return the data.
        field : str, optional
            The name of the field to return. The default is 'all'.

        Raises
        ------
        KeyError
            Raised if table file does not exist.

        Returns
        -------
        pd.core.frame.DataFrame or pd.core.series.Series
            If field == None, returns the dataframe containing the subset of
            variables found in that table, otherwise returns a series of all
            variable values for that field.

        """

        if not table_file in self.get_file_list():
            raise KeyError('Table not found!')
        if not field:
            return self.site_df.loc[self.site_df.file_name==table_file]
        return self.site_df.loc[self.site_df.file_name==table_file, field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### PRIVATE METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_site_df(self, logger_name_in_file=True):
        """
        Create the dataframe that contains the data to allow mapping from
        site variable names to standard variable names

        Parameters
        ----------
        logger_name_in_file : bool, optional
            If true, combines logger name with table name to create file name.
            The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe.

        """

        IMPORT_LIST = ['Label', 'Variable name', 'Variable units', 'Table name',
                       'Logger name', 'Disable', 'Long name']
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
            converters={'Variable units': lambda x: x if len(x) > 0 else None,
                        'Required': lambda x: True if x==1 else False}
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
        """
        Get the requested RTMC-formatted start condition.

        Parameters
        ----------
        start_cond : str
            The start condition required.

        Returns
        -------
        dict
            A dictionary with key 'start_cond' and the RTMC start condition
            string as value.

        """

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
        """
        Scale an RTMC evaluated string relative to its range.

        Parameters
        ----------
        eval_string : str
            The string that will be evaluated by RTMC.

        Returns
        -------
        str
            RTMC-readable string to generate a variable scaled relative to its
            range (max - min).

        """

        return (
            '({ev} - MinRun({ev})) / (MaxRun({ev}) - MinRun({ev}))'
            .format(ev=eval_string)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_alias_string(self, long_name):
        """
        Generate an RTMC-valid alias structure.

        Parameters
        ----------
        long_name : str
            The variable for which to return the alias string.

        Returns
        -------
        str
            Formatted RTMC alias string.

        """

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
    def get_comm_status_string(self, logger_name):
        """


        Parameters
        ----------
        logger_name : TYPE
            DESCRIPTION.

        Raises
        ------
        KeyError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        if not logger_name in self.site_df.logger_name.tolist():
            raise KeyError('No such logger name in table!')
        return (
            '"Server:__statistics__.{}_std.Collection State" > 2 '
            .format(logger_name)
            )
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
### FUNCTIONS ###
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_data_tables(site):

#     path = PATHS.get_local_path(resource='xl_variable_map')
#     converter = {'Site': lambda x: x.replace(' ', '')}
#     df = pd.read_excel(path, sheet_name='L1_table_list', converters=converter,
#                        index_col='Site')
#     df.rename({'Table name': 'table_name', 'Logger name': 'logger_name'},
#               axis=1, inplace=True)
#     df = df.loc[[site]]
#     df['file_name'] = df.logger_name + '_' + df.table_name + '.dat'
#     df = df.reset_index(drop=True).set_index(keys='table_name')
#     df['has_file'], df['start_date'], df['end_date'] = False, np.nan, np.nan
#     for rec in df.index:
#         file_path = PATHS.get_local_path(
#             resource='data', site=site, stream='flux_slow'
#             ) / df.loc[rec, 'file_name']
#         if file_path.exists():
#             df.loc[rec, 'has_file'] = True
#             dates = get_file_dates(file=file_path)
#             df.loc[rec, 'start_date'] = dates['start_date']
#             df.loc[rec, 'end_date'] = dates['end_date']
#     return df
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_tables(site=None):

    path = PATHS.get_local_path(resource='xl_variable_map')
    converter = {'Site': lambda x: x.replace(' ', '')}
    df = pd.read_excel(path, sheet_name='L1_table_list', converters=converter)
    if site:
        df = df.loc[df.Site==site]
    df.rename({'Table name': 'table_name', 'Logger name': 'logger_name'},
              axis=1, inplace=True)
    df['file_name'] = df.logger_name + '_' + df.table_name + '.dat'
    df.index = pd.MultiIndex.from_frame(df[['logger_name', 'table_name']])
    df.drop(['logger_name', 'table_name'], axis=1, inplace=True)
    df['has_file'], df['start_date'], df['end_date'] = False, np.nan, np.nan
    for rec in df.index:
        this_site = df.loc[rec, 'Site']
        file_path = PATHS.get_local_path(
            resource='data', site=this_site, stream='flux_slow'
            ) / df.loc[rec, 'file_name']
        if file_path.exists():
            df.loc[rec, 'has_file'] = True
            dates = get_file_dates(file=file_path)
            df.loc[rec, 'start_date'] = dates['start_date']
            df.loc[rec, 'end_date'] = dates['end_date']
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_dates(file):

    date_format = '"%Y-%m-%d %H:%M:%S"'
    with open(file, 'rb') as f:
        while True:
            line_list = f.readline().decode().split(',')
            try:
                start_date = (
                    dt.datetime.strptime(line_list[0], date_format)
                    )
                break
            except ValueError:
                pass
        f.seek(2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line_list = f.readline().decode().split(',')
        end_date = dt.datetime.strptime(last_line_list[0], date_format)
    return {'start_date': start_date, 'end_date': end_date}
#------------------------------------------------------------------------------