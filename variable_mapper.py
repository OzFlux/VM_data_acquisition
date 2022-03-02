#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 07:31:34 2021

@author: imchugh
"""

import numpy as np
import pandas as pd
import pdb

path='/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'

class variable_mapper():

    def __init__(self, path, site):

        self.site = site
        self.rtmc_df = _make_component_df(path=path)
        self.master_df = _make_master_df(path=path)
        self.site_df = _make_site_df(path=path, site=site)
        # self._check_long_names()

    # #--------------------------------------------------------------------------
    # def _check_long_names(self):
    #     """
    #     Check that long_names specified in site spreadsheet are contained in
    #     the standard long names list in the master spreadsheet.

    #     Raises
    #     ------
    #     KeyError
    #         Raised if and long name is invalid.

    #     Returns
    #     -------
    #     None.
    #     """

    #     master_long_names = (
    #         self.master_df.index.get_level_values(level='long_name').tolist()
    #         )
    #     site_long_names = (
    #         self.site_df.index.get_level_values(level='long_name').tolist()
    #         )
    #     invalid_list = (
    #         [name for name in site_long_names if not name in master_long_names]
    #         )
    #     if invalid_list:
    #         raise KeyError(
    #             'The following long variable names were invalid: {}'
    #             .format(', '.join(invalid_list))
    #             )
    # #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_text_string_from_dict(self, text_dict):
        """
        Turn standardised dictionary input into rtmc text output.

        Parameters
        ----------
        text_dict : dict
            Dictionary containing alias map (maps the underlying rtmc variable
            to a short alias string) and evaluation (the text string defining
            the calculated quantity using the aliases).
        Returns
        -------
        str
            The string in the rtmc-expected format.
        """
        if text_dict['alias_map']:
            aliases = (
                ''.join([alias + '\r\n' for alias in text_dict['alias_map']])
                )
            return aliases + '\r\n' + text_dict['evaluation']
        return text_dict['evaluation']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable(self, long_name, label=None, field=None):
        """
        Get the details required to generate an rtmc entry from a given
        variable.

        Parameters
        ----------
        long_name : str
            The variable long name for which to generate the details.
        label : str, optional
            Names of sensors where variable describes multiple sensors.
            The default is None.
        field : str, optional
            The specific field to get. The default is None.

        Raises
        ------
        KeyError
            If long_name is not in the master spreadsheet.

        Returns
        -------
        pd.Series
            If field not specified and either long_name doesn't have multiple
            entries or the label for the specific entry is supplied'
        pd.DataFrame
            If field is not specified AND label is not specified AND there are
            multiple long_name entries
        str
            If field is specified and either long_name doesn't have multiple
            entries or the label for the specific entry is supplied'
        """

        if not long_name in self.master_df.index:
            raise KeyError('Long name {} not defined!'.format(long_name))
        try:
            pd_obj = self.site_df.loc[(long_name, label)]
        except KeyError:
            pd_obj = self.site_df.loc[long_name]
        if field:
            return pd_obj[field]
        return pd_obj
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def calculate_AH(self, build_string=True):
        """
        Generate components for an rtmc-valid calculation of absolute humidity.

        Parameters
        ----------
        build_string : boolean, optional
            If true, converts the dictionary of required components to an
            rtmc-valid string. The default is True.

        Returns
        -------
        str
            If build_string True, produces rtmc-valid string.
        dict
            if build_string False, produces dictionary containing required
            elements for building rtmc string.
        """

        e_dict = self.calculate_e(build_string=False)
        rho_mol_dict = self.calculate_molar_density(build_string=False)
        ps_data = self.get_variable(long_name='Surface air pressure')
        if ps_data.convert:
            alias_name = ps_data.evaluated_alias_name
        else:
            alias_name = ps_data.alias_name
        alias_map_list = e_dict['alias_map'] + [ps_data.alias_map]
        evaluation = '{0}/{1}*{2}*18'.format(
            e_dict['evaluation'], alias_name, rho_mol_dict['evaluation']
            )
        text_dict = {'alias_map': alias_map_list, 'evaluation': evaluation}
        if build_string:
            return self._make_text_string_from_dict(text_dict=text_dict)
        return text_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def calculate_e(self, build_string=True):
        """
        Generate components for an rtmc-valid calculation of vapour pressure.

        Parameters
        ----------
        build_string : boolean, optional
            If true, converts the dictionary of required components to an
            rtmc-valid string. The default is True.

        Returns
        -------
        str
            If build_string True, produces rtmc-valid string.
        dict
            if build_string False, produces dictionary containing required
            elements for building rtmc string.
        """

        es_dict = self.calculate_es(build_string=False)
        rh_data = self.get_variable('Relative humidity')
        if rh_data.convert:
            alias_name = rh_data.alias_name
        else:
            alias_name = '{}/100'.format(rh_data.alias_name)
        alias_map_list = es_dict['alias_map'] + [rh_data.alias_map]
        evaluation = '{0}*({1})'.format(alias_name, es_dict['evaluation'])
        text_dict = {'alias_map': alias_map_list, 'evaluation': evaluation}
        if build_string:
            return self._make_text_string_from_dict(text_dict=text_dict)
        return text_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def calculate_es(self, build_string=True):

        return {'alias_map': [self.get_variable('Air temperature').alias_map],
                'evaluation': '0.6106*exp(17.27*Ta/(Ta+237.3))'}
    #--------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    def calculate_molar_density(self, build_string=True):

        ps_data = self.get_variable('Surface air pressure')
        if not ps_data.convert:
            ps_alias_name = ps_data.alias_name
        else:
            ps_alias_name = ps_data.evaluated_alias_name
        ps_alias_map = ps_data.alias_map
        Ta_data = self.get_variable('Air temperature')
        Ta_alias_name = Ta_data.alias_name
        Ta_alias_map = Ta_data.alias_map
        alias_map_list = [ps_alias_map, Ta_alias_map]
        evaluation = ('({}*1000/(8.3143*({}+273.15)))'
                       .format(ps_alias_name, Ta_alias_name))
        text_dict = {'alias_map': alias_map_list, 'evaluation': evaluation}
        if build_string:
            return self._make_text_string_from_dict(text_dict=text_dict)
        return text_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def calculate_vpd(self, build_string=True):
        """
        Generate components for an rtmc-valid calculation of vapour pressure
        deficit.

        Parameters
        ----------
        build_string : boolean, optional
            If true, converts the dictionary of required components to an
            rtmc-valid string. The default is True.

        Returns
        -------
        str
            If build_string True, produces rtmc-valid string.
        dict
            if build_string False, produces dictionary containing required
            elements for building rtmc string.
        """

        e_dict = self.calculate_e(build_string=False)
        es_dict = self.calculate_es(build_string=False)
        calc_str = es_dict['evaluation'] + '-' + e_dict['evaluation']
        alias_list = list(set(e_dict['alias_map'] + es_dict['alias_map']))
        text_dict = {'alias_map': alias_list,'evaluation': calc_str}
        if build_string:
            return self._make_text_string_from_dict(text_dict=text_dict)
        return text_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_record_counter(self):

        dummy = self.get_variable(long_name='Battery voltage')
        pdb.set_trace()
        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_rtmc_component_fields(self, screen, component, field=None):

        try:
            return self.rtmc_df.loc[(screen, component), field]
        except KeyError:
            return self.rtmc_df.loc[(screen, component)]
        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_rtmc_component_variables(self, screen, component):

        rtmc_series = self.rtmc_df.loc[(screen, component)]
        rtmc_series['Screen'] = screen
        rtmc_series['Component'] = component
        name_list = rtmc_series.long_name.split(',')
        try:
            axes_list = rtmc_series.axis.split(',')
        except AttributeError:
            axes_list = ['Left'] * len(name_list)
        d = {}
        for i, name in enumerate(name_list):
            try:
                var_obj = self.get_variable(name)
            except KeyError:
                pdb.set_trace()
            if isinstance(var_obj, pd.core.frame.DataFrame):
                pass
            var_obj = var_obj.append(rtmc_series.drop('long_name'))
            var_obj['axis'] = axes_list[i]
            d['info'] = var_obj
        return d
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_rtmc_variable(self, long_name, label=None, build_string=True):

        constructor_dict = {'Vapour pressure deficit': self.calculate_vpd,
                            'Absolute humidity sensor': self.calculate_AH}
        try:
            var_data = self.get_variable(long_name=long_name, label=label)
            if var_data.convert:
                use_name = var_data.evaluated_alias_name
                alias_map = [var_data.alias_map]
            else:
                use_name = var_data.rtmc_name
                alias_map = None
            text_dict = {'alias_map': alias_map, 'evaluation': use_name}
            if build_string:
                return self._make_text_string_from_dict(text_dict=text_dict)
            return text_dict
        except KeyError:
            return constructor_dict[long_name](build_string=build_string)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_time_diff(self, long_name):

        text_dict = (
            self.get_rtmc_variable(long_name=long_name, build_string=False)
            )
        text_dict['evaluation'] = (
            '{0}-Last({0})'.format(text_dict['evaluation'])
            )
        return self._make_text_string_from_dict(text_dict=text_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_rtmc_statistic_over_interval(self, long_name, statistic, interval):

        stat_dict = {'max': 'MaxRunOverTimeWithReset',
                     'min': 'MinRunOverTimeWithReset'}
        reset_dict = {'monthly': 'RESET_MONTHLY', 'daily': 'RESET_DAILY'}
        text_dict = (
            self.get_rtmc_variable(long_name=long_name, build_string=False)
            )
        stat_var, reset_var = stat_dict[statistic], reset_dict[interval]
        calc_var = text_dict['evaluation']
        pdb.set_trace()
        try:
            timestamp_var = text_dict['alias_map'][0]
        except TypeError:
            timestamp_var = calc_var
        stat_string = (
            '{0}({1},Timestamp({2}),{3})'
            .format(stat_var, calc_var, timestamp_var, reset_var)
            )
        text_dict['evaluation'] = stat_string
        return self._make_text_string_from_dict(text_dict=text_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_rtmc_statistic(self, long_name, statistic):

        constructor_dict = {'time_difference': self._get_time_diff}
        return constructor_dict[statistic](long_name)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_alias(self, long_name, label=None, as_string=True):

        pass
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_rtmc_name(df):

    l = []
    for this_var in df.index:
        try:
            l.append(
                '"Server:{}"'.format('.'.join(
                    [df.loc[this_var, 'logger_name'],
                     df.loc[this_var, 'table_name'],
                     df.loc[this_var, 'site_variable_name']]
                    )
                    )
            )
        except TypeError:
            l.append(
                '"{}"'.format(df.loc[this_var, 'file_data_source'] + ':' +
                '.'.join([df.loc[this_var, 'table_name'],
                          df.loc[this_var, 'site_variable_name']])
                )
                )
    return l
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_alias_map(df):

    alias_list, alias_map_list = [], []
    # pdb.set_trace()
    for this_var in df.index:
        alias = this_var[1]
        try:
            if np.isnan(alias):
                alias = df.loc[this_var, 'standard_variable_name']
        except TypeError:
            pass
        rtmc_var = df.loc[this_var, 'rtmc_name']
        alias_list.append(alias)
        alias_map_list.append('Alias({},{});'.format(alias, rtmc_var))
    return alias_list, alias_map_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_component_df(path):

    df = pd.read_excel(path, sheet_name='RTMC_components')
    df.index = (
        pd.MultiIndex.from_tuples(zip(df['Screen name'],
                                      df['RTMC component name']),
                                  names=['screen','component'])
        )
    df.drop(labels=['Screen name', 'RTMC component name'], axis=1, inplace=True)
    df.columns = ['long_name', 'operation', 'axis']
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_master_df(path):

    def converter(val, default_val='None'):
        if len(val) > 0:
            return val
        return default_val

    df = pd.read_excel(
        path, sheet_name='master_variables', index_col='Long name',
        converters={'Variable units': converter}
    )
    df.index.name = 'long_name'
    df.rename({'Variable name': 'variable_name',
               'Variable units':'variable_units'}, axis=1, inplace=True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_site_df(path, site):

    def converter(val, default_val='None'):
        if len(val) > 0:
            return val
        return default_val

    units_dict = {'mg/m^2/s': 'CO2flux*1000/44',
                  'frac': 'RH*100'}

    # Create the site dataframe
    site_df = pd.read_excel(path, sheet_name=site,
                            converters={'Variable units': converter})
    pdb.set_trace()
    site_df.index = (
        pd.MultiIndex.from_tuples(zip(site_df['Long name'],
                                      site_df['Label']),
                                  names=['long_name','label'])
        )
    site_df.drop(labels=['Long name', 'Label'], axis=1, inplace=True)
    site_df.rename({'Variable name': 'site_variable_name',
                    'Variable units': 'site_variable_units',
                    'Table name': 'table_name',
                    'Logger name': 'logger_name',
                    'File data source': 'file_data_source'},
                   axis=1, inplace=True)

    # Get and write rtmc-specific variable name strings
    site_df = site_df.assign(rtmc_name=_make_rtmc_name(df=site_df))

    # Get and write variable standard names and unit strings
    master_df = _make_master_df(path=path)
    site_df = (
        site_df.assign(standard_variable_name=
                       [master_df.loc[x, 'variable_name'] for x in
                        site_df.index.get_level_values(level='long_name')])
        )
    site_df = (
        site_df.assign(standard_variable_units=
                       [master_df.loc[x, 'variable_units'] for x in
                        site_df.index.get_level_values(level='long_name')])
        )

    # Get and write conversion string for each variable
    site_df = site_df.assign(
        convert=lambda x: x['standard_variable_units'] !=
                          x['site_variable_units']
        )

    # Get and write alias strings for each variable
    alias_names, alias_maps = _make_alias_map(df=site_df)
    site_df = site_df.assign(alias_name=alias_names)
    site_df = site_df.assign(alias_map=alias_maps)

    site_df = site_df.assign(
        evaluated_alias_name=
        lambda x: x.site_variable_units.map(units_dict).fillna(
            x.alias_name
            )
        )
    # site_df.drop('convert', axis=1, inplace=True)

    return site_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PUBLIC FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_statistic(stat, reset)->str:

    stat_dict = {'max_limited': 'MaxRunOverTimeWithReset',
                 'max_unlimited': 'MaxRun',
                 'min_limited': 'MinRunOverTimeWithReset',
                 'min_unlimited': 'MinRun'}
    reset_dict = {'monthly': 'RESET_MONTHLY', 'daily': 'RESET_DAILY'}
    first_str = '{}('.format(stat_dict[stat]) + 'Timestamp({}),'
    second_str = '{})'.format(reset_dict[reset])
    return first_str + second_str
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_difference(text_dict):

    pass

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_units(units):

    d = {'mg/m^2/s': 'CO2flux*1000/44',
         'frac': 'RH*100'}
    try:
        return d[units]
    except KeyError:
        pass
#------------------------------------------------------------------------------