#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 07:31:34 2021

@author: imchugh

To do:
    - deal with variables that have multiple labels
    - why does it take so long to create the rtmc constructor? Check where...
"""

import numpy as np
import pandas as pd
import pdb

path='/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'

#------------------------------------------------------------------------------
class _rtmc_constructor():

    def __init__(self, data_series):

        if not isinstance(data_series, pd.core.series.Series):
            raise TypeError('data_series must be a pandas Series!')

        self._data = data_series
        self.eval_alias = data_series.evaluated_alias_name
        self.alias_name = [data_series.alias_name]
        self.rtmc_name = [self._get_rtmc_variable_name()]
        self.primary_data = True
        self.alias_map = (
            ['Alias({},{});'.format(data_series.alias_name,
                                    self.rtmc_name[0])]
            )

    def get_rtmc_output(self, as_alias=False):

        if self._data.convert:
            as_alias = True
        if not as_alias:
            return _list_to_str_with_crlf(self.rtmc_name)
        return (
            _list_to_str_with_crlf(self.alias_map) +
            '\r\n\r\n' +
            self.eval_alias
            )

    def _get_rtmc_variable_name(self):

        try:
            return '"Server:{}"'.format(
                '.'.join([self._data.logger_name, self._data.table_name,
                          self._data.site_variable_name])
                )
        except TypeError:
            return '"{}"'.format(
                self._data.file_data_source + ':' +
                '.'.join([self._data.table_name, self._data.site_variable_name])
                )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class _rtmc_sec_constructor():

    def __init__(self, data_dict):


        self._data = data_dict['data_series']
        self.eval_alias = data_dict['eval_string']
        self.alias_name = self._data.index.tolist()
        self.rtmc_name = self._data.tolist()
        self.primary_data = False
        self.alias_map = self._get_alias_map()

    def get_rtmc_output(self):

        return '\r\n'.join([self.alias_map]) + '\r\n\r\n' + self.eval_alias

    def _get_alias_map(self):

        return [
            'Alias({0},{1});'.format(x[0], x[1]) for x in
             zip(self._data.index, self._data.tolist())
            ]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class _calculation_constructor():

    stat_dict = {'max_limited': 'MaxRunOverTimeWithReset',
                  'max_unlimited': 'MaxRun',
                  'min_limited': 'MinRunOverTimeWithReset',
                  'min_unlimited': 'MinRun'}

    reset_dict = {'monthly': 'RESET_MONTHLY', 'daily': 'RESET_DAILY'}

    def __init__(self, rtmc_obj, **kwargs):

        self.rtmc_obj = rtmc_obj
        self.args = kwargs

    def max_limited(self, interval):

        stat = self.stat_dict['max_limited']
        reset = self.reset_dict[interval]
        eval_string = self.rtmc_obj.eval_alias
        timestamp_var = self.rtmc_obj.rtmc_name[0]
        stat_string = (
            '{0}({1},Timestamp({2}),{3})'
            .format(stat, eval_string, timestamp_var, reset)
            )
        alias_map_string = _list_to_str_with_crlf(self.rtmc_obj.alias_map)
        return alias_map_string + '\r\n\r\n' + stat_string
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class variable_mapper():

    def __init__(self, path, site):

        self.site = site
        self.rtmc_df = _make_component_df(path=path)
        self.master_df = _make_master_df(path=path)
        self.site_df = _make_site_df(path=path, site=site)

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
    def get_variable(self, long_name, label=None):
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
        if isinstance(pd_obj, pd.core.frame.DataFrame):
            raise NotImplementedError('doesnt work for dataframes yet!')
        parse_list = ['variable_units', 'rtmc_name', 'alias_name',
                      'eval_string']
        parse_dict = pd_obj[parse_list].to_dict()
        output_dict = {x: [parse_dict[x]] for x in ['rtmc_name', 'alias_name']}
        parse_dict.update(output_dict)
        return parse_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_rtmc_constructor(self, long_name):

        data_series = self.get_variable(long_name)
        return _rtmc_constructor(data_series=data_series)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_AH(self):
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

        e_dict = self._calculate_e()
        rho_dict = self._calculate_molar_density()
        ps_dict = self.get_variable(long_name='Surface air pressure')
        eval_string = '{0}/{1}*({2})*18'.format(
            e_dict['eval_string'], ps_dict['eval_string'],
            rho_dict['eval_string']
            )
        temp_df = (pd.concat(
            [pd.DataFrame(e_dict), pd.DataFrame(rho_dict),
             pd.DataFrame(ps_dict)])
            [['alias_name', 'rtmc_name']]
            )
        temp_df.drop_duplicates(inplace=True)
        new_dict = {'eval_string': eval_string, 'variable_units': 'g/m^3'}
        new_dict.update({x: temp_df[x].tolist() for x in temp_df.columns})
        return new_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_e(self):
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

        es_dict = self._calculate_es()
        rh_dict = self.get_variable(long_name='Relative humidity')
        for this_key in ['rtmc_name', 'alias_name']:
            es_dict[this_key].append(rh_dict[this_key][0])
        es_dict['eval_string'] = '{0}/100*({1})'.format(rh_dict['eval_string'],
                                                        es_dict['eval_string'])
        return es_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_es(self):

        rtmc_dict = self.get_variable(long_name='Air temperature')
        rtmc_dict['variable_units'] = 'kPa'
        rtmc_dict['eval_string'] = '0.6106*exp(17.27*Ta/(Ta+237.3))'
        return rtmc_dict
    #--------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    def _calculate_molar_density(self):

        ps_dict = self.get_variable(long_name='Surface air pressure')
        Ta_dict = self.get_variable(long_name='Air temperature')
        for this_key in ['rtmc_name', 'alias_name']:
            ps_dict[this_key].append(Ta_dict[this_key][0])
        ps_dict['eval_string'] = (
            '{}*1000/(8.3143*({}+273.15))'
            .format(ps_dict['eval_string'], Ta_dict['eval_string'])
            )
        ps_dict['variable_units'] = 'mol/m^3'
        return ps_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_vpd(self):
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

        e_dict = self._calculate_e()
        es_dict = self._calculate_es()
        e_dict['eval_string'] = (
            es_dict['eval_string'] + '-' + e_dict['eval_string']
            )
        return e_dict
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
    def get_rtmc_variable(self, long_name, label=None):

        constructor_dict = {'Vapour pressure deficit': self._calculate_vpd,
                            'Absolute humidity sensor': self._calculate_AH,
                            'Saturation vapour pressure': self._calculate_es}
        try:
            return self._get_rtmc_constructor(long_name=long_name)
        except KeyError:
            data_dict = constructor_dict[long_name]()
            return _rtmc_sec_constructor(data_dict=data_dict)
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

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _list_to_str_with_crlf(the_list):

    return '\r\n'.join(the_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_alias(series):

    try:
        if np.isnan(series.name[1]):
            return series.variable_name
    except TypeError:
        return series.name[1]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_rtmc_variable_name(series):

    try:
        return '"Server:{}"'.format(
            '.'.join([series.logger_name, series.table_name,
                      series.site_variable_name])
            )
    except TypeError:
        return '"{}"'.format(
            series.file_data_source + ':' +
            '.'.join([series.table_name, series.site_variable_name])
            )
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

    # Write variable standard names and unit strings
    master_df = _make_master_df(path=path)
    site_df = (
        site_df.assign(variable_name=
                       [master_df.loc[x, 'variable_name'] for x in
                        site_df.index.get_level_values(level='long_name')])
        )
    site_df = (
        site_df.assign(variable_units=
                       [master_df.loc[x, 'variable_units'] for x in
                        site_df.index.get_level_values(level='long_name')])
        )

    # # Write unit conversion boolean for each variable
    # site_df = site_df.assign(
    #     convert=lambda x: x['standard_variable_units'] !=
    #                       x['site_variable_units']
    #     )

    # Write variable name for each variable
    site_df = site_df.assign(
        rtmc_name=lambda x: x.apply(_get_rtmc_variable_name, axis=1)
        )

    # Write alias string for each variable
    site_df = site_df.assign(
        alias_name=
        lambda x: x.apply(_make_alias, axis=1)
        )

    # Write alias conversion string for each variable
    site_df = site_df.assign(
        eval_string=
        lambda x: x.site_variable_units.map(units_dict).fillna(
            x.alias_name
            )
        )

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


