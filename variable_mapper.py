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

    def __init__(self, data_dict):

        self._data = data_dict
        self.eval_string = data_dict['eval_string']
        self.variable_units = data_dict['variable_units']
        self.alias_name = data_dict['alias_name']
        self.rtmc_name = data_dict['rtmc_name']

    def get_alias_map(self):

        return '\r\n'.join(
            ['Alias({0},{1});'.format(x[0], x[1])
             for x in zip(self.alias_name, self.rtmc_name)]
            )

    def get_intvl_ltd_statistic(self, statistic, interval):

        return _get_intvl_ltd_statistic(
            rtmc_obj=self, statistic=statistic, interval=interval
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_intvl_ltd_statistic(rtmc_obj, statistic, interval):

    stat_dict = {'max_limited': 'MaxRunOverTimeWithReset',
                 'min_limited': 'MinRunOverTimeWithReset'}
    reset_dict = {'daily': 'RESET_DAILY', 'weekly': 'RESET_WEEKLY',
                  'monthly': 'RESET_MONTHLY', }
    start_dict = {'daily': 'nsecPerDay', 'weekly': 'nsecPerWeek',
                  'monthly': 'nsecPerDay*30'}

    try:
        stat_string = stat_dict[statistic]
    except KeyError:
        print('Statistic keyword must be one of the following: {})'
              .format(', '.join(list(stat_dict.keys())))
              );
        raise

    try:
        reset_when = reset_dict[interval]
        start_when = start_dict[interval]
    except KeyError:
        print('Interval keyword must be one of the following: {})'
              .format(', '.join(list(reset_dict.keys())))
              );
        raise

    start_cond = ('StartRelativeToNewest({},OrderCollected);'
                  .format(start_when)
                  )
    alias_map = rtmc_obj.get_alias_map()
    eval_string = rtmc_obj.eval_string
    timestamp_var = rtmc_obj.rtmc_name[0]
    new_eval_string = (
        '{0}({1},Timestamp({2}),{3})'
        .format(stat_string, eval_string, timestamp_var, reset_when)
        )
    return '\r\n\r\n'.join([start_cond, alias_map, new_eval_string])
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
    def get_raw_variable(self, long_name, label=None):
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
        ps_dict = self.get_raw_variable(long_name='Surface air pressure')
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
        rh_dict = self.get_raw_variable(long_name='Relative humidity')
        for this_key in ['rtmc_name', 'alias_name']:
            es_dict[this_key].append(rh_dict[this_key][0])
        es_dict['eval_string'] = '{0}/100*({1})'.format(rh_dict['eval_string'],
                                                        es_dict['eval_string'])
        return es_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_es(self):

        rtmc_dict = self.get_raw_variable(long_name='Air temperature')
        rtmc_dict['variable_units'] = 'kPa'
        rtmc_dict['eval_string'] = '0.6106*exp(17.27*Ta/(Ta+237.3))'
        return rtmc_dict
    #--------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    def _calculate_molar_density(self):

        ps_dict = self.get_raw_variable(long_name='Surface air pressure')
        Ta_dict = self.get_raw_variable(long_name='Air temperature')
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

        dummy = self.get_raw_variable(long_name='Battery voltage')
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
                var_obj = self.get_raw_variable(name)
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
            data_dict = self.get_raw_variable(long_name=long_name)
        except KeyError:
            data_dict = constructor_dict[long_name]()
        return _rtmc_constructor(data_dict=data_dict)
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