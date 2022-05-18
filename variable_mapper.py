#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 07:31:34 2021

@author: imchugh

To do:
    - deal with variables that have multiple labels
    - deal with compound variables breaking when subject to statistics
      (e.g. try getting the max of VPD)
"""

import numpy as np
import pandas as pd
import pdb

path = '/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
COND_DICT = {'start': 'StartRelativeToNewest({},OrderCollected);'}
RESET_DICT = {'daily': 'RESET_DAILY', 'weekly': 'RESET_WEEKLY',
              'monthly': 'RESET_MONTHLY', }
START_DICT = {'daily': 'nsecPerDay', 'weekly': 'nsecPerWeek',
              'monthly': 'nsecPerDay*30'}
STAT_DICT = {'max_limited': 'MaxRunOverTimeWithReset',
             'min_limited': 'MinRunOverTimeWithReset',
             'total_limited': 'TotalOverTimeWithReset',
             'max': 'MaxRunOverTime', 'min': 'MinRunOverTime',
             'total': 'TotalOverTime'}
COMPONENT_DICT = {'Image': '10702',
                  'Digital': '10101',
                  'TimeSeriesChart': '10602',
                  'Time': '10108',
                  'BasicStatusBar': '10002',
                  'MultiStateAlarm': '10207',
                  'CommStatusAlarm': '10205',
                  'MultiStateImage': '10712',
                  'NoDataAlarm': '10204'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class rtmc_component_mapper():

    def __init__(self, path, screen):

        self.rtmc_df = _make_component_df(path=path).loc[screen]
        self.screen = screen

    def get_component_attr(self, component, attr=None):

        s = self.rtmc_df.loc[component]
        if not attr:
            return s
        return s.loc[attr]

    def get_TimeSeriesChart_aliases(self, chart_name):

        component_type = chart_name.split('_')[-1]
        if not component_type == 'TimeSeriesChart':
            raise KeyError('chart_name argument does not contain a recognised'
                           ' component type!')
        s = self.rtmc_df.loc[chart_name]
        return dict(zip(
            s.label.split(','),
            s.long_name.split(',')
            )
            )
    #--------------------------------------------------------------------------


#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class _rtmc_constructor():

    def __init__(self, data_dict):
        """
        Creates object that generates rtmc output from attributes and methods.

        Parameters
        ----------
        data_dict : dict
            Dictionary containing requisite key:value pairs (see attributes).

        Returns
        -------
        None.

        """

        self.eval_string = data_dict['eval_string']
        self.long_name = data_dict['long_name']
        self.variable_units = data_dict['variable_units']
        self.alias_name = data_dict['alias_name']
        self.rtmc_name = data_dict['rtmc_name']
        self.parse_as_raw = data_dict['parse_as_raw']
        self.alias_map = self._get_alias_map()
        self.rtmc_output = self._get_rtmc_output()

    def _get_alias_map(self):
        """
        Returns
        -------
        str
            DESCRIPTION.

        """

        return _str_joiner(
            ['Alias({0},{1});'.format(x[0], x[1])
             for x in zip(self.alias_name, self.rtmc_name)],
            joiner='\r\n'
        )

    def _get_rtmc_output(self, as_alias=False):

        if not as_alias:
            if self.parse_as_raw:
                return _str_joiner(self.rtmc_name)
        return _str_joiner([self.get_alias_map(), self.eval_string])

    def get_interval_statistic(self, statistic, interval):

        start = COND_DICT['start'].format(START_DICT[interval])
        alias = self.get_alias_map()
        eval_string = (
            '{0}({1},Timestamp({2}),{3})'
            .format(STAT_DICT[statistic], self.eval_string,
                    self.rtmc_name[0], RESET_DICT[interval])
            )
        return _str_joiner([start, alias, eval_string])

    def get_absolute_statistic(self, statistic):

        eval_string = self.get_rtmc_output()
        if self.parse_as_raw:
            return '{0}({1})'.format(STAT_DICT[statistic], eval_string)
        alias = self.get_alias_map()
        eval_string = '{0}({1})'.format(STAT_DICT[statistic], eval_string)
        return _str_joiner([alias, eval_string])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class stat_generator():

    def __init__(self, eval_str, alias_map=None)->str:

        self.eval_str = eval_str
        self.alias_map = alias_map

    def _str_joiner(self, str_list, joiner='\r\n\r\n'):

        return joiner.join(str_list)

    def get_absolute_statistic(self, statistic):

        if not self.alias_map:
            return '{0}({1})'.format(STAT_DICT[statistic], self.eval_str)
        new_eval_str = '{0}({1})'.format(STAT_DICT[statistic], self.eval_str)
        return self._str_joiner([self.alias_map, new_eval_str])

    def get_interval_statistic(self, statistic, interval):

        start = COND_DICT['start'].format(START_DICT[interval])
        # alias = self.alias_map
        eval_string = (
            '{0}({1},Timestamp({2}),{3})'
            .format(STAT_DICT[statistic], self.eval_string,
                    self.rtmc_name[0], RESET_DICT[interval])
            )
        return self._str_joiner([start, self.alias_map, eval_string])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class variable_mapper():

    def __init__(self, path, site):

        self.site = site
        self.rtmc_df = _make_component_df(path=path)
        self.master_df = _make_master_df(path=path)
        self.site_df = _make_site_df(path=path, site=site)

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
                      'eval_string', 'parse_as_raw', 'disable',
                      'default_value']
        parse_dict = pd_obj[parse_list].to_dict()
        output_dict = {x: [parse_dict[x]] for x in ['rtmc_name', 'alias_name']}
        parse_dict.update(output_dict)
        parse_dict['long_name'] = long_name
        return parse_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_calculated_variable(self, long_name):

        constructor_dict = {'Vapour pressure deficit': self._calculate_vpd,
                            'Absolute humidity sensor': self._calculate_AH,
                            'Saturation vapour pressure': self._calculate_es,
                            'Vapour pressure': self._calculate_e,
                            'Records': self._calculate_records}
        data_dict = constructor_dict[long_name]()
        data_dict['long_name'] = long_name
        data_dict['parse_as_raw'] = False
        return data_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_two_variable_operation(self, long_name_first, long_name_second):

        ops_dict = {}
        first_var = self.get_rtmc_variable(long_name=long_name_first)
        second_var = self.get_rtmc_variable(long_name=long_name_second)
        data_dict = {}
        data_dict['alias_name'] = first_var.alias_name + second_var.alias_name
        data_dict['rtmc_name'] = first_var.rtmc_name + second_var.rtmc_name
        data_dict['eval_string'] = (
            '{0}-{1}'.format(first_var.eval_string, second_var.eval_string)
            )
        data_dict['variable_units'] = first_var.variable_units
        data_dict['long_name'] = (
            'Difference between {0} and {1}'.format(first_var.long_name,
                                                    second_var.long_name)
            )
        data_dict['parse_as_raw'] = False
        return data_dict    # #--------------------------------------------------------------------------
    # def get_rtmc_component_variables(self, screen, component):

    #     rtmc_dict = self.rtmc_df.loc[(screen, component)].to_dict()
    #     rtmc_dict['Screen'] = screen
    #     rtmc_dict['Component'] = component
    #     name_list = rtmc_dict['long_name'].split(',')
    #     try:
    #         axis_list = rtmc_dict['axis'].split(',')
    #     except AttributeError:
    #         axis_list = ['Left'] * len(name_list)
    #     if not len(name_list) == len(axis_list):
    #         raise RuntimeError('Number of comma-separated items in sheet '
    #                            '"RTMC_components" must be the same for '
    #                            'columns "Long name" and "Axis"!')
    #     obj_list = []
    #     for i, this_name in enumerate(name_list):
    #         try:
    #             data_dict = self.get_raw_variable(long_name=this_name)
    #         except KeyError:
    #             data_dict = self.get_calculated_variable(long_name=this_name)
    #         this_axis = axis_list[i]
    #         rtmc_dict['axis'] = this_axis
    #         obj_list.append(
    #             _rtmc_comp_constructor(data_dict=data_dict,
    #                                    component_dict=rtmc_dict)
    #             )
    #     return obj_list
    # #--------------------------------------------------------------------------
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
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
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

    #--------------------------------------------------------------------------
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
    def _calculate_records(self):

        rtmc_dict = self.get_raw_variable(long_name='Battery voltage')
        rtmc_dict['eval_string'] = 'iif(Vbat>-1,1,1)'
        rtmc_dict['long_name'] = 'Records'
        return rtmc_dict
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def _calculate_rh(self):

    #     rho_dict = self._calculate_molar_density()
    #     pdb.set_trace()
    #     rtmc_dict['eval_string'] = 'iif(Vbat>-1,1,1)'
    #     rtmc_dict['long_name'] = 'Records'
    #     return rtmc_dict
    # #--------------------------------------------------------------------------

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
    def get_rtmc_variable(self, long_name, label=None):

        try:
            data_dict = self.get_raw_variable(long_name=long_name)
        except KeyError:
            data_dict = self.get_calculated_variable(long_name=long_name)
        return _rtmc_constructor(data_dict=data_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def VarName_2_LongName(self, var_name):

        bool_idx = self.master_df['variable_name'] == var_name
        if not any(bool_idx):
            raise IndexError('Unknown variable')
        return self.master_df[bool_idx].index.item()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def LongName_2_VarName(self, long_name):

        return self.master_df.loc[long_name, 'variable_name']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_TimeSeriesChart_aliases(self, screen_name, chart_name):

        s = self.rtmc_df.loc[screen_name, chart_name]
        return dict(zip(
            s.label.item().split(','),
            s.long_name.item().split(',')
            )
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_Digital_LongName(self, screen_name, digital_name):

        return (
            self.rtmc_df.loc[(screen_name, digital_name), 'long_name'].item()
            )
    #--------------------------------------------------------------------------

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
                                  names=['screen', 'component'])
    )
    df.drop(labels=['Screen name', 'RTMC component name'],
            axis=1, inplace=True)
    df.columns = ['long_name', 'label', 'operation', 'interval', 'axis']
    return df.sort_index()
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
               'Variable units': 'variable_units'}, axis=1, inplace=True)
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
                                  names=['long_name', 'label'])
    )
    site_df.drop(labels=['Long name', 'Label'], axis=1, inplace=True)
    site_df.rename({'Variable name': 'site_variable_name',
                    'Variable units': 'site_variable_units',
                    'Table name': 'table_name',
                    'Logger name': 'logger_name',
                    'File data source': 'file_data_source',
                    'Disable': 'disable',
                    'Default value': 'default_value'},
                   axis=1, inplace=True)

    # Write variable standard names and unit strings
    master_df = _make_master_df(path=path)
    site_df = (
        site_df.assign(
            variable_name=[master_df.loc[x, 'variable_name'] for x in
                           site_df.index.get_level_values(level='long_name')]
            )
    )
    site_df = (
        site_df.assign(
            variable_units=[master_df.loc[x, 'variable_units'] for x in
                            site_df.index.get_level_values(level='long_name')]
            )
    )

    # Write flag to determine whether raw string can be used in rtmc output
    # strings (for example, raw variable string should not be used if
    # conversion of units is required)
    site_df = site_df.assign(
        parse_as_raw=lambda x: x.site_variable_units==x.variable_units
    )

    # Write variable name for each variable
    site_df = site_df.assign(
        rtmc_name=lambda x: x.apply(_get_rtmc_variable_name, axis=1)
    )

    # Write alias string for each variable
    site_df = site_df.assign(
        alias_name=lambda x: x.apply(_make_alias, axis=1)
    )

    # Write alias conversion string for each variable
    site_df = site_df.assign(
        eval_string=lambda x: x.site_variable_units.map(units_dict).fillna(
            x.alias_name
        )
    )

    return site_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _str_joiner(str_list, joiner='\r\n\r\n'):

    return joiner.join(str_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PUBLIC FUNCTIONS
#------------------------------------------------------------------------------
