# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 14:05:44 2023

@author: jcutern-imchugh
"""

import numpy as np

#------------------------------------------------------------------------------
def convert_co2(data, from_units='mg/m^2/s'):

    if from_units == 'mg/m^2/s':
        return data * 1000 / 44
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_RH(data, from_units='frac'):

    if from_units == 'frac':
        return data * 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_variable(variable):

    conversions_dict = {'Fco2': convert_co2,
                        'RH': convert_RH}
    return conversions_dict[variable]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_AH_from_RH(**kwargs):

    return (
        calculate_e(Ta=kwargs['Ta'], RH=kwargs['RH']) / kwargs['ps'] *
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * 18
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_CO2_density(**kwargs):

    return (
        kwargs['CO2'] / 10**3 *
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * 44
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_CO2_mole_fraction(**kwargs):

    return (
        (kwargs['CO2_density'] / 44) /
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * 10**3
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_e(**kwargs):

    return calculate_es(Ta=kwargs['Ta']) * kwargs['RH'] / 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_es(**kwargs):

    return 0.6106 * np.exp(17.27 * kwargs['Ta'] / (kwargs['Ta'] + 237.3))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_molar_density(**kwargs):

    return kwargs['ps'] * 1000 / ((kwargs['Ta'] + 273.15) * 8.3143)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_RH_from_AH(**kwargs):

    e = (
        (kwargs['AH_sensor'] / 18) /
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * kwargs['ps']
        )
    es = calculate_es(Ta=kwargs['Ta'])
    return e / es * 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_function(variable):

    calculate_dict = {'es': calculate_es,
                      'e': calculate_e,
                      'AH_sensor': calculate_AH_from_RH,
                      'molar_density': calculate_molar_density,
                      'CO2_mole_fraction': calculate_CO2_mole_fraction,
                      'RH': calculate_RH_from_AH,
                      'CO2_density': calculate_CO2_density}
    return calculate_dict[variable]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_variable_from_std_frame(variable, df):

    args_dict = {'es': ['Ta'],
                 'e': ['Ta', 'RH'],
                 'AH_sensor': ['Ta', 'RH', 'ps'],
                 'molar_density': ['Ta', 'ps'],
                 'CO2_mole_fraction': ['CO2_density', 'Ta', 'ps'],
                 'RH': ['Ta', 'AH_sensor', 'ps'],
                 'CO2_density': ['Ta', 'ps', 'CO2']}

    func_dict = {'es': calculate_es,
                 'e': calculate_e,
                 'AH_sensor': calculate_AH_from_RH,
                 'molar_density': calculate_molar_density,
                 'CO2_mole_fraction': calculate_CO2_mole_fraction,
                 'RH': calculate_RH_from_AH,
                 'CO2_density': calculate_CO2_density}

    func = func_dict[variable]
    args = {arg: df[arg] for arg in args_dict[variable]}
    return func(**args)
#------------------------------------------------------------------------------
