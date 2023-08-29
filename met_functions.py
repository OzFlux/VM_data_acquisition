# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 14:05:44 2023

@author: jcutern-imchugh
"""

import ephem
import numpy as np
from pytz import timezone
from timezonefinder import TimezoneFinder

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

CO2_MOL_MASS = 44
H2O_MOL_MASS = 18
K = 273.15
R = 8.3143

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class TimeFunctions():

    def __init__(self, lat, lon, elev, date):

        self.date = date
        self.time_zone = get_timezone(lat=lat, lon=lon)
        self.utc_offset = get_timezone_utc_offset(tz=self.time_zone, date=date)
        obs = ephem.Observer()
        obs.lat = str(lat)
        obs.long = str(lon)
        obs.elev = elev
        obs.date = date
        self.obs = obs

    def get_next_sunrise(self, as_local=True):

        return self._get_rise_set(
            rise_or_set='rise', next_or_last='next', as_utc=not(as_local)
            )

    def get_last_sunrise(self, as_local=True):

        return self._get_rise_set(
            rise_or_set='rise', next_or_last='last', as_utc=not(as_local)
            )

    def get_next_sunset(self, as_local=True):

        return self._get_rise_set(
            rise_or_set='set', next_or_last='next', as_utc=not(as_local)
            )

    def get_last_sunset(self, as_local=True):

        return self._get_rise_set(
            rise_or_set='set', next_or_last='last', as_utc=not(as_local)
            )

    def _get_rise_set(self, rise_or_set, next_or_last, as_utc=True):

        sun = ephem.Sun()
        funcs_dict = {'rise':
                      {'next': self.obs.next_rising(sun).datetime(),
                       'last': self.obs.previous_rising(sun).datetime()},
                      'set':
                      {'next': self.obs.next_setting(sun).datetime(),
                       'last': self.obs.previous_setting(sun).datetime()}
                      }

        if as_utc:
            return funcs_dict[rise_or_set][next_or_last]
        return funcs_dict[rise_or_set][next_or_last] + self.utc_offset
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_co2(data, from_units='mg/m^2/s'):

    if from_units == 'mg/m^2/s':
        return data * 1000 / CO2_MOL_MASS
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_CO2_density(data, from_units='mmol/m^3'):

    if from_units == 'mmol/m^3':
        return data * CO2_MOL_MASS
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_H2O_density(data, from_units='mmol/m^3'):

    if from_units == 'mmol/m^3':
        return data * H2O_MOL_MASS / 10**3
    if from_units == 'kg/m^3':
        return data * 10**3
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_pressure(data, from_units='Pa'):

    if from_units == 'Pa':
        return data / 10**3
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_RH(data, from_units='frac'):

    if from_units == 'frac':
        return data * 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_temperature(data, from_units='K'):

    if from_units == 'K':
        return data - K
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_variable(variable):

    conversions_dict = {'Fco2': convert_co2,
                        'RH': convert_RH,
                        'CO2': convert_CO2_density,
                        'AH_IRGA': convert_H2O_density,
                        'AH_sensor': convert_H2O_density,
                        'Ta': convert_temperature,
                        'ps': convert_pressure}
    return conversions_dict[variable]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_AH_from_RH(**kwargs):

    return (
        calculate_e(Ta=kwargs['Ta'], RH=kwargs['RH']) / kwargs['ps'] *
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * H2O_MOL_MASS
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_CO2_density(**kwargs):

    return (
        kwargs['CO2'] / 10**3 *
        calculate_molar_density(Ta=kwargs['Ta'], ps=kwargs['ps']) * CO2_MOL_MASS
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def calculate_CO2_mole_fraction(**kwargs):

    return (
        (kwargs['CO2_density'] / CO2_MOL_MASS) /
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

    return kwargs['ps'] * 1000 / ((kwargs['Ta'] + K) * R)
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
def calculate_ustar_from_tau_rho(**kwargs):

    return abs(kwargs['tau']) / kwargs['rho']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_timezone(lat, lon):
    """Get the timezone (as region/city)"""

    tf = TimezoneFinder()
    return tf.timezone_at(lng=lon, lat=lat)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_timezone_utc_offset(tz, date, dst=False):
    """Get the UTC offset (local standard or daylight)"""

    tz_obj = timezone(tz)
    utc_offset = tz_obj.utcoffset(date)
    if not dst:
        return utc_offset - tz_obj.dst(date)
    return utc_offset
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

    args_dict = {
        'es': ['Ta'],
        'e': ['Ta', 'RH'],
        'AH_sensor': ['Ta', 'RH', 'ps'],
        'molar_density': ['Ta', 'ps'],
        'CO2_mole_fraction': ['CO2_density', 'Ta', 'ps'],
        'RH': ['Ta', 'AH_sensor', 'ps'],
        'CO2_density': ['Ta', 'ps', 'CO2'],
        'ustar': ['Tau', 'rho']
        }

    func_dict = {
        'es': calculate_es,
        'e': calculate_e,
        'AH_sensor': calculate_AH_from_RH,
        'molar_density': calculate_molar_density,
        'CO2_mole_fraction': calculate_CO2_mole_fraction,
        'RH': calculate_RH_from_AH,
        'CO2_density': calculate_CO2_density,
        'ustar': calculate_ustar_from_tau_rho
        }

    func = func_dict[variable]
    args = {arg: df[arg] for arg in args_dict[variable]}
    return func(**args)
#------------------------------------------------------------------------------
