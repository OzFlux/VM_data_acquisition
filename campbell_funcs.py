#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh
"""

### Standard modules ###
import datetime as dt
import os
import pandas as pd
import pathlib
import sys

### Custom modules ###
import site_utils as su
import time_functions as tf

#------------------------------------------------------------------------------
### Classes ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class TOA5_file_constructor():

    """Class for construction of TOA5 files"""    

    def __init__(self, data, header=None, units=None, samples=None):

        try:
            if units:
                assert len(units) == len(data.columns)
            else:
                units = len(data.columns)
            if samples:
                assert len(samples) == len(units)
            else:
                samples = len(data.columns)
        except AssertionError:
            raise IndexError('Length of units, sampling and dataframe columns '
                             'must match!')
        self.data = data
        self.header = header
        self.units = units
        self.samples = samples

    def make_data(self, as_str=None):

        return _make_TOA5_data(data=self.data)

    def make_header(self):

        return _make_TOA5_header(header=self.header)

    def make_samples(self):

        return _make_TOA5_line(line_type='samples', list_or_int=self.samples)

    def make_units(self):

        return _make_TOA5_line(line_type='units', list_or_int=self.units)

    def make_variables(self):

        return _make_TOA5_variable_names(df=self.data)

    def make_file_output(self):

        return [self.make_header(), self.make_variables(), self.make_units(),
                self.make_samples()] + self.make_data()

    def output_file(self, file_path):

        """Write output to file
        
        Args:
            * file_path (str or Pathlib.Path): full path and filename for \
              output
        Raises:
            * FileNotFoundError if the parent path doesn't exist
        """

        out_list = self.make_file_output()
        path = pathlib.Path(file_path)
        if not path.parent.exists():
            raise FileNotFoundError('Specified directory does not exist')
        with open(file_path, 'w') as f:
            f.writelines(out_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Private functions ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_TOA5_data(data) -> list:

    """Turn the dataframe data into text lines; for multiple record \
       dataframes a datetime index is required, since all logger records are \
       structured this way
    
    Args:
        * data (dataframe): dataframe containing columns in desired variable \
          order
    Returns:
        * a TOA5-formatted data line str
    Raises:
        *
    """    

    df = data.copy()
    try:
        date_index = df.index.to_pydatetime()
        df.index = [x.strftime('%Y-%m-%d %H:%M:%S') for x in date_index]
    except AttributeError:
        if len(df) > 1:
            raise RuntimeError('Dataframes with length > 1 must have a '
                               'datetime index!')
        date_index = [dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    date_index = ['"' + x + '"' for x in date_index]
    df.index = date_index
    df.reset_index(inplace=True)
    return [','.join(map(str, x)) + '\n' for x in df.values.tolist()]

def _make_TOA5_header(header=None) -> str:

    """Make a TOA5-formatted header
    
    Args:
        * header (list): list of elements to use for header (uses default if None)
    Returns:
        * a TOA5-formatted header line str
    Raises:
        * Runtimeerror if not eight elements in passed header list
    """    

    default_header = ['TOA5', 'NoStation', 'CR1000', '9999',
                      'cr1000.std.99.99', 'CPU:noprogram.cr1', '9999',
                      'Site_details']
    if not header:
        header = default_header
    else:
        if not len(header) == 8:
            raise RuntimeError('Header must contain eight elements!')
    return ','.join(['"' + x + '"' for x in header]) + '\n'

def _make_TOA5_line(line_type, list_or_int) -> str:

    """Make a TOA5-formatted line
    
    Args:
        * line_type (str): either 'units' or 'samples' 
        * list_or_int (list or int): either a list of elements or an integer \
          requesting the number of default elements
    Returns:
        * a TOA5-formatted line str of requested type
    """    

    if line_type == 'units':
        default_str = 'unitless'
        time_str = 'ts'
    elif line_type == 'samples':
        default_str = 'Smp'
        time_str = ''
    else:
        raise TypeError('line_type must be either "units" or "samples"')
    if isinstance(list_or_int, int):
        out_list = [time_str] + [default_str] * list_or_int
    elif isinstance(list_or_int, list):
        out_list = list_or_int
    out_list = ['"' + x + '"' for x in out_list]
    return ','.join(out_list) + '\n'

def _make_TOA5_variable_names(df):

    """Make a TOA5-formatted variable name line"""    

    return (
        ','.join(['"TIMESTAMP"'] +
                 ['"' + x + '"' for x in df.columns]) +
        '\n'
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Public functions ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_info_TOA5(site, num_to_str=None):

    """Make TOA5 constructor containing details from site_master
    
    Args:
        * num_to_str (list): variables to encase in double quotes \
          (will be read as str)
    Returns:
        * TOA5 file constructor class populated with site_info
    Raises:
        * TypeError if num_to_str not of type list
    """
    
    # Construct site details dataframe and add sunrise / sunset times
    details = su.get_site_details(site)
    sun = tf.get_sunrise_sunset_local(lat=str(details.Latitude), 
                                      lon=str(details.Longitude), 
                                      elev=details.Altitude, 
                                      tz=details['Time zone'], 
                                      date=dt.datetime.now())
    utc_offset = tf.get_timezone_utc_offset(details['Time zone'], 
                                            date=dt.datetime.now())
    details['sunrise'] = sun['rising'].strftime('%H:%M')
    details['sunset'] = sun['setting'].strftime('%H:%M')
    details['UTC offset'] = utc_offset.seconds / 3600
    df = pd.DataFrame(details).T
    
    # Check passed num_to_str arg is correct
    if not num_to_str:
        num_to_str = []
    else:
        if not isinstance(num_to_str, list):
            raise TypeError('num_to_str must be of type list!')
    
    # Add double quotes to all non-numeric data or numeric data we want as str
    for this_col in df.columns:
        do_flag = False
        if this_col in num_to_str:
            do_flag = True
        else:
            try:
                int(df[this_col].item())
            except ValueError:
                do_flag = True
        if do_flag:
            df.loc[:, this_col]='"{}"'.format(df.loc[:, this_col].item())
    header = ['TOA5', df.index[0], 'CR1000', '9999', 'cr1000.std.99.99', 
              'CPU:noprogram.cr1', '9999', 'Site_details']           
    toa5_class = TOA5_file_constructor(data=df, header=header)
    return toa5_class

def make_site_10Hz_update():
    
    """Make TOA5 constructor containing latest 10Hz files for sites"""
    
    data_path = su.get_data_path(data='flux_raw_fast') / 'TOB3'
    site_list = ['Boyagin', 'Calperum', 'GreatWesternWoodlands']
    result_dict = {}
    for site in site_list:
        site_data_path = pathlib.Path(str(data_path).replace('<site>', site))
        if not site_data_path.exists():
            raise FileNotFoundError('path does not exist')
        gen = site_data_path.rglob('TOB3*.dat')
        fname = '"{}"'.format(max(gen, key=os.path.getctime).name)
        result_dict.update({site: fname})
    index = [dt.datetime.now().date()]
    df = pd.DataFrame(data=result_dict, index=index)
    toa5_class = TOA5_file_constructor(data=df)
    return toa5_class
#------------------------------------------------------------------------------

#-----------------------------------------------------------------------------
### Main ###
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    
    # If passed 'update_10Hz' as argument:
    if sys.argv[1] == 'update_10Hz':
        path = (pathlib.Path(su.get_base_path(to='generic_RTMC_files')) /
                'latest_fast_data.dat')
        a = make_site_10Hz_update()
        a.output_file(path)