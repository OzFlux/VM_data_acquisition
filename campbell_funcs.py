#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh
"""

### Standard modules ###
import datetime as dt
import logging
import os
import pandas as pd
import pathlib
import sys

### Custom modules ###
import paths_manager as pm
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
import sparql_site_details as sd


#------------------------------------------------------------------------------
### Constants ###
#------------------------------------------------------------------------------

PATHS = pm.paths()
SITE_DETAILS = sd.site_details()
# logger = logging.getLogger(__name__)

#------------------------------------------------------------------------------

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
                             'must match if specified!')
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
        try:
            if len(df) > 1:
                raise RuntimeError('Dataframes with length > 1 must have a '
                                   'datetime index!')
        except RuntimeError:
            logging.error('Exception occurred: ', exc_info=True)
        date_index = (
            [dt.datetime.combine(dt.datetime.now().date(),
                                 dt.datetime.min.time())
             .strftime('%Y-%m-%d %H:%M:%S')]
            )
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

    rename_dict = {'latitude': 'Latitude', 'longitude': 'Longitude',
                   'elevation': 'Elevation', 'time_zone': 'Time zone',
                   'time_step': 'Time step', 'UTC_offset': 'UTC offset'}

    # Construct site details dataframe and add sunrise / sunset times
    logging.info('Generating site details file')
    details = SITE_DETAILS.get_single_site_details(site=site).copy()
    details.rename(rename_dict, inplace=True)
    details_new = pd.concat([
        pd.Series({'Start year': details.date_commissioned.year}),
        details[rename_dict.values()]
        ])
    details_new.name = details.name
    for var in ['Elevation', 'Time step']:
        details_new.loc[var] = details_new.loc[var].astype(int)
    details_new['sunrise'] = (
        SITE_DETAILS.get_sunrise(site=site, date=dt.datetime.now(), which='next')
        .strftime('%H:%M')
        )
    details_new['sunset'] = (
        SITE_DETAILS.get_sunset(site=site, date=dt.datetime.now(), which='next')
        .strftime('%H:%M')
        )
    details_new['10Hz file'] = get_latest_10Hz_file(site=site)
    df = pd.DataFrame(details_new).T

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
              'CPU:noprogram.cr1', '9999', 'site_details']
    toa5_class = TOA5_file_constructor(data=df, header=header)
    return toa5_class
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_latest_10Hz_file(site):

    data_path = (
        PATHS.get_local_path(resource='data', stream='flux_fast', site=site)
        )
    try:
        return max(data_path.rglob('TOB3*.dat'), key=os.path.getctime).name
    except ValueError:
        return 'No files'
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_latest_10Hz_files():

#     result_dict = {}
#     for site in SITES:
#         latest_file = get_latest_10Hz_file(site)
#         result_dict[site] = '"{}"'.format(latest_file.name)
#     index = [dt.datetime.now().date()]
#     df = pd.DataFrame(data=result_dict, index=index)
#     header = ['TOA5', 'NoStation', 'CR1000', '9999',
#               'cr1000.std.99.99', 'CPU:noprogram.cr1', '9999',
#               'Latest_10Hz']
#     return TOA5_file_constructor(data=df, header=header)
# #------------------------------------------------------------------------------