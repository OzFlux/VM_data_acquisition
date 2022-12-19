#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh
"""

### Standard modules ###
import csv
import datetime as dt
import logging
import os
import pandas as pd
import pathlib
import sys
import pdb

### Custom modules ###
import paths_manager as pm
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
import sparql_site_details as sd

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

PATHS = pm.paths()
SITE_DETAILS = sd.site_details()

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class TOA5_file_constructor():

    """Class for construction of TOA5 files"""

    def __init__(self, data, info_header=None, units=None, samples=None):


        self.data = data
        self.n_cols = len(self.data.columns)
        self._do_data_checks()
        self.info_header = (
            info_header if info_header else
            self._get_header_defaults(line='info')
            )
        self.units = (
            units if units else self._get_header_defaults(line='units')
            )
        self.samples = (
            samples if samples else self._get_header_defaults(line='samples')
            )

        self._do_header_checks()

    #--------------------------------------------------------------------------
    def assemble_full_header(self):
        """
        Put together the headers from inputs or defaults

        Returns
        -------
        str_list : list
            List of strings, each one corresponding to a header row.

        """

        str_list = []
        str_list.append(_fmtd_string_from_list(the_list=self.info_header))
        str_list.append(_fmtd_string_from_list(
            the_list=['TIMESTAMP'] + self.data.columns.tolist(),
            ))
        str_list.append(_fmtd_string_from_list(the_list=self.units))
        str_list.append(_fmtd_string_from_list(the_list=self.samples))
        return str_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_data_checks(self):
        """
        Basic checks of data integrity - really just that there is a timestamp
        based index

        Raises
        ------
        RuntimeError
            Raised if not a timestamp-based index.

        Returns
        -------
        None.

        """

        try:
            self.data.index.to_pydatetime()
        except AttributeError:
            msg = 'Dataframe passed to "data" arg must have a datetime index!'
            logging.error(msg)
            raise RuntimeError(msg)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_header_checks(self):
        """
        Does integrity checks on passed header lines.

        Raises
        ------
        TypeError
            Raised if passed items were not lists.
        IndexError
            Raised if wrong number of elements in list.
        AssertionError
            Raised if not all list elements are strings.

        Returns
        -------
        None.

        """

        item_list = ['info_header', 'units', 'samples']
        len_list = [8, self.n_cols + 1, self.n_cols + 1]
        for i, item in enumerate([self.info_header, self.units, self.samples]):
            if item:
                item_name, item_len = item_list[i], len_list[i]
                try:
                    if not isinstance(item, list):
                        raise TypeError(
                            f'"{item_name}" kwarg must be of type list!'
                            )
                    if not len(item) == item_len:
                        raise IndexError(
                            'Number of elements in list passed to '
                            f'"{item_name}" must match number of elements '
                            'in passed dataframe!'
                            )
                    for x in item:
                        assert isinstance(x, str)
                except (TypeError, IndexError, AssertionError) as e:
                    msg = (
                        'Integrity check failed with the following message: '
                        f'{e}'
                        )
                    logging.error(msg); raise
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_header_defaults(self, line):
        """
        Get default header output for the given line

        Parameters
        ----------
        line : str
            The line for which to return the defaults (info, units and samples).

        Raises
        ------
        KeyError
            Raised if the "line" argument isn't one of the above.

        Returns
        -------
        list
            The list of default elements for the given line.

        """

        ok_list = ['info', 'units', 'samples']
        if not line in ok_list:
            msg = '"line" arg must be one of {}'.format(', '.join(ok_list))
            logging.error(msg); raise KeyError
        if line == 'info':
            return ['TOA5', 'NoStation', 'CR1000', '9999', 'cr1000.std.99.99',
                    'CPU:noprogram.cr1', '9999', 'Site_details']
        elif line == 'units':
            return ['ts'] + ['unitless'] * self.n_cols
        elif line == 'samples':
            return [''] + ['Smp'] * self.n_cols
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_output_file(self, dest):
        """
        Write the output file to it's destination'

        Parameters
        ----------
        dest : pathlib.Path
            The destination path for the output file.

        Returns
        -------
        None.

        """

        header_lines = self.assemble_full_header()
        data = self.data.reset_index()
        timestamps = data['index'].apply(
            dt.datetime.strftime, format='%Y-%m-%d %H:%M:%S'
            )
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

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _fmtd_string_from_list(the_list)->str:
    """
    Return a joined and quoted string from a list of strings

    Parameters
    ----------
    the_list : list
        List of strings to format and join.

    Returns
    -------
    str
        The formatted string.

    """

    return ','.join([f'"{item}"' for item in the_list]) + '\n'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PUBLIC FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_latest_10Hz_file(site):

    data_path = PATHS.get_local_path(
        resource='data', stream='flux_fast', site=site, subdirs=['TMP']
        )
    try:
        return max(data_path.glob('TOB3*.dat'), key=os.path.getctime).name
    except ValueError:
        return 'No files'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_info_TOA5(site):

    """Make TOA5 constructor containing details from site_master"""

    rename_dict = {'latitude': 'Latitude', 'longitude': 'Longitude',
                   'elevation': 'Elevation', 'time_zone': 'Time zone',
                   'time_step': 'Time step', 'UTC_offset': 'UTC offset'}

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
    df.index = [
        dt.datetime.combine(dt.datetime.now().date(), dt.datetime.min.time())
        ]
    df.loc[:, 'Start year'] = str(df.loc[:, 'Start year'].item())
    info_header = ['TOA5', site, 'CR1000', '9999', 'cr1000.std.99.99',
                   'CPU:noprogram.cr1', '9999', 'site_details']
    constructor = TOA5_file_constructor(data=df, info_header=info_header)
    output_path = (
        PATHS.get_local_path(resource='site_details') / f'{site}_details.dat'
        )
    constructor.write_output_file(dest=output_path)
#------------------------------------------------------------------------------
