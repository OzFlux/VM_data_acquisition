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

#------------------------------------------------------------------------------
class TOA5_concatenator():

    def __init__(self, path, master_file, sub_str, freq):


        self.path = pathlib.Path(path)
        self.master_file = self.path / master_file
        self.sub_str = sub_str
        self.freq=freq
        self._check_inputs()
        self.merge_file_list = self._get_merge_file_list()
        self.header_lines = ['variable', 'units', 'stat']

    #--------------------------------------------------------------------------
    def _check_inputs(self):

        if not self.path.exists():
            raise FileNotFoundError(f'Directory "{self.path}" does not exist!')
        if not self.master_file.exists():
            raise FileNotFoundError(f'File "{self.master_file}" does not exist!')
        if not self.master_file.suffix == '.dat':
            raise TypeError('File must be of type ".dat"')
        if not isinstance(self.sub_str, str):
            raise TypeError('"sub_str" arg must be a string!')
        if not self.sub_str in self.master_file.name:
            raise RuntimeError(
                'content of "sub_str" arg must be found in master file name'
                )
        if not self.freq in [30, 60]:
            raise RuntimeError('"freq" arg must be either 30 or 60')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_merge_file_list(self):

        files = list(self.path.glob(f'*{self.sub_str}*.dat*'))
        files.remove(self.master_file)
        if not files:
            raise RuntimeError('No files to concatenate!')
        return files
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_header_info(self, with_file, raise_err=True):

        illegal_list = ['format', 'logger_type', 'table_name']
        merge_file = self.path / with_file
        if not merge_file in self.merge_file_list:
            raise FileNotFoundError(f'File "{with_file}" not found!')
        master_info = TOA5_data_handler(self.master_file).get_info(as_dict=True)
        merge_info = TOA5_data_handler(merge_file).get_info(as_dict=True)
        for key in master_info:
            if master_info[key] != merge_info[key]:
                if key in illegal_list:
                    msg = f'Illegal mismatch in header info line {key}'
                    raise RuntimeError(msg)
                else:
                    print(
                        'Warning: difference in station info header - '
                        f'{key} was {master_info[key]} in master file, '
                        f'{merge_info[key]} in merge file'
                        )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_header_row(self, with_file, which, raise_err=True):

        master_list = (
            TOA5_data_handler(self.master_file).get_header_df()
            .reset_index()
            .rename({'index': 'variable'}, axis=1)
            .loc[:, which]
            .tolist()
            )
        merge_list = (
            TOA5_data_handler(self.path / with_file).get_header_df()
            .reset_index()
            .rename({'index': 'variable'}, axis=1)
            .loc[:, which]
            .tolist()
            )
        master_not_merge = (
            ', '.join(list(set(master_list) - set(merge_list)))
                )
        if master_not_merge:
            msg = (
                'The following {0} are in master but not in merge file {1}: {2}'
                .format(which, with_file, master_not_merge)
                )
            raise RuntimeError(msg)
        merge_not_master = (
            ', '.join(list(set(merge_list) - set(master_list)))
                )
        if merge_not_master:
            msg = (
                'The following {0} are in merge file {1} but not '
                'in master file: {2}'
                .format(which, with_file, merge_not_master)
                )
            raise RuntimeError(msg)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_headers(self, with_file, raise_err=True):

        self.compare_header_info(with_file=with_file)
        for this_line in self.header_lines:
            self.compare_header_row(with_file=with_file, which=this_line)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_files(self, subset=[]):

        master_data = TOA5_data_handler(file=self.master_file).get_data_df()
        file_list = self.merge_file_list if not subset else subset
        df_list = [master_data]
        for file in file_list:
            fname = file.name
            self.compare_headers(with_file=fname)
            df_list.append(TOA5_data_handler(file=file).get_data_df())
        new_df = (
            pd.concat(df_list)
            .drop_duplicates()
            .sort_index()
            )
        return new_df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_and_write(self, subset=[], file_name=None):

        df = self.merge_files(subset=subset)
        if file_name:
            outfile = self.path / file_name
        else:
            new_name, suffix = self.master_file.name, self.master_file.suffix
            new_name = new_name.replace(suffix, '') + '_merged' + suffix
            outfile = self.path / new_name
        headers = TOA5_data_handler(self.master_file).get_headers()
        _write_TOA5_from_df(df=df, headers=headers, dest=outfile)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class TOA5_data_handler():

    def __init__(self, file):
        """
        Class to get headers and data from a TOA5 file.

        Parameters
        ----------
        file : str
            Full path to file. Can be string or pathlib.Path.

        """

        self.file = pathlib.Path(file)

    def get_headers(self):
        """
        Get a list of the four TOA5 header strings.

        Returns
        -------
        headers : list
            List of the raw header strings.

        """

        headers = []
        with open(pathlib.Path(self.file)) as f:
            for i in range(4):
                headers.append(f.readline())
        return headers

    def get_header_df(self):
        """
        Get a dataframe with variables as index and units and statistical
        sampling type as columns.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe as per above.

        """

        headers = self.get_headers()[1:]
        lines_dict = {}
        for line, var in enumerate(['variable', 'units', 'stat']):
            lines_dict[var] = headers[line].rstrip().replace('"', '').split(',')
        idx = lines_dict.pop('variable')
        return pd.DataFrame(data=lines_dict, index=idx)

    def get_info(self, as_dict=True):
        """
        Get the first line with the station info from the logger.

        Parameters
        ----------
        as_dict : Bool, optional
            Returns a dictionary with elements split and assigned descriptive
            keys if true, else just the raw string The default is True.

        Returns
        -------
        str or dict
            Station info header line.

        """

        info = self.get_headers()[0]
        if not as_dict:
            return info
        info_list = ['format', 'station_name', 'logger_type', 'serial_num',
                     'OS_version', 'program_name', 'program_sig', 'table_name']
        info_elements = info.replace('\n', '').replace('"', '').split(',')
        return dict(zip(info_list, info_elements))

    def get_variable_list(self):
        """
        Gets the list of variables in the TOA5 header line

        Returns
        -------
        list
            The list.

        """

        df = self.get_header_df()
        return df.index.tolist()

    def get_variable_units(self, variable):
        """
        Gets the units for a given variable

        Parameters
        ----------
        variable : str
            The variable for which to return the units.

        Returns
        -------
        str
            The units.

        """

        df = self.get_header_df()
        return df.loc[variable, 'units']

    def get_variable_stats(self, variable):

        df = self.get_header_df()
        return df.loc[variable, 'stats']

    def get_dates(self):

        date_format = '"%Y-%m-%d %H:%M:%S"'
        with open(self.file, 'rb') as f:
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

    def get_data(self):

        date_format = '"%Y-%m-%d %H:%M:%S"'
        dates, data = [], []
        with open(self.file) as f:
            for i, line in enumerate(f):
                if i < 4: continue
                line_list = line.split(',')
                dates.append(dt.datetime.strptime(line_list[0], date_format))
                data.append(line)
        return dict(zip(dates, data))

    def get_data_df(self):

        return pd.read_csv(
            self.file, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
            index_col=['TIMESTAMP'], na_values='NAN', sep=',', engine='c',
            on_bad_lines='warn'
            )

        # headers = _read_headers(file_path=self.file)
        # header_list = (
        #     [x.replace('"', '').strip().split(',') for x in headers]
        #     )
        # index = ['Name', 'Units', 'Sampling']
        # if incl_prog_info:
        #     padded_header = (
        #         header_list[0] +
        #         [''] * (len(header_list[1]) - len(header_list[0]))
        #         )
        #     header_list = [padded_header] + header_list[1:]
        #     index = ['Program Info'] + index
        # headers_df = pd.DataFrame(data=header_list, index=index)
        # return headers_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_TOA5_from_df(df, headers, dest):

    df = df.reset_index()
    df.fillna('NAN', inplace=True)
    with open(dest, 'w', newline='\n') as f:
        for line in headers:
            f.write(line)
        df.to_csv(f, header=False, index=False, na_rep='NAN',
                  quoting=csv.QUOTE_NONNUMERIC)
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
        resource='data', stream='flux_fast', site=site, subdirs=['TOB3']
        )
    try:
        return max(data_path.rglob('TOB3*.dat'), key=os.path.getctime).name
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
