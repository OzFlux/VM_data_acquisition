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
import numpy as np
import os
import pandas as pd
import pathlib
import sys

### Custom modules ###
import met_functions as mf
import paths_manager as pm
import variable_mapper as vm
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

        amended_names = ['TIMESTAMP'] + self.data.columns.tolist()
        amended_units = ['TS'] + self.units
        amended_samples = [''] + self.samples
        return [
            _fmtd_string_from_list(the_list=self.info_header),
            _fmtd_string_from_list(the_list=amended_names),
            _fmtd_string_from_list(the_list=amended_units),
            _fmtd_string_from_list(the_list=amended_samples)
            ]
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
        if not self.data.index.name == 'TIMESTAMP':
            raise IndexError('Dataframe index must be named TIMESTAMP!')
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
        len_list = [8, self.n_cols, self.n_cols]
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
                    'CPU:noprogram.cr1', '9999', 'default_table']
        elif line == 'units':
            return ['unitless'] * self.n_cols
        elif line == 'samples':
            return ['Smp'] * self.n_cols
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

        _write_TOA5_from_df(
            df=self.data, headers=self.assemble_full_header(), dest=dest
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class table_merger():

    def __init__(self, site):

        self.site = site
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )
        self.time_step = int(SITE_DETAILS.get_single_site_details(
            site=site, field='time_step'
            ))
        self.table_map = get_site_data_tables(site=site)
        self.var_map = vm.mapper(site=site)

    #--------------------------------------------------------------------------
    def get_table_data(self, file):

        full_path = self.path / file
        time_step = get_TOA5_interval(full_path)
        if time_step != f'{self.time_step}T':
            raise RuntimeError(f'Unexpected data table interval for file {file}')
        variable_df = self.var_map.get_variable_fields(table_file=file)
        usecols = ['TIMESTAMP'] + variable_df.site_name.tolist()
        translation_dict = dict(zip(
            variable_df.site_name, variable_df.translation_name
            ))
        start, end = (
            self.table_map.loc[self.table_map.file_name==file, 'start_date'].item(),
            self.table_map.loc[self.table_map.file_name==file, 'end_date'].item()
            )
        new_index = pd.date_range(start=start, end=end, freq=time_step)
        new_index.name = 'TIMESTAMP'
        df = (get_TOA5_data(file=full_path, usecols=usecols)
              .rename(translation_dict, axis=1)
              .reindex(new_index)
              [variable_df.translation_name.tolist()]
              )
        self._do_unit_conversions(df=df)
        self._apply_limits(df=df)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_unit_conversions(self, df):

        converts_df = self.var_map.get_conversion_variables()
        for variable in converts_df.translation_name:
            if not variable in df.columns:
                continue
            data = df[variable]
            units = converts_df.loc[
                converts_df.translation_name==variable, 'site_units'
                ].item()
            func = mf.convert_variable(variable=variable)
            df[variable] = data.apply(func, from_units=units)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, df):

        for var in df.columns:
            limits = self.var_map.get_variable_limits(variable=var)
            if not len(limits):
                continue
            filter_bool = (
                (df[var]<limits.Min.item())|(df[var]>limits.Max.item())
                )
            df.loc[filter_bool, var] = np.nan
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_tables(self):

        df_list = []
        file_list = self.var_map.get_file_list()
        trunc_table_map = (
            self.table_map.reset_index().set_index(keys='file_name')
            .loc[file_list]
            )
        date_idx = pd.date_range(
            start=trunc_table_map.start_date.min(),
            end=trunc_table_map.end_date.max(),
            freq=f'{self.time_step}T'
            )
        date_idx.name = 'TIMESTAMP'
        for file in file_list:
            df_list.append(
                self.get_table_data(file=file)
                .reindex(date_idx)
                )
        df = pd.concat(df_list, axis=1)
        self._make_missing_variables(df=df)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_missing_variables(self, df):

        missing_vars = self.var_map.get_missing_variables()
        for variable in missing_vars.translation_name:
            df[variable] = (
                mf.calculate_variable_from_std_frame(variable=variable, df=df)
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_headers(self):

        df_list = []
        for file in self.var_map.get_file_list():
            df_list.append(self.translate_header(file))
        missing_df = self.var_map.get_missing_variables()
        df_list.append(
            missing_df[['translation_name', 'standard_units']]
            .assign(sampling='')
            .reset_index(drop=True)
            .set_index(keys='translation_name')
            .rename({'standard_units': 'units'}, axis=1)
            )
        return pd.concat(df_list)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_header(self, file):

        header_list = [
            x.strip().replace('"', '').split(',') for x in
            get_file_headers(self.path / file)
            ]
        variables_df = self.var_map.get_variable_fields(
            table_file=file
            )
        return (
            pd.DataFrame(
                data=header_list[3], index=header_list[1],
                columns=['sampling']
                )
            .loc[variables_df.site_name]
            .assign(translation_name=variables_df.translation_name.tolist(),
                    units=variables_df.standard_units.tolist())
            .reset_index(drop=True)
            .set_index(keys='translation_name')
            [['units', 'sampling']]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_output_file(self):

        info_header = [
            'TOA5', self.site, 'CR1000', '9999', 'cr1000.std.99.99',
            'CPU:noprogram.cr1', '9999', 'merged']
        data = self.merge_tables()
        header = self.merge_headers()
        constructor = TOA5_file_constructor(
            data=data, info_header=info_header, units=header.units.tolist(),
            samples=header.sampling.tolist()
            )
        constructor.write_output_file(dest=self.path / f'{self.site}_merged.dat')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class L1_constructor():

    def __init__(self, site, path_to_files, time_step):
        """
        Simple class for generating the collated L1 file from individual TOA5

        Parameters
        ----------
        site : str
            Site name.
        path_to_files : pathlib.Path or str
            A valid path to the directory containing the files for collation.
        time_step : int
            The averaging interval for the site.

        Returns
        -------
        None.

        """

        self.site = site
        self.time_step = time_step
        self.tables_df = get_site_data_tables(site=site)
        self.path = pathlib.Path(path_to_files)

    #--------------------------------------------------------------------------
    def get_encapsulating_date_range(self):
        """
        Find the earliest and latest dates across ALL available files.

        Returns
        -------
        dict
            Start and end dates.

        """

        return pd.date_range(
            start=self.tables_df.start_date.min(),
            end=self.tables_df.end_date.max(),
            freq=str(int(self.time_step)) + 'T',
            name='TIMESTAMP'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def convert_header(self, file):
        """
        Reformats the raw string header and returns it in a dataframe.

        Parameters
        ----------
        file : str
            The file for which to get the formatted header.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing formatted header.

        """

        header = get_file_headers(file=self.path / file)
        header_list = [
            x.strip().replace('"', '').split(',') for x in header
            ]
        return pd.DataFrame(header_list)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_excel(self, dest, na_values=''):
        """
        Write table data out to separate tabs in an xlsx file.

        Parameters
        ----------
        dest : pathlib.Path or str
            Complete file name to write to.
        na_values : str, optional
            The na values to fill nans with. The default is ''.

        Raises
        ------
        FileNotFoundError
            Raised if the directory part of dest does not exist.
        TypeError
            Raised if file extension is not xlsx.

        Returns
        -------
        None.

        """

        dest = pathlib.Path(dest)
        if not dest.parent.exists():
            raise FileNotFoundError('Destination directory does not exist!')
        if not dest.name.split('.')[-1] == 'xlsx':
            raise TypeError('File extension must be of type "xlsx"')
        with pd.ExcelWriter(path=dest) as writer:
            for file in self.tables_df.file_name.tolist():
                table = file.split('.')[0]
                logging.info(f'    Parsing file {file}...')
                headers_df = self.convert_header(file=file)
                headers_df.to_excel(
                    writer, sheet_name=table, header=False, index=False,
                    startrow=0
                    )
                data_df = get_TOA5_data(file=self.path / file)
                data_df = (
                    data_df.reindex(self.get_encapsulating_date_range())
                    .reset_index()
                    )
                data_df.to_excel(
                    writer, sheet_name=table, header=False, index=False,
                    startrow=4, na_rep=na_values
                    )
        logging.info('Collation successful!')
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
                    msg=(
                        'difference in station info header - '
                        f'{key} was {master_info[key]} in master file, '
                        f'{merge_info[key]} in merge file'
                        )
                    if raise_err:
                        raise RuntimeError(f'Error: {msg}')
                    print(f'Warning: {msg}')
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
            if raise_err:
                raise RuntimeError(msg)
            else:
                print(f'Warning: {msg}')
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

        self.compare_header_info(with_file=with_file, raise_err=raise_err)
        for this_line in self.header_lines:
            self.compare_header_row(
                with_file=with_file, which=this_line, raise_err=raise_err
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_files(self, subset=[], raise_err=True):

        master_data = TOA5_data_handler(file=self.master_file).get_data_df()
        file_list = self.merge_file_list if not subset else subset
        df_list = [master_data]
        for file in file_list:
            fname = file.name
            self.compare_headers(with_file=fname, raise_err=raise_err)
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

    #--------------------------------------------------------------------------
    def get_file_interval(self):

        return get_TOA5_interval(file=self.file)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_header_df(self):
        """
        Get a dataframe with variables as index and units and statistical
        sampling type as columns.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe as per above.

        """

        headers = get_file_headers(file=self.file)[1:]
        lines_dict = {}
        for line, var in enumerate(['variable', 'units', 'stat']):
            lines_dict[var] = headers[line].rstrip().replace('"', '').split(',')
        idx = lines_dict.pop('variable')
        return pd.DataFrame(data=lines_dict, index=idx)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
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

        info = get_file_headers(file=self.file)[0]
        if not as_dict:
            return info
        info_list = ['format', 'station_name', 'logger_type', 'serial_num',
                     'OS_version', 'program_name', 'program_sig', 'table_name']
        info_elements = info.replace('\n', '').replace('"', '').split(',')
        return dict(zip(info_list, info_elements))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
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
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
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
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_stats(self, variable):

        df = self.get_header_df()
        return df.loc[variable, 'stats']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_dates(self):

        return get_file_dates(file=self.file)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_df(self):

        return get_TOA5_data(file=self.file)
    #--------------------------------------------------------------------------

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
        df.to_csv(
            f, header=False, index=False, na_rep='NAN',
            quoting=csv.QUOTE_NONNUMERIC
            )
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
def get_file_dates(file):

    date_format = '"%Y-%m-%d %H:%M:%S"'
    with open(file, 'rb') as f:
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_headers(file):

    """
    Get a list of the four TOA5 header strings.

    Returns
    -------
    headers : list
        List of the raw header strings.

    """

    headers = []
    with open(pathlib.Path(file)) as f:
        for i in range(4):
            headers.append(f.readline())
    return headers
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

#--------------------------------------------------------------------------
def get_site_data_tables(site):
    """
    Get a dataframe containing information about the logger tables.

    Raises
    ------
    FileNotFoundError
        Raised if any file does not exist.

    Returns
    -------
    tables_df : pd.core.frame.DataFrame
        The dataframe.

    """

    tables_df = vm.get_data_tables(site=site)
    if not all(tables_df.has_file):
        files = ', '.join(tables_df.loc[~tables_df.has_file, 'file_name'].tolist())
        raise FileNotFoundError(
            f'The following files do not exist: {files}'
            )
    return tables_df
#--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOA5_data(file, usecols=None):

    df = pd.read_csv(
        file, skiprows=[0,2,3], usecols=usecols, parse_dates=['TIMESTAMP'],
        index_col=['TIMESTAMP'], na_values='NAN', sep=',', engine='c',
        on_bad_lines='warn', low_memory=False
        )
    non_nums = df.select_dtypes(include='object')
    for col in non_nums.columns:
        df[col]=pd.to_numeric(non_nums[col], errors='coerce')
    df.drop_duplicates(inplace=True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOA5_interval(file):

    df = get_TOA5_data(file=file, usecols=['TIMESTAMP', 'RECORD']).reset_index()
    df = df - df.shift()
    df['minutes'] = df.TIMESTAMP.dt.components.minutes
    df = df.loc[(df.RECORD==1) & (df.minutes!=0)]
    interval_list = df.minutes.unique().tolist()
    if not len(interval_list) == 1:
        raise RuntimeError('Inconsistent interval between records!')
    return '{}T'.format(str(round(interval_list[0])))
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
    df.index.name = 'TIMESTAMP'
    df.loc[:, 'Start year'] = str(df.loc[:, 'Start year'].item())
    info_header = ['TOA5', site, 'CR1000', '9999', 'cr1000.std.99.99',
                   'CPU:noprogram.cr1', '9999', 'site_details']
    constructor = TOA5_file_constructor(data=df, info_header=info_header)
    output_path = (
        PATHS.get_local_path(resource='site_details') / f'{site}_details.dat'
        )
    constructor.write_output_file(dest=output_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_L1(site):

    IO_path = PATHS.get_local_path(
        resource='data', stream='flux_slow', site=site
        )
    details = SITE_DETAILS.get_single_site_details(site=site).copy()
    constructor = L1_constructor(
        site=site, path_to_files=IO_path, time_step=details.time_step
        )
    output_path = IO_path / f'{site}_L1.xlsx'
    constructor.write_to_excel(dest=output_path)
#------------------------------------------------------------------------------