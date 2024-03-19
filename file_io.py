# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 13:37:33 2023

Todo:
    - might want some more file checks
    - can we rationalise the line / date formatters? Do we need the formatter
    function at all? Roll the line and date formatters together so don't need
    to call both

@author: jcutern-imchugh

Contains basic file input-output functions and structural configurations for
handling both TOA5 and EddyPro files. It has functions to retrieve both data
and headers. It is strictly tasked with parsing files between disk and memory,
checking only structural integrity of the files. It does NOT evaluate data
integrity!
"""

import csv
import datetime as dt
import os

import numpy as np
import pandas as pd
import pathlib

###############################################################################
### CONSTANTS ###
###############################################################################



FILE_CONFIGS = {
    'TOA5': {
        'info_line': 0,
        'header_lines': {'variable': 1, 'units': 2, 'sampling': 3},
        'separator': ',',
        'non_numeric_cols': ['TIMESTAMP'],
        'time_variables': {'TIMESTAMP': 0},
        'na_values': 'NAN',
        'unique_file_id': 'TOA5',
        'quoting': csv.QUOTE_NONNUMERIC,
        'dummy_info': [
            'TOA5', 'NoStation', 'CR1000', '9999', 'cr1000.std.99.99',
            'CPU:noprogram.cr1', '9999', 'default_table'
            ]
        },
    'EddyPro': {
        'info_line': None,
        'header_lines': {'variable': 0, 'units': 1},
        'separator': '\t',
        'non_numeric_cols': ['DATAH', 'filename', 'date', 'time'],
        'time_variables': {'date': 2, 'time': 3},
        'na_values': 'NaN',
        'unique_file_id': 'DATAH',
        'quoting': csv.QUOTE_MINIMAL,
        'dummy_info': [
            'TOA5', 'SmartFlux', 'SmartFlux', '9999', 'OS', 'CPU:noprogram',
            '9999', 'default_table'
            ]
        }
    }

INFO_FIELDS = [
    'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
    'program_name', 'program_sig', 'table_name'
    ]

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

EDDYPRO_SEARCH_STR = 'EP-Summary'



###############################################################################
### FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN FILE READ / WRITE FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def get_data(file, file_type=None, usecols=None):
    """
    Read the data from the TOA5 file

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.
    usecols : list, optional
        The subset of columns to keep.
        The default is None (all columns retained).

    Returns
    -------
    df : pd.core.frame.DataFrame
        Data.

    """


    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # Get dictionary containing file configurations
    MASTER_DICT = FILE_CONFIGS[file_type]

    # Set rows to skip
    REQ_TIME_VARS = list(MASTER_DICT['time_variables'].keys())
    CRITICAL_FILE_VARS = MASTER_DICT['non_numeric_cols']
    rows_to_skip = list(set([0] + list(MASTER_DICT['header_lines'].values())))
    rows_to_skip.remove(MASTER_DICT['header_lines']['variable'])

    # Usecols MUST include critical non-numeric variables (including date vars)
    # and at least ONE additional column; if this condition is not satisifed,
    # do not subset the columns on import.
    thecols = None
    if usecols and not usecols == CRITICAL_FILE_VARS:
        thecols = (
            CRITICAL_FILE_VARS +
            [col for col in usecols if not col in CRITICAL_FILE_VARS]
            )

    # Now import data
    return (
        pd.read_csv(
            file,
            skiprows=rows_to_skip,
            usecols=thecols,
            parse_dates={'DATETIME': REQ_TIME_VARS},
            keep_date_col=True,
            na_values=MASTER_DICT['na_values'],
            sep=MASTER_DICT['separator'],
            engine='c',
            on_bad_lines='warn',
            low_memory=False
            )
        .set_index(keys='DATETIME')
        .astype({x: object for x in REQ_TIME_VARS})
        .pipe(_integrity_checks, non_numeric=CRITICAL_FILE_VARS)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _integrity_checks(df, non_numeric):
    """
    Check the integrity of data and indices.

    Parameters
    ----------
    df : pd.core.frame.DataFrame
        Dataframe containing the data.
    non_numeric : list
        Column names to ignore when coercing to numeric type.

    Returns
    -------
    df : pd.core.frame.DataFrame
        Dataframe containing the checked / altered data.

    """

    # Coerce non-numeric data in numeric columns
    non_nums = df.select_dtypes(include='object')
    for col in non_nums.columns:
        if col in non_numeric: continue
        df[col] = pd.to_numeric(non_nums[col], errors='coerce')

    # Check the index type, and if bad time data exists, dump the record
    if not df.index.dtype == '<M8[ns]':
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~pd.isnull(df.index)]

    # Sort the index
    return df.sort_index()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_headers(file, begin, end, sep=','):
    """
    Get a list of the header strings.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    begin : int
        Line number of first header line.
    end : int
        Line number of last header line.

    Returns
    -------
    headers : list
        List of the raw header strings.

    """

    line_list = []
    with open(file, 'r') as f:
        for i in range(end + 1):
            line = f.readline()
            if not i < begin:
                line_list.append(line)
    return [line for line in csv.reader(line_list, delimiter=sep)]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_header_df(file, file_type=None):
    """
    Get a dataframe with variables as index and units and statistical
    sampling type as columns.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe as per above.

    """

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    configs_dict = FILE_CONFIGS[file_type]
    return (
        pd.DataFrame(
            dict(zip(
                configs_dict['header_lines'].keys(),
                get_file_headers(
                     file=file,
                     begin=min(configs_dict['header_lines'].values()),
                     end=max(configs_dict['header_lines'].values()),
                     sep=configs_dict['separator']
                     )
                ))
            )
        .set_index(keys='variable')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_info(file, file_type=None, dummy_override=False):
    """
    Get the information from the first line of the TOA5 file. If EddyPro file
    OR dummy_override, just grab the defaults from the configuration dictionary.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.
    dummy_override : bool, optional
        Whether to just retrieve the default info. The default is False.

    Returns
    -------
    dict
        File info with fields.

    """

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # If dummy override (or EddyPro, which has no file info) requested,
    # return dummy info
    if dummy_override or file_type == 'EddyPro':
        return dict(zip(INFO_FIELDS, FILE_CONFIGS[file_type]['dummy_info']))

    # Otherwise get the TOA5 file info from the file
    return dict(zip(
        INFO_FIELDS,
        get_file_headers(
            file=file,
            begin=FILE_CONFIGS['TOA5']['info_line'],
            end=FILE_CONFIGS['TOA5']['info_line'],
            sep=FILE_CONFIGS['TOA5']['separator']
            )[0]
        ))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_type(file):
    """
    Get the file type (TOA5 or EddyPro).

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.

    Raises
    ------
    TypeError
        Raised if file type not recognised.

    Returns
    -------
    file_type : str
        The file type.

    """

    for file_type in FILE_CONFIGS.keys():
        id_field = (
            get_file_headers(
                file=file,
                begin=0,
                end=0,
                sep=FILE_CONFIGS[file_type]['separator']
                )
            [0][0]
            ).strip('\"')
        if id_field == FILE_CONFIGS[file_type]['unique_file_id']:
            return file_type
    raise TypeError('Unknown file type!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_type_configs(file_type=None, return_field=None):
    """
    Get the configuration dictionary for the file type.

    Parameters
    ----------
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.

    Returns
    -------
    dict
        The type-specific configuration dictionary.

    """
    if not file_type:
        return FILE_CONFIGS
        if not return_field is None:
            raise RuntimeError(
                'Cannot return individual field if file type not set!'
                )
    if return_field is None:
        return FILE_CONFIGS[file_type]
    return FILE_CONFIGS[file_type][return_field]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_data_to_file(
        headers, data, abs_file_path, output_format=None, info=None
        ):
    """
    Write headers and data to file. Checks only for consistency between headers
    and data column names (no deeper analysis of consistency with output format).

    Parameters
    ----------
    headers : pd.core.frame.DataFrame
        The dataframe containing the headers as columns.
    data : pd.core.frame.DataFrame
        The dataframe containing the data.
    abs_file_path : str or pathlib.Path
        Absolute path (including file name to write to).
    output_format : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.
    info : dict, optional
        The file info. Only required if outputting TOA5, and retrieves
        file type-specific dummy input if not supplied. The default is None.

    Returns
    -------
    None.

    """

    # Cross-check header / data column consistency
    _check_data_header_consistency(
        headers=headers,
        data=data
        )

    # Add the requisite info to the output if TOA5
    row_list = []
    if not output_format:
        output_format = 'TOA5'
    if output_format == 'TOA5':
        if info is None:
            info = dict(zip(
                INFO_FIELDS,
                FILE_CONFIGS[output_format]['dummy_info']
                ))
        row_list.append(list(info.values()))

    # Construct the row list
    output_headers = headers.reset_index()
    [row_list.append(output_headers[col].tolist()) for col in output_headers]

    # Write the data to file
    file_configs = get_file_type_configs(file_type=output_format)
    with open(abs_file_path, 'w', newline='\n') as f:

        # Write the header
        writer = csv.writer(
            f,
            delimiter=file_configs['separator'],
            quoting=file_configs['quoting']
            )
        for row in row_list:
            writer.writerow(row)

        # Write the data
        data.to_csv(
            f, header=False, index=False, na_rep=file_configs['na_values'],
            sep=file_configs['separator'], quoting=file_configs['quoting']
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_data_header_consistency(headers, data):
    """
    Checks that the passed headers and data are consistent.

    Parameters
    ----------
    headers : pd.core.frame.DataFrame
        The dataframe containing the headers as columns.
    data : pd.core.frame.DataFrame
        The dataframe containing the data.

    Raises
    ------
    RuntimeError
        Raised if inconsistent.

    Returns
    -------
    None.

    """

    headers_list = headers.index.tolist()
    data_list = data.columns.tolist()
    if not headers_list == data_list:
        headers_not_in_data = list(set(headers_list) - set(data_list))
        data_not_in_headers = list(set(data_list) - set(headers_list))
        raise RuntimeError(
            'Header and data variables dont match: '
            f'Header variables not in data: {headers_not_in_data}, '
            f'Data variables not in header: {data_not_in_headers}, '
            )
#------------------------------------------------------------------------------



###############################################################################
### END FILE READ / WRITE FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN FILE FORMATTING FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def get_formatter(file_type, which):
    """


    Parameters
    ----------
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.
    which : str
        .

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    formatter_dict = {
        'TOA5': {
            'read_line': _TOA5_line_read_formatter,
            'write_line': _TOA5_line_write_formatter,
            'read_date': _TOA5_date_read_formatter,
            'write_date': _TOA5_date_write_formatter
            },
        'EddyPro': {
            'read_line': _EddyPro_line_read_formatter,
            'write_line': _EddyPro_line_write_formatter,
            'read_date': _EddyPro_date_read_formatter,
            'write_date': _EddyPro_date_write_formatter
            }
        }
    return formatter_dict[file_type][which]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_line_read_formatter(line):

    sep = FILE_CONFIGS['EddyPro']['separator']
    return line.strip().split(sep)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_line_write_formatter(line_list):

    joiner = lambda x: FILE_CONFIGS['EddyPro']['separator'].join(x) + '\n'
    return joiner(line_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_date_read_formatter(line_list):

    locs = FILE_CONFIGS['EddyPro']['time_variables'].values()
    return _generic_date_constructor(
        date_elems=[line_list[loc] for loc in locs]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_date_write_formatter(py_date, which):

    return {
        'date': py_date.strftime('%Y-%m-%d'),
        'time': py_date.strftime('%H:%M:%S')
        }[which]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_line_read_formatter(line):

    sep = FILE_CONFIGS['TOA5']['separator']
    return [x.replace('"', '') for x in line.strip().split(sep)]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_line_write_formatter(line_list):

    formatter = lambda x: f'"{x}"'
    joiner = lambda x: FILE_CONFIGS['TOA5']['separator'].join(x) + '\n'
    return joiner(formatter(x) for x in line_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_date_read_formatter(line_list):

    locs = FILE_CONFIGS['TOA5']['time_variables'].values()
    return _generic_date_constructor(
        date_elems=[line_list[loc] for loc in locs]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_date_write_formatter(py_date, which='TIMESTAMP'):

    return py_date.strftime('%Y-%m-%d %H:%M:%S')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _generic_date_constructor(date_elems):

    return dt.datetime.strptime(' '.join(date_elems), DATE_FORMAT)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reformat_data(data, output_format):
    """


    Parameters
    ----------
    data : pd.core.frame.DataFrame
        The data.
    output_format : str
        The output format (must be TOA5 or EddyPro).

    Raises
    ------
    TypeError
        Raised if the data does not have a datetime index.

    Returns
    -------
    pd.core.frame.DataFrame
        The modified data.

    """

    # Initialisation stuff
    if not isinstance(data.index, pd.core.indexes.datetimes.DatetimeIndex):
        raise TypeError('Passed data must have a DatetimeIndex!')
    _check_format(fmt=output_format)
    funcs_dict = {'TOA5': _TOA5ify_data, 'EddyPro': _EPify_data}
    df = data.copy()

    # Remove only the date variables unless requestes to remove all, and even
    # then only
    # remove_str = 'time_variables'
    # if remove_non_numeric:
    #     if output_format == 'TOA5':
    #         remove_str = 'non_numeric_cols'

    # Remove all format-specific data columns to make data format-agnostic.
    for fmt in FILE_CONFIGS.keys():
        for var in FILE_CONFIGS[fmt]['time_variables']:
            try:
                df.drop(var, axis=1, inplace=True)
            except KeyError:
                continue
    return funcs_dict[output_format](data=df)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5ify_data(data):
    """


    Parameters
    ----------
    data : TYPE
        DESCRIPTION.
    deindex : TYPE, optional
        DESCRIPTION. The default is True.

    Raises
    ------
    TypeError
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    # Create the date outputs
    formatter = get_formatter(file_type='TOA5', which='write_date')
    date_series = pd.Series(data.index.to_pydatetime(), index=data.index)
    data.insert(0, 'TIMESTAMP', date_series.apply(formatter))
    return data
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EPify_data(data):

    # Check whether DATAH and filename columns are present - if so, leave them
    # if not (e.g. if source file is TOA5), insert them!
    non_num_strings = {'DATAH': 'DATA', 'filename': 'none'}
    for i, var in enumerate(['DATAH', 'filename']):
        if not var in data.columns:
            data.insert(i, var, non_num_strings[var])

    # Create the date outputs and put them in slots 2 and 3
    formatter = get_formatter(file_type='EddyPro', which='write_date')
    date_series = pd.Series(data.index.to_pydatetime(), index=data.index)
    i = 2
    for var in FILE_CONFIGS['EddyPro']['time_variables'].keys():
        series = date_series.apply(formatter, which=var)
        data.insert(i, var, series)
        i += 1

    return data
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reformat_headers(headers, output_format):
    """
    Create formatted header from dataframe header.

    Parameters
    ----------
    headers : pd.core.frame.DataFrame
        Formatted headers returned by get_header_df call.
    output_format : str
        The output format (must be TOA5 or EddyPro).

    Returns
    -------
    pd.core.frame.DataFrame
        Headers reformatted to requested format.

    """

    # Initialisation stuff
    _check_format(fmt=output_format)
    funcs_dict = {'TOA5': _TOA5ify_headers, 'EddyPro': _EPify_headers}
    df = headers.copy()

    # Remove all format-specific header columns to make header format-agnostic.
    for fmt in FILE_CONFIGS.keys():
        for var in FILE_CONFIGS[fmt]['non_numeric_cols']:
            try:
                df.drop(var, inplace=True)
            except KeyError:
                continue

    # Pass to function to format header as appropriate
    return funcs_dict[output_format](headers=df)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EPify_headers(headers):
    """


    Parameters
    ----------
    headers : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    # Create EddyPro-specific headers
    add_df = pd.DataFrame(
        data={'units': ['DATAU', '', '[yyyy-mm-dd]', '[HH:MM]']},
        index=pd.Index(['DATAH', 'filename', 'date', 'time'], name='variable'),
        )

    # Drop the sampling header line if it exists
    if 'sampling' in headers.columns:
        headers.drop('sampling', axis=1, inplace=True)

    # Concatenate and return
    return pd.concat([add_df, headers])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5ify_headers(headers):
    """


    Parameters
    ----------
    headers : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    # Create TOA5-specific headers
    add_df = pd.DataFrame(
        data={'units': 'TS', 'sampling': ''},
        index=pd.Index(['TIMESTAMP'], name='variable'),
        )

    # Add the sampling header line if it doesn't exist
    if not 'sampling' in headers.columns:
        headers = headers.assign(sampling='')
    headers.sampling.fillna('', inplace=True)

    # Concatenate and return
    return pd.concat([add_df, headers])
#------------------------------------------------------------------------------

###############################################################################
### END FILE FORMATTING FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN DATE HANDLING FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def get_dates(file, file_type=None):
    """
    Date parser (faster than line by line code).

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.
    Returns
    -------
    date_list : list
        The dates.

    """

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # Get the time variables and rows to skip
    time_vars = list(FILE_CONFIGS[file_type]['time_variables'].keys())
    rows_to_skip = list(set(
        [0] +
        list(FILE_CONFIGS[file_type]['header_lines'].values())
        ))
    rows_to_skip.remove(FILE_CONFIGS[file_type]['header_lines']['variable'])
    separator = FILE_CONFIGS[file_type]['separator']

    # Get the data
    df = pd.read_csv(
        file,
        usecols=time_vars,
        skiprows=rows_to_skip,
        parse_dates={'DATETIME': time_vars},
        sep=separator,
        )

    # Check the date parser worked - if not, fix it
    if df.DATETIME.dtype == 'object':
        df.DATETIME = pd.to_datetime(df.DATETIME, errors='coerce')
        df.dropna(inplace=True)

    # Return the dates
    return pd.DatetimeIndex(df.DATETIME).to_pydatetime()
    # return df.DATETIME.dt.to_pydatetime()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_start_end_dates(file, file_type=None):
    """
    Get start and end dates only.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.

    Returns
    -------
    dict
        Containing key:value pairs of start and end dates with "start_date"
        and "end_date" as keys.

    """

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # Get the formatters
    line_formatter = get_formatter(file_type=file_type, which='read_line')
    date_formatter = get_formatter(file_type=file_type, which='read_date')

    # Open file in binary
    with open(file, 'rb') as f:

        # Iterate forward to find first valid start date
        start_date = None
        for line in f:
            try:
                start_date = date_formatter(line_formatter(line.decode()))
                break
            except ValueError:
                continue

        # Iterate backwards to find last valid end date
        end_date = None
        f.seek(2, os.SEEK_END)
        while True:
            try:
                if f.read(1) == b'\n':
                    pos = f.tell()
                    try:
                        end_date = (
                            date_formatter(
                                line_formatter(f.readline().decode())
                                )
                            )
                        break
                    except ValueError:
                        f.seek(pos - f.tell(), os.SEEK_CUR)
                f.seek(-2, os.SEEK_CUR)
            except OSError:
                break

    return {'start_date': start_date, 'end_date': end_date}
#------------------------------------------------------------------------------



###############################################################################
### END DATE HANDLING FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN CONCATENATION FILE RETRIEVAL FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def get_eligible_concat_files(file, file_type=None):
    """
    Get the list of files that can be concatenated, based on file and file type.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.

    Returns
    -------
    list
        The list of files that can be concatenated.

    """

    funcs_dict = {
        'TOA5': get_TOA5_backups,
        'EddyPro': get_EddyPro_files
        }

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # Return the files
    return funcs_dict[file_type](file=file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOA5_backups(file):
    """
    Get the list of TOA5 backup files that can be concatenated.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.

    Returns
    -------
    list
        The list of files that can be concatenated.

    """

    file_to_parse = _check_file_exists(file=file)
    return list(file_to_parse.parent.glob(f'{file_to_parse.stem}*.backup'))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_EddyPro_files(file):
    """
    Get the list of EddyPro summary files that can be concatenated.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.

    Raises
    ------
    RuntimeError
        Raised if the passed file is older than any of the concatenation files.

    Returns
    -------
    file_list : list
        The list of files that can be concatenated.

    """

    file_to_parse = _check_file_exists(file=file)
    file_list = list(file_to_parse.parent.glob(f'*{EDDYPRO_SEARCH_STR}.txt'))
    file_list.sort()
    if file in file_list:
        file_list.remove(file_to_parse)
    return file_list
#------------------------------------------------------------------------------



###############################################################################
### END CONCATENATION FILE RETRIEVAL FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN FILE INTERVAL FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def get_file_interval(file, file_type=None):
    """
    Find the file interval (i.e. time step)

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str, optional
        The type of file (must be either "TOA5" or "EddyPro").
        The default is None.

    Raises
    ------
    RuntimeError
        Raised if the most common value is not the minimum value.

    Returns
    -------
    minimum_val : int
        The interval.

    """

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)
    return get_datearray_interval(
        datearray=np.unique(np.array(get_dates(file=file, file_type=file_type)))
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_datearray_interval(datearray):
    """


    Parameters
    ----------
    datearray : TYPE
        DESCRIPTION.

    Raises
    ------
    RuntimeError
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    if len(datearray) == 1:
        return None
    datearray = np.unique(datearray)
    deltas, counts = np.unique(datearray[1:] - datearray[:-1], return_counts=True)
    minimum_val = deltas[0].seconds / 60
    common_val = (deltas[np.where(counts==counts.max())][0]).seconds / 60
    if minimum_val == common_val:
        return int(minimum_val)
    raise RuntimeError('Minimum and most common values do not coincide!')
#------------------------------------------------------------------------------



###############################################################################
### END FILE INTERVAL FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN FILE CHECKING FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def _check_file_exists(file):

    file_to_parse = pathlib.Path(file)
    if not file_to_parse.exists():
        raise FileNotFoundError('Passed file does not exist!')
    return file_to_parse
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_format(fmt):

    if not fmt in FILE_CONFIGS.keys():
        raise NotImplementedError(f'Format {fmt} is not implemented!')
#------------------------------------------------------------------------------

###############################################################################
### END FILE CHECKING FUNCTIONS ###
###############################################################################
