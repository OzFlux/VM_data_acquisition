# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 13:37:33 2023

@author: jcutern-imchugh
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



###############################################################################
### FUNCTIONS ###
###############################################################################



#----------------#
# File functions #
#----------------#



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
    df.sort_index(inplace=True)

    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_headers(file, begin, end):
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

    return_list = []
    with open(file, 'r') as f:
        for i in range(end + 1):
            line = f.readline()
            if not i < begin:
                return_list.append(line)
    return return_list
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

    HEADERS_DICT = FILE_CONFIGS[file_type]['header_lines']
    formatter = get_formatter(file_type=file_type, which='read')
    return (
        pd.DataFrame(
            dict(zip(
                HEADERS_DICT.keys(),
                [formatter(line) for line in
                 get_file_headers(
                     file=file,
                     begin=min(HEADERS_DICT.values()),
                     end=max(HEADERS_DICT.values())
                     )
                 ]
                ))
            )
        .set_index(keys='variable')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_info(file, file_type=None, dummy_override=False):

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # If dummy override (or EddyPro, which has no file info) requested,
    # return dummy info
    if dummy_override or file_type == 'EddyPro':
        return dict(zip(INFO_FIELDS, FILE_CONFIGS[file_type]['dummy_info']))

    # Otherwise get the TOA5 file info from the file
    line_num = FILE_CONFIGS['TOA5']['info_line']
    formatter = get_formatter(file_type='TOA5', which='read')
    info = formatter(
        get_file_headers(file=file, begin=line_num, end=line_num)[0]
            )
    return dict(zip(INFO_FIELDS, info))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_type(file):

    header = get_file_headers(file=file, begin=0, end=0)[0]
    for file_type in FILE_CONFIGS.keys():
        formatter = get_formatter(file_type=file_type, which='read')
        uniq_id = FILE_CONFIGS[file_type]['unique_file_id']
        if formatter(header)[0] == uniq_id:
            return file_type
    raise TypeError('Unknown file type!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_type_configs(file_type):

    return FILE_CONFIGS[file_type]
#------------------------------------------------------------------------------



#------------#
# Formatting #
#------------#



#------------------------------------------------------------------------------
def get_formatter(file_type, which):

    formatter_dict = {
        'TOA5': {
            'read': _TOA5_read_formatter,
            'write': _TOA5_write_formatter,
            'date': _TOA5_date_formatter
            },
        'EddyPro': {
            'read': _EddyPro_read_formatter,
            'write': _EddyPro_write_formatter,
            'date': _EddyPro_date_formatter
            }
        }
    return formatter_dict[file_type][which]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_read_formatter(line):

    sep = FILE_CONFIGS['EddyPro']['separator']
    return line.strip().split(sep)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_write_formatter(line_list):

    joiner = lambda x: FILE_CONFIGS['EddyPro']['separator'].join(x) + '\n'
    return joiner(line_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_date_formatter(line_list):

    locs = FILE_CONFIGS['EddyPro']['time_variables'].values()
    return _generic_date_constructor(
        date_elems=[line_list[loc] for loc in locs]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_read_formatter(line):

    sep = FILE_CONFIGS['TOA5']['separator']
    return [x.replace('"', '') for x in line.strip().split(sep)]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_write_formatter(line_list):

    formatter = lambda x: f'"{x}"'
    joiner = lambda x: FILE_CONFIGS['TOA5']['separator'].join(x) + '\n'
    return joiner(formatter(x) for x in line_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_date_formatter(line_list):

    locs = FILE_CONFIGS['TOA5']['time_variables'].values()
    return _generic_date_constructor(
        date_elems=[line_list[loc] for loc in locs]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _generic_date_constructor(date_elems):

    return dt.datetime.strptime(' '.join(date_elems), DATE_FORMAT)
#------------------------------------------------------------------------------



#---------------#
# Date handling #
#---------------#



#------------------------------------------------------------------------------
def get_dates(file, file_type=None):
    """
    Line-by-line date parser.

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

    # Get the formatters
    line_formatter = get_formatter(file_type=file_type, which='read')
    date_formatter = get_formatter(file_type=file_type, which='date')
    date_list = []
    with open(file, 'r') as f:
        for line in f:
            try:
                date_list.append(date_formatter(line_formatter(line)))
            except (TypeError, ValueError):
                continue
    return date_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_start_end_dates(file, file_type=None):

    # If file type not supplied, detect it.
    if not file_type:
        file_type = get_file_type(file)

    # Get the formatters
    line_formatter = get_formatter(file_type=file_type, which='read')
    date_formatter = get_formatter(file_type=file_type, which='date')

    # Open file in binary
    with open(file, 'rb') as f:

        # Iterate forward to find first valid start date
        start_date = None
        for i, line in enumerate(f):
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

# #------------------------------------------------------------------------------
# def get_record_from_date(file, pydt, from_end=False):

#     with open(file, 'rb') as file_obj:
#         return _file_date_backwards_parser(file_obj, pydt, file_type='TOA5')

#     pass
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def _file_date_backwards_parser(file_obj, pydt, file_type):

#     # Get the formatters
#     line_formatter = get_formatter(file_type=file_type, which='read')
#     date_formatter = get_formatter(file_type=file_type, which='write')

#     file_obj.seek(2, os.SEEK_END)
#     while True:
#         try:
#             if file_obj.read(1) == b'\n':
#                 pos = file_obj.tell()
#                 try:
#                     line = line_formatter(file_obj.readline().decode())
#                     date = date_formatter(line)
#                     if date == pydt:
#                         return(file_obj.read().decode())
#                 except ValueError:
#                     pass
#                 file_obj.seek(pos - file_obj.tell(), os.SEEK_CUR)
#             file_obj.seek(-2, os.SEEK_CUR)
#         except OSError:
#             break

#     pass
# #------------------------------------------------------------------------------



#------------------------------#
# Concatenation file retrieval #
#------------------------------#



#------------------------------------------------------------------------------
def get_eligible_concat_files(file, file_type=None):

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

    file_to_parse = _check_file_exists(file=file)
    return list(file_to_parse.parent.glob(f'{file_to_parse.stem}*.backup'))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_EddyPro_files(file):

    file_to_parse = _check_file_exists(file=file)
    search_str = '_'.join(file_to_parse.stem.split('_')[1:])
    file_list = list(file_to_parse.parent.glob(f'*{search_str}.txt'))
    file_list.sort()
    if not file_list[-1] == file_to_parse:
        raise RuntimeError('Master concatenation file should be the most recent!')
    if file_list:
        file_list.remove(file_to_parse)
    return file_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_file_exists(file):

    file_to_parse = pathlib.Path(file)
    if not file_to_parse.exists():
        raise FileNotFoundError('Passed file does not exist!')
    return file_to_parse
#------------------------------------------------------------------------------



#---------------#
# File interval #
#---------------#



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

    datearray = np.unique(datearray)
    deltas, counts = np.unique(datearray[1:] - datearray[:-1], return_counts=True)
    minimum_val = deltas[0].seconds / 60
    common_val = (deltas[np.where(counts==counts.max())][0]).seconds / 60
    if minimum_val == common_val:
        return int(minimum_val)
    raise RuntimeError('Minimum and most common values do not coincide!')
#------------------------------------------------------------------------------



# #---------------#
# # Miscellaneous #
# #---------------#



# #------------------------------------------------------------------------------
# def _write_data_to_file(header_lines, data, file_type, abs_file_path):

#     # Get required formatting
#     sep = FILE_CONFIGS[file_type]['separator']
#     na = FILE_CONFIGS[file_type]['na_values']
#     quoting = FILE_CONFIGS[file_type]['quoting']

#     # Write to file
#     with open(abs_file_path, 'w', newline='\n') as f:
#         for line in header_lines:
#             f.write(line)
#         data.to_csv(
#             f, header=False, index=False, na_rep=na,
#             quoting=quoting, sep=sep
#             )
# #------------------------------------------------------------------------------
