# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 13:37:33 2023

@author: jcutern-imchugh
"""

import pandas as pd

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

FILE_TYPES = ['TOA5', 'SmartFlux_summary']

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class generic_data_ingester():

    #--------------------------------------------------------------------------
    def __init__(self, file, file_type, usecols=None):

        retrieval_dict = {
            'TOA5': get_Campbell_TOA5_data,
            'SmartFlux_summary': get_SmartFlux_summary_data
            }
        self.file = file
        self.file_type = file_type
        self.usecols = usecols
        self.data = retrieval_dict[file_type](file=file, usecols=usecols)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_Campbell_TOA5_data(file, usecols=None):
    """
    Read the data from the TOA5 file

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    usecols : list, optional
        The subset of columns to keep. The default is None (all columns retained).

    Returns
    -------
    df : pd.core.frame.DataFrame
        Data.

    """

    # Specify columns to ignore when coercing non-numeric data, and specify
    # requisite time variables
    NON_NUMERIC_COLS = []
    REQ_TIME_VARS = ['TIMESTAMP']

    # Usecols MUST include TIMESTAMP and at least ONE additional column; if not,
    # do not subset the columns on import.
    thecols = None
    if usecols and not usecols == REQ_TIME_VARS:
        thecols = list(set(REQ_TIME_VARS + usecols))

    # Import data
    return (
        pd.read_csv(
            file, skiprows=[0,2,3], usecols=thecols, parse_dates=['TIMESTAMP'],
            index_col=['TIMESTAMP'], na_values='NAN', sep=',', engine='c',
            on_bad_lines='warn', low_memory=False
            )
        .pipe(_integrity_checks, exclude_cols=NON_NUMERIC_COLS)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_SmartFlux_summary_data(file, usecols=None):
    """
    Read the data from the SmartFlux summary file

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    usecols : list, optional
        The subset of columns to keep. The default is None (all columns retained).

    Returns
    -------
    df : pd.core.frame.DataFrame
        Data.

    """

    # Specify columns to ignore when coercing non-numeric data, and specify
    # requisite time variables
    NON_NUMERIC_COLS = ['DATAH', 'filename']
    REQ_TIME_VARS = ['date', 'time']

    # Usecols MUST include date, time and at least ONE additional column; if not,
    # do not subset the columns on import.
    thecols = None
    if usecols and not usecols == REQ_TIME_VARS:
        thecols = list(set(REQ_TIME_VARS + usecols))

    # Import data
    return (
        pd.read_csv(
            file, skiprows=[1], usecols=thecols, parse_dates=[['date', 'time']],
            na_values='NaN', sep='\t', engine='c', on_bad_lines='warn',
            low_memory=False
            )
        .rename({'date_time': 'TIMESTAMP'}, axis=1)
        .set_index(keys='TIMESTAMP')
        .pipe(_integrity_checks, exclude_cols=NON_NUMERIC_COLS)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _integrity_checks(df, exclude_cols):

    # Coerce non-numeric data in numeric columns
    non_nums = df.select_dtypes(include='object')
    for col in non_nums.columns:
        if col in exclude_cols: continue
        df[col] = pd.to_numeric(non_nums[col], errors='coerce')

    # Check the index type, and if bad time data exists, dump the record
    if not df.index.dtype == '<M8[ns]':
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~pd.isnull(df.index)]

    # Sort the index
    df.sort_index(inplace=True)

    return df
#------------------------------------------------------------------------------