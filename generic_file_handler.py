# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 13:37:33 2023

@author: jcutern-imchugh
"""

import datetime as dt

import numpy as np
import pandas as pd

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

FILE_CONFIGS = {
    'TOA5': {
        'info_line': 0,
        'header_lines': {'variable': 1, 'units': 2, 'sampling': 3},
        'separator': ',',
        'non_numeric_cols': [],
        'time_variables': {'TIMESTAMP': 0},
        'na_values': 'NAN'
        },
    'SF_summary': {
        'info_line': None,
        'header_lines': {'variable': 0, 'units': 1},
        'separator': '\t',
        'non_numeric_cols': ['DATAH', 'filename'],
        'time_variables': {'date': 2, 'time': 3},
        'na_values': 'NaN'
        }
    }

UNIT_ALIASES = {
    'degC': ['C'],
    'n': ['arb', 'samples'],
    'arb': ['n', 'samples'],
    'samples': ['arb', 'n']
    }

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class merge_report_handler():

    def __init__(self, merge_results):

        self.master_file = merge_results['master_file']
        self.merge_file = merge_results['merge_file']
        self.variable_merge_legal = merge_results['variable_merge_legal']
        self.units_merge_legal = merge_results['units_merge_legal']
        self.interval_merge_legal = merge_results['interval_merge_legal']
        self.file_merge_legal = all(
            [merge_results[key] for key in merge_results.keys() if 'legal' in key]
            )

        common_variables = merge_results['common_variables']
        if common_variables:
            self.common_variables = common_variables
        else:
            self.common_variables = None

        master_variables = merge_results['master_only']
        if master_variables:
            self.master_variables = master_variables
        else:
            self.master_variables = None

        merge_variables = merge_results['merge_only']
        if merge_variables:
            self.merge_variables = master_variables
        else:
            self.merge_variables = None

    def get_report_text(self):

        write_list = []
        write_list.append(f'Master file: {self.master_file}')
        write_list.append(f'Merge file: {self.merge_file}')
        return write_list

    def write_report(self):

        pass



#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class generic_data_ingester():

    #--------------------------------------------------------------------------
    def __init__(self, file, file_type):

        self.file = file
        self.file_type = file_type
        self.data = get_data(file=file, file_type=file_type)
        self.interval_as_num = get_index_interval(idx=self.data.index)
        self.interval_as_offset = f'{self.interval_as_num}T'
        self.headers = get_header_df(file=file, file_type=file_type)

    #--------------------------------------------------------------------------
    def get_conditioned_data(self,
            usecols=None, monotonic_index=False, drop_duplicates=False,
            raise_if_dupe_index=False, resample_intvl=None
            ):
        """
        Generate an altered version of the data.

        Parameters
        ----------
        usecols : list or dict, optional
            The columns to include in the output. If a dict is passed, then
            the columns are renamed, with the mapping from existing to new
            defined by the key (old name): value (new_name) pairs.
            The default is None.
        monotonic_index : bool, optional
            Align the data to a monotonic index. The default is False.
        drop_duplicates : bool, optional
            Remove any duplicate records or indices. The default is False.
        raise_if_dupe_index : bool, optional
            Raise an error if duplicate indices are found with non-duplicate
            data. The default is False.
        resample_intvl : date_offset, optional
            The time interval to which the data should be resampled.
            The default is None.

        Raises
        ------
        RuntimeError
            Raised if duplicate indices are found with non-duplicate data.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with altered data (returns the existing data if all
                                         options are default).

        """

        # Initialise the column subset and dictionaries (for renaming variables)
        # if passed, or set to existing
        rename_dict = {}
        thecols = self.data.columns
        if usecols:
            if isinstance(usecols, dict):
                rename_dict = usecols.copy()
                thecols = list(rename_dict.keys())
            elif isinstance(usecols, list):
                thecols = usecols.copy()

        # Initialise the new datetime index (or set to existing); note that
        # reindexing is required if resampling is requested, so user choice to
        # NOT have a monotonic index must be overriden in this case.
        must_reindex = monotonic_index or resample_intvl
        new_index = self.data.index
        if must_reindex:
            dates = self.get_date_span()
            new_index = pd.date_range(
                start=dates['start_date'],
                end=dates['end_date'],
                freq=self.interval_as_offset
                )
            new_index.name = self.data.index.name

        # Check for duplicate indices (ie. where records aren't duplicated)
        dupe_indices = self.get_duplicate_indices()
        if any(dupe_indices) and raise_if_dupe_index:
            raise RuntimeError(
                'Duplicate indices with non-duplicate data!'
                )

        # Initialise the duplicate_mask - note that a monotonic index by def
        # requires the duplicate index labels to be dropped, so user choice
        # to NOT drop duplicates must be overriden if monotonic index
        # is required.
        dupe_records = self.get_duplicate_records()
        dupes_mask = pd.Series(data=False, index=self.data.index)
        if drop_duplicates or must_reindex:
            dupes_mask = dupe_records | dupe_indices

        # Set the resampling interval if not set
        if not resample_intvl:
            resample_intvl = self.interval_as_offset

        # Apply duplicate mask, column subset, new index, new column names
        # and resampling to existing data (which remains unchanged), and return
        return (
            self.data.loc[~dupes_mask, thecols]
            .reindex(new_index)
            .resample(resample_intvl).interpolate(limit=1)
            .rename(rename_dict, axis=1)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_records(self, as_dates=False):
        """
        Get a representation of duplicate records (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate records occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.data.duplicated()
        if as_dates:
            return self.data[records].index.to_pydatetime().tolist()
        return records
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_indices(self, as_dates=False):
        """
        Get a representation of duplicate indices (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate indices occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.get_duplicate_records()
        indices = ~records & self.data.index.duplicated()
        if as_dates:
            return self.data[indices].index.to_pydatetime().tolist()
        return indices
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_gap_distribution(self):
        """
        List of gap lengths and their frequencies (counts).

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with number of records as index and count as column.

        """

        dupes = self.get_duplicate_indices() | self.get_duplicate_records()
        local_data = self.data[~dupes]
        gap_series = (
            (
                local_data.reset_index()['TIMESTAMP'] -
                local_data.reset_index()['TIMESTAMP'].shift()
                )
            .astype('timedelta64[m]')
            .replace(self.interval_as_num, np.nan)
            .dropna()
            )
        gap_series /= self.interval_as_num
        unique_gaps = gap_series.unique().astype(int)
        counts = [len(gap_series[gap_series==x]) for x in unique_gaps]
        return (
            pd.DataFrame(
                data=zip(unique_gaps - 1, counts),
                columns=['n_records', 'count']
                )
            .set_index(keys='n_records')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_date_span(self):
        """
        Get start and end dates

        Returns
        -------
        dict
            Dictionary containing start and end dates (keys "start" and "end").

        """

        return {
            'start_date': self.data.index[0].to_pydatetime(),
            'end_date': self.data.index[-1].to_pydatetime()
            }
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

        return self.headers.index.tolist()
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

        return self.headers.loc[variable, 'units']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_variable_units(self):
        """
        Get a dictionary cross-referencing all variables in file to their units

        Returns
        -------
        dict
            With variables (keys) and units (values).

        """

        return dict(zip(
            self.headers.index.tolist(),
            self.headers.units.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_sampling_units(self):

        if self.file_type == 'SF_summary':
            raise NotImplementedError(
                f'No station info available for file type "{self.file_type}"')
        return dict(zip(
            self.headers.index.tolist(),
            self.headers.sampling.tolist()
            ))
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_concatenator():

    def __init__(self, master_file, file_type, concat_list):

        self.master_file = master_file
        self.master_interval = get_file_interval(
            file=master_file, file_type=file_type
            )
        temp_list = concat_list.copy()
        if master_file in temp_list:
            temp_list.remove(master_file)
        self.concat_list = temp_list
        self.file_type = file_type


    def compare_variables(self, with_file):

        master_df = get_header_df(
            file=self.master_file, file_type=self.file_type
            )
        merge_df = get_header_df(
            file=with_file, file_type=self.file_type
            )
        common = list(set(master_df.index).intersection(merge_df.index))
        return {
            'common_variables': common,
            'variable_merge_legal': len(common) > 0,
            'master_only': list(
                set(master_df.index.tolist()) - set(merge_df.index.tolist())
                ),
            'merge_only': list(
                set(merge_df.index.tolist()) - set(master_df.index.tolist())
                )
            }

    #--------------------------------------------------------------------------
    def compare_units(self, with_file):
        """
        Compare units header line (merge is deemed illegal if there are unit
                                   changes).

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        dict
            return a dictionary with the mismatched header elements
            (key='units_mismatch') and whether the merge is legal
            (key='units_merge_legal').

        """

        # Subset the analysis for common variables, and build comparison df
        common_vars = (
            self.compare_variables(with_file=with_file)['common_variables']
            )
        compare_df = pd.concat(
            [(get_header_df(file=self.master_file, file_type=self.file_type)
              .rename({'units': 'master'}, axis=1)
              .loc[common_vars, 'master']
              ),
              (get_header_df(file=with_file, file_type=self.file_type)
              .rename({'units': 'merge'}, axis=1)
              .loc[common_vars, 'merge']
              )], axis=1
            )

        # Get mismatched variables and check whether they are just aliases
        # (in which case merge is deemed legal)
        mismatch_df = compare_df[compare_df['master']!=compare_df['merge']]
        mismatch_list, units_merge_legal = [], True
        for variable in mismatch_df.index:
            master_units = mismatch_df.loc[variable, 'master']
            merge_units = mismatch_df.loc[variable, 'merge']
            try:
                assert merge_units in UNIT_ALIASES[master_units]
            except (KeyError, AssertionError):
                mismatch_list.append(variable)
                units_merge_legal = False

        # Return the result
        return {
            'units_mismatch': mismatch_list,
            'units_merge_legal': units_merge_legal
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_interval(self, with_file):
        """
        Cross-check concatenation file has same frequency as master.

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        dict
            Dictionary with indication of whether merge intervals are same
            (legal) or otherwise (illegal).

        """

        return {
            'interval_merge_legal':
                self.master_interval ==
                get_file_interval(file=with_file, file_type=self.file_type)
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def do_merge_assessment(self, with_file, as_text=False):
        """
        Do full assessment of merge

        Parameters
        ----------
        with_file : str
            Name of file to compare to the master file (filename only,
                                                        omit directory).
        as_text : bool, optional
            If true, returns a formatted text output. The default is False.

        Returns
        -------
        dict or list
            Dictionary containing results of merge assessment, or list of
            formatted strings if requested.

        """

        return merge_report_handler(
            {'master_file': str(self.master_file), 'merge_file': str(with_file)} |
            self.compare_variables(with_file=with_file) |
            self.compare_units(with_file=with_file) |
            self.compare_interval(with_file=with_file)
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#---------------#
# File handling #
#---------------#

#------------------------------------------------------------------------------
def concatenate_files(file_list, file_type):

    return pd.concat(
        [get_data(file=file, file_type=file_type) for file in file_list]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(file, file_type, usecols=None):
    """
    Read the data from the TOA5 file

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "SF_summary")
    usecols : list, optional
        The subset of columns to keep. The default is None (all columns retained).

    Returns
    -------
    df : pd.core.frame.DataFrame
        Data.

    """

    # Get dictionary containing file configurations, and create requisite
    # config variables
    MASTER_DICT = FILE_CONFIGS[file_type]
    REQ_TIME_VARS = list(MASTER_DICT['time_variables'].keys())
    rows_to_skip = [
        i for i in set([0] + list(MASTER_DICT['header_lines'].values()))
        ]
    rows_to_skip.remove(MASTER_DICT['header_lines']['variable'])
    index_col = '_'.join(REQ_TIME_VARS)
    timestamp_cols = REQ_TIME_VARS.copy()
    if len(timestamp_cols) > 1:
        timestamp_cols = [timestamp_cols]

    # Usecols MUST include time variables and at least ONE additional column;
    # if this condition is not satisifed, do not subset the columns on import.
    thecols = None
    if usecols and not usecols == REQ_TIME_VARS:
        thecols = list(set(REQ_TIME_VARS + usecols))

    # Now import data
    return (
        pd.read_csv(
            file,
            skiprows=rows_to_skip,
            usecols=thecols,
            parse_dates=timestamp_cols,
            na_values=MASTER_DICT['na_values'],
            sep=MASTER_DICT['separator'],
            engine='c',
            on_bad_lines='warn',
            low_memory=False
            )
        .rename({index_col: 'TIMESTAMP'}, axis=1)
        .set_index(keys='TIMESTAMP')
        .pipe(_integrity_checks, non_numeric=MASTER_DICT['non_numeric_cols'])
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _integrity_checks(df, non_numeric):

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
    with open(file) as f:
        for i in range(end + 1):
            line = f.readline()
            if not i < begin:
                return_list.append(line)
    return return_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_header_df(file, file_type, usecols=None):
    """
    Get a dataframe with variables as index and units and statistical
    sampling type as columns.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "SF_summary")

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe as per above.

    """

    HEADERS_DICT = FILE_CONFIGS[file_type]['header_lines']
    formatter = get_line_formatter(file_type=file_type)
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

#-----------------#
# Line formatting #
#-----------------#

#------------------------------------------------------------------------------
def get_line_formatter(file_type):
    """
    Get the function to format the file lines

    Parameters
    ----------
    file_type : str
        The type of file for which to return the formatter.

    Returns
    -------
    function
        Formatting function.

    """

    if file_type == 'TOA5':
        return _TOA5_line_formatter
    elif file_type == 'SF_summary':
        return _SmartFlux_line_formatter
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _SmartFlux_line_formatter(line):

    sep = FILE_CONFIGS['SF_summary']['separator']
    return line.strip().split(sep)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _TOA5_line_formatter(line):

    sep = FILE_CONFIGS['TOA5']['separator']
    return [x.replace('"', '') for x in line.strip().split(sep)]
#------------------------------------------------------------------------------

#---------------#
# Date handling #
#---------------#

#------------------------------------------------------------------------------
def get_dates(file, file_type):

    line_formatter = get_line_formatter(file_type=file_type)
    last_line = max(FILE_CONFIGS[file_type]['header_lines'].values())
    with open(file) as f:
        for i, line in enumerate(f):
            if i > last_line:
                try:
                    date_list = [
                        _date_extractor(line_formatter(line), file_type=file_type)
                        for line in f
                        ]
                except (TypeError, ValueError):
                    continue
    return date_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _date_extractor(fmt_line, file_type):

    locs = FILE_CONFIGS[file_type]['time_variables'].values()
    return dt.datetime.strptime(
        ' '.join([fmt_line[loc] for loc in locs]),
        '%Y-%m-%d %H:%M:%S'
        )
#------------------------------------------------------------------------------

#---------------#
# Miscellaneous #
#---------------#

#------------------------------------------------------------------------------
def get_file_interval(file, file_type):

    dates = np.unique(np.array(get_dates(file=file, file_type=file_type)))
    deltas, counts = np.unique(dates[1:] - dates[:-1], return_counts=True)
    minimum_val = deltas[0].seconds / 60
    common_val = (deltas[np.where(counts==counts.max())][0]).seconds / 60
    if minimum_val == common_val:
        return minimum_val
    raise RuntimeError('Minimum and most common values do not coincide!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_index_interval(idx):

    diffs = (idx.drop_duplicates()[1:] - idx.drop_duplicates()[:-1]).components
    diff_mins = diffs.days * 1440 + diffs.hours * 60 + diffs.minutes
    minimum_val = diff_mins.min()
    common_val = diff_mins.value_counts().idxmax()
    if minimum_val == common_val:
        return common_val
#------------------------------------------------------------------------------