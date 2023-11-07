# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 13:37:33 2023

@author: jcutern-imchugh
"""

import datetime as dt

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
        'na_values': 'NAN'
        },
    'EddyPro': {
        'info_line': None,
        'header_lines': {'variable': 0, 'units': 1},
        'separator': '\t',
        'non_numeric_cols': ['date', 'time',  'DATAH', 'filename'],
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



###############################################################################
### CLASSES ###
###############################################################################



#------------------------------------------------------------------------------
class GenericDataHandler():

    #--------------------------------------------------------------------------
    def __init__(self, file, file_type, concat_list=None):
        """
        Set attributes of handler.

        Parameters
        ----------
        file : str or pathlib.Path
            Absolute path to file for which to create the handler.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro")
        concat_list : list, optional
            The list of additional files concatenated with the main file.
            The default is None.

        Returns
        -------
        None.

        """

        data, headers, concat_list, concat_report = _get_handler_elements(
            file=file, file_type=file_type
            )
        self.file = file
        self.file_type = file_type
        self.data = data
        self.headers = headers
        self.interval_as_num = get_datearray_interval(
            datearray=np.array(data.index.to_pydatetime())
            )
        self.interval_as_offset = f'{self.interval_as_num}T'
        self.concat_list = concat_list
        self.concat_report = concat_report

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
        to_drop = list(FILE_CONFIGS[self.file_type]['time_variables'].keys())
        local_data = (
            self.data[~dupes]
            .drop(to_drop, axis=1)
            .reset_index()
            ['TIMESTAMP']
            )
        gap_series = (
            (local_data - local_data.shift())
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
            .sort_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_records(self):
        """
        Get simple statistics for missing records

        Returns
        -------
        dict
            Dictionary containing the number of missing cases, the % of missing
            cases and the distribution of gap sizes.

        """

        dupes = self.get_duplicate_indices() | self.get_duplicate_records()
        local_data = self.data[~dupes]
        complete_index = pd.date_range(
            start=local_data.index[0],
            end=local_data.index[-1],
            freq=self.interval_as_offset
            )
        n_missing = len(complete_index) - len(local_data)
        return {
            'n_missing': n_missing,
            '%_missing': round(n_missing / len(complete_index) * 100, 2),
            'gap_distribution': self._get_gap_distribution()
            }
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
        """
        Get a dictionary cross-referencing all variables in file to their
        sampling methods

        Returns
        -------
        dict
            With variables (keys) and sampling (values).

        """

        if self.file_type == 'EddyPro':
            raise NotImplementedError(
                f'No station info available for file type "{self.file_type}"')
        return dict(zip(
            self.headers.index.tolist(),
            self.headers.sampling.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_concatenation_report(self, abs_file_path):

        if not self.concat_report:
            raise TypeError(
                'Cannot write a concatenation report if there are no '
                'concatenated files!'
                )
        _write_text_to_file(
            line_list=self.concat_report,
            abs_file_path=abs_file_path
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Merging / concatenation classes #
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileConcatenator():
    """Class to allow multiple file merges to a master file"""

    def __init__(self, master_file, concat_list, file_type):
        """
        Get merge reports as hidden attributes.

        Parameters
        ----------
        master_file : str or pathlib.Path
            Absolute path to master file.
        concat_list : list
            List of absolute path to files to be concatenated with the master
            file.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro")

        Returns
        -------
        None.

        """

        self.master_file = master_file
        self.concat_list = concat_list
        self.file_type = file_type
        reports = [
            FileMergeAnalyser(
                master_file=master_file,
                merge_file=file,
                file_type=file_type
                )
            .get_merge_report()
            for file in self.concat_list
            ]
        self.legal_list = [
            report['merge_file'] for report in reports if
            report['file_merge_legal']
            ]
        self.concat_reports = reports
        self.alias_maps = {
            report['merge_file']: report['aliased_units'] for report in
                reports
                }

    #--------------------------------------------------------------------------
    def get_concatenated_data(self):
        """
        Concatenate the data from the (legal) files in the concatenation list.

        Returns
        -------
        pd.core.frame.DataFrame
            The data, with columns order enforced from header (in case pandas
            handles ordering differently for the data versus header).

        """

        df_list = [get_data(file=self.master_file, file_type=self.file_type)]
        for file in self.legal_list:
            df_list.append(
                get_data(file=file, file_type=self.file_type)
                .rename(self.alias_maps[file], axis=1)
                )
        ordered_vars = self.get_concatenated_header().index.tolist()
        return (
            pd.concat(df_list)
            [ordered_vars]
            .sort_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_concatenated_header(self):
        """
        Concatenate the headers from the (legal) files in the concatenation list.

        Returns
        -------
        pd.core.frame.DataFrame
            The concatenated headers.

        """

        df_list = [get_header_df(file=self.master_file, file_type=self.file_type)]
        for file in self.legal_list:
            df_list.append(
                get_header_df(file=file, file_type=self.file_type)
                .rename(self.alias_maps[file])
                )
        df = pd.concat(df_list)
        return df[~df.index.duplicated()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_concatenation_report(self, as_text=False):
        """
        Get the concatenation report.

        Parameters
        ----------
        as_text : bool
            Return the reports as a list of strings ready for output to file.

        Returns
        -------
        None.

        """

        if not as_text:
            return self.concat_reports

        line_list = [
            f'Merge report for {self.file_type} master file '
            f'{self.master_file}\n'
            ]

        for report in self.concat_reports:
            line_list.extend(
                _get_results_as_txt(report) + ['\n']
                )

        if as_text:
            return line_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_concatenation_report(self, abs_file_path):
        """
        Write the concatenation report.

        Parameters
        ----------
        abs_file_path : str or pathlib.Path
            The file (including absolute path) to the file to write the report to.

        Returns
        -------
        None.

        """

        _write_text_to_file(
            line_list=self.get_concatenation_report(as_text=True),
            abs_file_path=abs_file_path
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileMergeAnalyser():
    """Analyse compatibility of merge between master and merge file."""

    def __init__(self, master_file, merge_file, file_type):
        """
        Initialise master, merge and file type parameters.

        Parameters
        ----------
        master_file : str or pathlib.Path
            Absolute path to master file.
        merge_file : str or pathlib.Path
            Absolute path to merge file.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro")

        Returns
        -------
        None.

        """

        if master_file == merge_file:
            raise RuntimeError('Master and merge file are the same!')
        self.master_file = master_file
        self.merge_file = merge_file
        self.file_type = file_type

    #--------------------------------------------------------------------------
    def compare_variables(self):
        """
        Check which variables are common or held in one or other of master and
        merge file (merge is deemed illegal if there are no common variables).

        Returns
        -------
        dict
            Analysis results and boolean legality.

        """

        master_df = get_header_df(
            file=self.master_file, file_type=self.file_type
            )
        merge_df = get_header_df(
            file=self.merge_file, file_type=self.file_type
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

    #--------------------------------------------------------------------------
    def compare_units(self):
        """
        Compare units header line (merge is deemed illegal if there are unit
        changes).

        Returns
        -------
        dict
            return a dictionary with the mismatched header elements
            (key='units_mismatch'), any aliased units(key='aliased units')
            and boolean legality (key='units_merge_legal').

        """

        # Subset the analysis for common variables, and build comparison df
        common_vars = (
            self.compare_variables()['common_variables']
            )
        compare_df = pd.concat(
            [(get_header_df(file=self.master_file, file_type=self.file_type)
              .rename({'units': 'master'}, axis=1)
              .loc[common_vars, 'master']
              ),
              (get_header_df(file=self.merge_file, file_type=self.file_type)
              .rename({'units': 'merge'}, axis=1)
              .loc[common_vars, 'merge']
              )], axis=1
            )

        # Get mismatched variables and check whether they are just aliases
        # (in which case merge is deemed legal, and alias mapped to master)
        mismatch_df = compare_df[compare_df['master']!=compare_df['merge']]
        mismatch_list, alias_dict = [], {}
        units_merge_legal = True
        for variable in mismatch_df.index:
            master_units = mismatch_df.loc[variable, 'master']
            merge_units = mismatch_df.loc[variable, 'merge']
            try:
                assert merge_units in UNIT_ALIASES[master_units]
                alias_dict.update({merge_units: master_units})
            except (KeyError, AssertionError):
                mismatch_list.append(variable)
                units_merge_legal = False

        # Return the result
        return {
            'units_mismatch': mismatch_list,
            'aliased_units': alias_dict,
            'units_merge_legal': units_merge_legal
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_interval(self):
        """
        Cross-check concatenation file has same frequency as master (merge is
        deemed illegal if not).

        Returns
        -------
        dict
            Dictionary with indication of whether merge intervals are same
            (legal) or otherwise (illegal).

        """

        return {
            'interval_merge_legal':
                get_file_interval(
                    file=self.master_file, file_type=self.file_type
                    ) ==
                get_file_interval(
                    file=self.merge_file, file_type=self.file_type
                    )
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_dates(self):
        """
        Check that the merge file contains unique dates (merge is deemed
        illegal if there are none).

        Returns
        -------
        dict
            Dictionary with indication of whether unique dates exist (legal)
            or otherwise (illegal).


        """

        return {
            'date_merge_legal':
                len(
                    set(get_dates(
                        file=self.master_file, file_type=self.file_type
                        )) -
                    set(get_dates(
                        file=self.merge_file, file_type=self.file_type
                        ))
                    ) > 0
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_merge_report(self, as_text=False, abs_file_path=None):
        """
        Get the merge report.

        Parameters
        ----------
        as_text : bool
            Return the reports as a list of strings ready for output to file.
        abs_file_path : str or pathlib.Path, optional
            Absolute file path to which to output the data. The default is None.

        Returns
        -------
        None.

        """

        results = (
            {
                'master_file': str(self.master_file),
                'merge_file': str(self.merge_file)
                } |
            self.compare_dates() |
            self.compare_interval() |
            self.compare_variables() |
            self.compare_units()
            )
        results['file_merge_legal'] = all(
            results[key] for key in results.keys() if 'legal' in key
            )

        if not any([as_text, abs_file_path]):
            return results

        line_list = (
            [f'Merge report for {self.file_type} master file '
             f'{self.master_file}\n'] +
            _get_results_as_txt(results=results)
            )

        if as_text:
            return line_list

        if abs_file_path:

            _write_text_to_file(
                line_list=line_list,
                abs_file_path=abs_file_path
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------



###############################################################################
### FUNCTIONS ###
###############################################################################



#-------------------#
# Handler retrieval #
#-------------------#



# #------------------------------------------------------------------------------
# def _get_handler_elements(file, file_type, concat_list=None):
#     """
#     Parameterise and return file handler for either single file or
#     multi-file concatenated data.

#     Parameters
#     ----------
#     file : str or pathlib.Path
#         Absolute path to master file.
#     file_type : str
#         The type of file (must be either "TOA5" or "EddyPro")
#     concat_list : list, optional
#         A list of files to merge with the master (master file variables and
#                                                   units are king).
#         The default is None.

#     Returns
#     -------
#     dict
#         Contains data (key 'data'), headers (key 'headers') and concatenation
#         report (key 'concat_report').

#     """

#     # Configure and return a file handler with data from single
#     if not concat_list:
#         data = get_data(file=file, file_type=file_type)
#         headers = get_header_df(file=file, file_type=file_type)
#         concat_report = None

#     # Get the concatenator
#     if concat_list:
#         concatenator = FileConcatenator(
#             master_file=file,
#             file_type=file_type,
#             concat_list=concat_list
#             )
#         data=concatenator.get_concatenated_data()
#         headers=concatenator.get_concatenated_header()
#         concat_report=concatenator.get_concatenation_report(as_text=True)

#     # Configure and return the file handler
#     return data, headers, concat_report
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_handler_elements(file, file_type):
    """
    Get elements required to populate file handler for either single file or
    multi-file concatenated data.

    Parameters
    ----------
    file : str or pathlib.Path
        Absolute path to master file.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")

    Returns
    -------
    dict
        Contains data (key 'data'), headers (key 'headers') and concatenation
        report (key 'concat_report').

    """

    # Try to grab the concatenation list
    if file_type == 'TOA5':
        concat_list = get_TOA5_backups(file=file)
    if file_type == 'EddyPro':
        concat_list = get_EddyPro_files(file=file)

    # Get data from single file
    if not concat_list:
        data = get_data(file=file, file_type=file_type)
        headers = get_header_df(file=file, file_type=file_type)
        concat_report = None

    # Get data from multiple files
    if concat_list:
        concatenator = FileConcatenator(
            master_file=file,
            file_type=file_type,
            concat_list=concat_list
            )
        data=concatenator.get_concatenated_data()
        headers=concatenator.get_concatenated_header()
        concat_report=concatenator.get_concatenation_report(as_text=True)

    # Return data
    return data, headers, concat_list, concat_report
#------------------------------------------------------------------------------



#---------------#
# File handling #
#---------------#



#------------------------------------------------------------------------------
def get_data(file, file_type, usecols=None):
    """
    Read the data from the TOA5 file

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")
    usecols : list, optional
        The subset of columns to keep. The default is None (all columns retained).

    Returns
    -------
    df : pd.core.frame.DataFrame
        Data.

    """

    # Get dictionary containing file configurations
    MASTER_DICT = FILE_CONFIGS[file_type]

    # Set rows to skip
    REQ_TIME_VARS = list(MASTER_DICT['time_variables'].keys())
    rows_to_skip = list(set([0] + list(MASTER_DICT['header_lines'].values())))
    rows_to_skip.remove(MASTER_DICT['header_lines']['variable'])

    # Set date parsing to handle file type (convention is to keep the native
    # date stamps as data as well as being used to build the index)
    if len(REQ_TIME_VARS) == 1:
        date_parse_arg = REQ_TIME_VARS
        drop_index_vars = False
    else:
        date_parse_arg = {'TIMESTAMP': REQ_TIME_VARS}
        drop_index_vars = True

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
            parse_dates=date_parse_arg,
            keep_date_col=True,
            na_values=MASTER_DICT['na_values'],
            sep=MASTER_DICT['separator'],
            engine='c',
            on_bad_lines='warn',
            low_memory=False
            )
        .set_index(keys='TIMESTAMP', drop=drop_index_vars)
        .pipe(_integrity_checks, non_numeric=MASTER_DICT['non_numeric_cols'])
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
def get_header_df(file, file_type):
    """
    Get a dataframe with variables as index and units and statistical
    sampling type as columns.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")

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
    if file_type == 'EddyPro':
        return _EddyPro_line_formatter
    raise NotImplementedError(f'File type {file_type} not implemented!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _EddyPro_line_formatter(line):

    sep = FILE_CONFIGS['EddyPro']['separator']
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
    """
    Line-by-line date parser.

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")

    Returns
    -------
    date_list : list
        The dates.

    """

    line_formatter = get_line_formatter(file_type=file_type)
    with open(file, 'r') as f:
        for line in f:
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
    """
    Convert line to date.

    Parameters
    ----------
    fmt_line : list.
        Line elements.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")

    Returns
    -------
    pydatetime
        The date and time.

    """

    locs = FILE_CONFIGS[file_type]['time_variables'].values()
    return dt.datetime.strptime(
        ' '.join([fmt_line[loc] for loc in locs]),
        '%Y-%m-%d %H:%M:%S'
        )
#------------------------------------------------------------------------------



#------------------------------#
# Concatenation file retrieval #
#------------------------------#



#------------------------------------------------------------------------------
def get_TOA5_backups(file, return_abs_path=True):

    file_to_parse = _check_file_exists(file=file)
    return list(file_to_parse.parent.glob(f'{file_to_parse.stem}*.backup'))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_EddyPro_files(file, return_abs_path=True):

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
def get_file_interval(file, file_type):
    """
    Find the file interval (i.e. time step)

    Parameters
    ----------
    file : str or Pathlib.Path
        The file to parse.
    file_type : str
        The type of file (must be either "TOA5" or "EddyPro")

    Raises
    ------
    RuntimeError
        Raised if the most common value is not the minimum value.

    Returns
    -------
    minimum_val : int
        The interval.

    """

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



#---------------#
# Miscellaneous #
#---------------#



#------------------------------------------------------------------------------
def _get_results_as_txt(results):

    return [
        f'Merge file: {results["merge_file"]}',
        f'Merge legal? -> {str(results["file_merge_legal"])}',
        f'  - Date merge legal? -> {str(results["date_merge_legal"])}',
        '  - Interval merge legal? -> '
        f'{str(results["interval_merge_legal"])}',
        '  - Variable merge legal? -> '
        f'{str(results["variable_merge_legal"])}',
        '    * Variables contained only in master file -> '
        f'{results["master_only"]}',
        '    * Variables contained only in merge file -> '
        f'{results["merge_only"]}',
        f'  - Units merge legal? -> {str(results["units_merge_legal"])}',
        f'    * Variables with aliased units -> {results["aliased_units"]}',
        '     * Variables with mismatched units -> '
        f'{results["units_mismatch"]}',
        ]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_text_to_file(line_list, abs_file_path):

    with open(abs_file_path, 'w') as f:
        for line in line_list:
            f.write(f'{line}\n')
#------------------------------------------------------------------------------
