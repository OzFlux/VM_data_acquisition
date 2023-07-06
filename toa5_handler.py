# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 10:58:42 2023

@author: jcutern-imchugh
"""

import datetime as dt
import numpy as np
import os
import pandas as pd
import pathlib

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

INFO_LIST = [
    'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
    'program_name', 'program_sig', 'table_name'
    ]

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class single_file_data_handler():

    def __init__(self, file):
        """
        Class to get headers and data from a TOA5 file WITH merge of backups.

        Parameters
        ----------
        file : str or pathlib.Path
            Absolute path to file.

        """

        self.file = check_file_path(file=file)
        self.interval_as_num = get_TOA5_interval(file=file, as_offset=False)
        self.interval_as_offset = f'{self.interval_as_num}T'
        self.info = get_station_info(file=file)
        self.headers = get_header_df(file=file)
        self.data = get_TOA5_data(file=file)
        self.merged_files = None

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
    def _get_gap_distribution(self):
        """
        List of gap lengths and their frequencies (counts).

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with number of records as index and count as column.

        """

        gap_series = (
            (
                self.data.reset_index()['TIMESTAMP'] -
                self.data.reset_index()['TIMESTAMP'].shift()
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
    def get_missing_records(self):
        """
        Get simple statistics for missing records

        Returns
        -------
        dict
            Dictionary containing the number of missing cases, the % of missing
            cases and the distribution of gap sizes.

        """

        complete_index = pd.date_range(
            start=self.data.index[0],
            end=self.data.index[-1],
            freq=self.interval_as_offset
            )
        n_missing = len(complete_index) - len(self.data)
        return {
            'n_missing': n_missing,
            '%_missing': round(n_missing / len(complete_index) * 100, 2),
            'gap_distribution': self._get_gap_distribution()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_sampling(self, variable):
        """
        Gets the sampling treatment for a given variable

        Parameters
        ----------
        variable : str
            The variable for which to return the sampling.

        Returns
        -------
        str
            The sampling.

        """

        return self.headers.loc[variable, 'sampling']
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

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class merged_file_data_handler(single_file_data_handler):

    def __init__(self, file, write_merge_report=True):
        """
        Class to get headers and data from a TOA5 file WITH merge of backups.

        Parameters
        ----------
        file : str or pathlib.Path
            Absolute path to file.
        write_merge_report : bool, optional
            Whether to write the merge report during init. The default is True.

        Returns
        -------
        None.

        """

        super().__init__(file)
        concatenator = file_concatenator(file=file)
        self.info = concatenator.make_station_info()
        self.headers = concatenator.merge_all_headers()
        self.data = concatenator.merge_all_data()
        self.merged_files = (
            [concatenator.master_file.name] + concatenator.merge_file_list
            )
        if write_merge_report:
            concatenator.write_merge_report()


#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_concatenator():

    def __init__(self, file, enforce_consistent_sampling=True):
        """
        Class to concatenate backups.

        Parameters
        ----------
        file : str or pathlib.Path
            Absolute path to file.
        enforce_consistent_sampling : TYPE, optional
            DESCRIPTION. The default is True.

        Raises
        ------
        FileNotFoundError
            Raised if no backup files or none that can be legally merged.

        Returns
        -------
        None.

        """

        self.master_file = check_file_path(file)
        self.path = self.master_file.parent
        self.merge_file_list = get_backup_files(self.master_file)
        if not self.merge_file_list:
            raise FileNotFoundError('No files to concatenate! Exiting...')
        self.get_legal_files()

    #--------------------------------------------------------------------------
    def compare_data_interval(self, with_file):
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

        master_interval = get_TOA5_interval(file=self.master_file)
        file_interval = get_TOA5_interval(file=self.path / with_file)
        if not file_interval == master_interval:
            return {'interval_merge_legal': False}
        return {'interval_merge_legal': True,
                'master_interval': f'{master_interval}'}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_sampling_header(self, with_file, enforce_legality=False):
        """
        Wrapper for comparison of statistical sampling header line.

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        dict
            return a dictionary with the mismatched header elements.

        """

        return self._compare_units_sampling_header(
            with_file=with_file, which='sampling'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_station_info(self, with_file):
        """
        Compare station info header lines.

        Parameters
        ----------
        with_file : str
            Name of file to compare to the master file.

        Returns
        -------
        dict
            results of comparison including legality assessment.

        """

        illegal_list = ['format', 'logger_type', 'table_name']
        master_info = get_station_info(self.master_file)
        backup_info = get_station_info(self.path / with_file)
        legal = True
        msg_list = []
        for key in master_info.keys():
            if backup_info[key] == master_info[key]:
                continue
            if key in illegal_list:
                legal = False
            msg_list.append(
                'Mismatch in station info header: '
                f'{key} was {master_info[key]} in master file, '
                f'{backup_info[key]} in merge file (legal -> {legal})'
                )
        if not msg_list:
            msg_list.append('No differences found in station info!')
        return {'info_merge_legal': legal, 'msg_list': msg_list}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_units_header(self, with_file):
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

        units_dict = self._compare_units_sampling_header(
            with_file=with_file, which='units'
            )
        if units_dict['units_mismatch'] != ['None']:
            units_dict['units_merge_legal'] = False
        else:
            units_dict['units_merge_legal'] = True
        return units_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _compare_units_sampling_header(self, with_file, which):
        """
        Compare either units (units) or statistical sampling type (stat) header
        line to see if all elements are consistent.

        Parameters
        ----------
        file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).
        which : str
            which of 'units' or 'stat' to compare.

        Returns
        -------
        dict
            return a dictionary with the mismatched header elements.

        """


        common_vars = (
            self.compare_variable_header(with_file=with_file)
            ['common_variables']
            )
        compare_df = pd.concat(
            [(get_header_df(file=self.master_file)
              .rename({which: f'master_{which}'}, axis=1)
              .loc[common_vars, f'master_{which}']
              ),
              (get_header_df(file=self.path / with_file)
              .rename({which: f'bkp_{which}'}, axis=1)
              .loc[common_vars, f'bkp_{which}']
              )], axis=1
            )
        mismatched_vars = (
            compare_df[compare_df[f'master_{which}']!=compare_df[f'bkp_{which}']]
            .index
            .tolist()
            )
        if len(mismatched_vars) == 0: mismatched_vars = ['None']
        return {f'{which}_mismatch': mismatched_vars}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_variable_header(self, with_file):
        """
        Cross-check the elements of the variable name header line ('info') of a
        backup file for concatenation with the master file.

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        dict
            Dictionary containing lists of variables common to both files
            (key='common'), those contained only in the master
            (key='master_only') and those contained only in the backup
            (key='backup_only').

        """

        master_df = get_header_df(file=self.master_file)
        backup_df = get_header_df(self.path / with_file)
        common_variables = list(set(master_df.index).intersection(backup_df.index))
        master_vars = list(
            set(master_df.index.tolist()) - set(backup_df.index.tolist())
            )
        if not master_vars:
            master_vars.append('None')
        backup_vars = list(
            set(backup_df.index.tolist()) - set(master_df.index.tolist())
            )
        if not backup_vars:
            backup_vars.append('None')
        return {
            'common_variables': common_variables,
            'master_only': master_vars,
            'backup_only': backup_vars
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

        merge_dict = (
            {'file_name': with_file} |
            get_file_dates(file=self.path / with_file) |
            self.compare_station_info(with_file=with_file) |
            self.compare_variable_header(with_file=with_file) |
            self.compare_units_header(with_file=with_file) |
            self.compare_sampling_header(with_file=with_file) |
            self.compare_data_interval(with_file=with_file)
            )
        if not as_text:
            return merge_dict
        return self._get_merge_assessment_as_text(merge_dict=merge_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_legal_files(self):
        """
        Return a list of the files which can be legally concatenated.

        Returns
        -------
        list
            The files that can be legally concatenated.

        """

        legals = [
            'info_merge_legal', 'units_merge_legal', 'interval_merge_legal'
            ]
        legal_list = []
        for file in self.merge_file_list:
            merge_dict = self.do_merge_assessment(with_file=file)
            if all(merge_dict[x] for x in legals):
                legal_list.append(file)
        if not legal_list:
            raise FileNotFoundError('No files can be legally merged!')
        return legal_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_merge_assessment_as_text(self, merge_dict):
        """
        Converts the dictionary containing merge results into a list of text
        strings to be written to file.

        Parameters
        ----------
        merge_dict : dict
            The output from the get_merge_assessment method.

        Returns
        -------
        write_list : list
            List of strings to be written to file.

        """

        write_list = []
        write_list.append(f'Backup file {merge_dict["file_name"]}\n\n')
        write_list.append(
            f'Start_date: {merge_dict["start_date"]}, '
            f'End_date: {merge_dict["end_date"]}\n'
            )
        write_list.append(
            'Info header line merge legal? -> '
            f'{merge_dict["info_merge_legal"]}\n'
            )
        write_list.append('Info header line merge warnings: \n')
        for x in merge_dict['msg_list']:
            write_list.append(f'    - {x}\n')
        master_only = ', '.join(merge_dict['master_only'])
        write_list.append(
            f'Variables contained only in master file -> {master_only}\n'
            )
        backup_only = ', '.join(merge_dict['backup_only'])
        write_list.append(
            f'Variables contained only in backup file -> {backup_only}\n'
            )
        write_list.append(
            'Units merge legal? -> '
            f'{merge_dict["units_merge_legal"]}\n'
            )
        mismatched_unit_vars = ', '.join(merge_dict['units_mismatch'])
        write_list.append(
            f'Variables with mismatched units -> {mismatched_unit_vars}\n'
            )
        mismatched_sampling_vars = ', '.join(merge_dict['sampling_mismatch'])
        write_list.append(
            'Variables with mismatched statistical sampling -> '
            f'{mismatched_sampling_vars}\n'
            )
        write_list.append(
            'Interval merge legal? -> '
            f'{merge_dict["interval_merge_legal"]}\n\n'
            )
        return write_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def make_station_info(self):
        """
        Make a synthetic station info header line for the merged file.

        Returns
        -------
        dict
            Dictionary with station info key-value pairs (see INFO_LIST).

        """

        df = self.merge_all_station_info()
        all_same = df.eq(df[self.master_file.name], axis=0).all(1)
        return dict(zip(
            INFO_LIST,
            df[self.master_file.name].where(all_same, 'N/A - merged')
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_all_station_info(self, legals_only=True):
        """
        Collate the station information for all files (including master).

        Parameters
        ----------
        legals_only : bool, optional
            If true, parses ONLY the files for which merge is legal.
            The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the station information (index) for each
            file (columns).

        """

        file_list = (
            [self.master_file.name] +
            self.get_legal_files() if legals_only else self.merge_file_list
            )
        return pd.DataFrame(
            [get_station_info(file=self.path / file) for file in file_list],
            index=file_list
            ).T
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_all_data(self):
        """
        Merge the master and backup file data (basic data conditioning - no
                                               checks for duplicates e.g.)

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe containing the data!

        """

        # Concatenate all of the legal files
        file_list = [self.master_file.name] + self.get_legal_files()
        df = (
            pd.concat([get_TOA5_data(self.path / file) for file in file_list])
            .sort_index()
            )

        # In case there are some ordering subtleties under the hood, force the
        # dataframe to share the same ordering as the headers
        variables = self.merge_all_headers().index.tolist()
        variables.remove('TIMESTAMP')
        return df[variables]

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_all_headers(self, legals_only=True):
        """
        Merge the header lines from the master and backup files.

        Parameters
        ----------
        legals_only : bool, optional
            If true, parses ONLY the files for which merge is legal.
            The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe containing the header info!

        """

        file_list = (
            [self.master_file.name] +
            self.get_legal_files() if legals_only else self.merge_file_list
            )
        return (
            pd.concat(
                [get_header_df(file=self.path / file) for file in file_list]
                )
            .reset_index()
            .drop_duplicates()
            .set_index(keys='variable')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_merge_report(self):
        """
        Write report to target directory master file.

        Returns
        -------
        None.

        """

        write_list = [f'Merge report for master file {self.master_file}\n\n']
        for file in self.merge_file_list:
            write_list += self.do_merge_assessment(with_file=file, as_text=True)
        out_name = self.master_file.name.replace('.dat', '')
        with open(self.path / f'merge_report_{out_name}.txt', 'w') as f:
            f.writelines(write_list)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_file_path(file, as_str=False):
    """
    Check whether is a real file, and if string, create a pathlib.Path object.

    Parameters
    ----------
    file : str or pathlib.Path object
        Absolute path to file (pathlib).

    Raises
    ------
    FileNotFoundError
        Raised if file does not exist.

    Returns
    -------
    pathlib.Path object
        Returns the full absolute path to the file.

    """

    file_to_check = pathlib.Path(file)
    if not file_to_check.parent.exists():
        raise FileNotFoundError('Invalid directory!')
    if not file_to_check.exists():
        raise FileNotFoundError('No file of that name in directory!')
    if as_str:
        return str(file_to_check)
    return file_to_check
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
def get_file_handler(file, concat_backups=False):

    if concat_backups:
        try:
            return merged_file_data_handler(file=file)
        except FileNotFoundError:
            pass
    return single_file_data_handler(file=file)
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
def get_file_info(file):
    """
    Convenience function to pull together header info, file start and finish
    and presence of backup files

    Parameters
    ----------
    file : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    return (
        get_station_info(file=file) |
        get_file_dates(file=file) |
        {'backups': ','.join(get_backup_files(file=file))}
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_header_df(file, usecols=None):
    """
    Get a dataframe with variables as index and units and statistical
    sampling type as columns.

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe as per above.

    """

    headers = get_file_headers(file=file)[1:]
    lines_dict = {}
    for line, var in enumerate(['variable', 'units', 'sampling']):
        lines_dict[var] = _format_line(line=headers[line])
    idx = pd.Index(lines_dict.pop('variable'), name='variable')
    df = pd.DataFrame(data=lines_dict, index=idx)
    if usecols: return df.loc[usecols]
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_station_info(file, as_dict=True):
    """
    Get the first line with the station info from the logger.

    Parameters
    ----------
    as_dict : Bool, optional
        Returns a dictionary with elements split and assigned descriptive
        keys if true, else just the raw string. The default is True.

    Returns
    -------
    str or dict
        Station info header line.

    """

    info = get_file_headers(file=file)[0]
    info_elements = _format_line(line=info)
    if not as_dict:
        return info_elements
    return dict(zip(INFO_LIST, info_elements))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOA5_data(file, usecols=None):

    # Make sure usecols contains more than just the timestamp
    if usecols == ['TIMESTAMP']:
        raise RuntimeError(
            'TIMESTAMP is the index column - it cannot be the only '
            'requested variable!'
            )

    # Here we want TIMESTAMP even if not requested in usecols arg -
    # TIMESTAMP becomes the index (and we want to preserve order of columns)
    if usecols:
        thecols = list(dict.fromkeys(['TIMESTAMP'] + usecols))
    else:
        thecols = None

    # Import data
    df = pd.read_csv(
        file, skiprows=[0,2,3], usecols=thecols, parse_dates=['TIMESTAMP'],
        index_col=['TIMESTAMP'], na_values='NAN', sep=',', engine='c',
        on_bad_lines='warn', low_memory=False
        )

    # Convert any non-numeric data
    non_nums = df.select_dtypes(include='object')
    for col in non_nums.columns:
        df[col] = pd.to_numeric(non_nums[col], errors='coerce')

    # Sort the index
    df.sort_index(inplace=True)

    # Return
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOA5_interval(file, as_offset=True):

    df = get_TOA5_data(file=file, usecols=['RECORD'])
    num = get_index_interval(idx=df.index.drop_duplicates())
    if as_offset: return f'{num}T'
    return num
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_backup_files(file):
    """
    Check in the master file's directory for the presence of backup files

    Parameters
    ----------
    file : pathlib.Path
        The absolute path to the file.

    Returns
    -------
    file_list : list
        List of backup files present (empty list if none).

    """

    file_to_parse = check_file_path(file=file)
    file_list = [
        x.name for x in file_to_parse.parent.glob(f'{file_to_parse.stem}*')
        ]
    file_list.remove(file_to_parse.name)
    first_backup = file_to_parse.name + '.backup'
    try:
        temp = file_list.pop(file_list.index(first_backup))
        file_list = [temp] + file_list
    except ValueError:
        pass
    return file_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_line(line):

    return [x.replace('"', '') for x in line.strip().split('","')]
#------------------------------------------------------------------------------

def get_index_interval(idx):

    diffs = (idx[1:] - idx[:-1]).unique()
    return (
        pd.TimedeltaIndex(
            filter(lambda x: np.mod(x.components.minutes, 30) == 0, diffs)
            )
        .min()
        .components.minutes
        )