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

    """Class for construction of generic TOA5 files"""

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

        def str_func(the_list):

           return ','.join([f'"{item}"' for item in the_list]) + '\n'

        amended_names = ['TIMESTAMP'] + self.data.columns.tolist()
        amended_units = ['TS'] + self.units
        amended_samples = [''] + self.samples
        return [
            str_func(the_list=self.info_header),
            str_func(the_list=amended_names),
            str_func(the_list=amended_units),
            str_func(the_list=amended_samples)
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
        """
        Class to generate a standardised dataset from source tables (requires
        a map that allows translation to standard names); note - currently
        contains a hack to stop Tumbarumba being processed in 60-minute blocks
        (but it should be - seems raw data from the Campbell system is only
        running at 30)

        Parameters
        ----------
        site : str
            Name of site for which to merge tables.

        Returns
        -------
        None.

        """

        self.site = site
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )
        if not site == 'Tumbarumba':
            self.time_step = int(SITE_DETAILS.get_single_site_details(
                site=site, field='time_step'
                ))
        else:
            self.time_step = 30
        self.table_map = get_site_data_tables(site=site)
        self.var_map = vm.mapper(site=site)

    #--------------------------------------------------------------------------
    def get_table_data(self, file):
        """


        Parameters
        ----------
        file : TYPE
            DESCRIPTION.

        Returns
        -------
        df : TYPE
            DESCRIPTION.

        """

        handler = _get_file_handler(file=self.path / file)
        time_step = get_TOA5_interval(file=self.path / file)
        variable_df = self.var_map.get_variable_fields(table_file=file)
        usecols = variable_df.site_name.tolist()
        translation_dict = dict(zip(
            variable_df.site_name, variable_df.translation_name
            ))
        dates = handler.get_date_span()
        new_index = pd.date_range(
            start=dates['start_date'], end=dates['end_date'], freq=time_step
                        )
        new_index.name = 'TIMESTAMP'
        df = (handler.get_data_df(usecols=usecols)
              .resample(f'{self.time_step}T')
              .interpolate(limit=1)
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

        headers_df = _get_file_handler(self.path / file).get_header_df()
        variables_df = self.var_map.get_variable_fields(table_file=file)
        return (
            headers_df
            .drop(labels='units', axis=1)
            .rename({'stat': 'sampling'}, axis=1)
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
        constructor.write_output_file(dest=self.path / f'{self.site}_merged_std.dat')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class L1_constructor():

    def __init__(self, site):
        """
        Simple class for generating the collated L1 file from individual TOA5
        files, including concatenation of backup files where legal.

        Parameters
        ----------
        site : str
            Site name.

        Returns
        -------
        None.

        """

        self.site = site
        self.time_step = int(SITE_DETAILS.get_single_site_details(
            site=site, field='time_step'
            ))
        self.tables_df = (
            get_site_data_tables(site=site)
            .reset_index()
            .set_index(keys='file_name')
            )
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )

    #--------------------------------------------------------------------------
    def get_file_data(self, file):
        """
        Get the file data, including all headers

        Parameters
        ----------
        file : pathlib.Path object
            Full path to file.

        Returns
        -------
        dict
            Contains pre-header info, variable-specific header and data as
            values with 'info', 'header' and 'data' as respective keys.

        """

        handler = _get_file_handler(file=self.path / file)
        return {
            'info': handler.get_info(as_dict=False),
            'header': handler.get_header_df(),
            'data': handler.get_data_df()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_inclusive_date_index(self):
        """
        Get an index that spans the earliest and latest dates across all files
        to be included in the collection.

        Returns
        -------
        pandas.core.indexes.datetimes.DatetimeIndex
            Index with correct (site-specific) time step.

        """

        return pd.date_range(
            start=self.tables_df.start_date.min(),
            end=self.tables_df.end_date.max(),
            freq=f'{self.time_step}T'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_excel(self, na_values=''):
        """
        Write table files out to separate tabs in an xlsx file.

        Parameters
        ----------
        na_values : str, optional
            The na values to fill nans with. The default is ''.

        Returns
        -------
        None.

        """

        # Get data and header for all files (do concatenation where required)
        dest = self.path / f'{self.site}_L1.xlsx'

        # Get inclusive date index to reindex all files to
        date_idx = self.get_inclusive_date_index()

        # Iterate over all files and write to separate excel workbook sheets
        with pd.ExcelWriter(path=dest) as writer:
            for file in self.tables_df.index:
                sheet_name = file.replace('.dat', '')
                output_dict = self.get_file_data(file=file)

                # Write info
                (pd.DataFrame(output_dict['info'])
                 .T
                 .to_excel(
                     writer, sheet_name=sheet_name, header=False, index=False,
                     startrow=0
                     )
                 )

                # Write header
                (output_dict['header']
                 .reset_index()
                 .T
                 .to_excel(
                     writer, sheet_name=sheet_name, header=False, index=False,
                     startrow=1
                     )
                 )

                # Write data
                (output_dict['data']
                 .reindex(date_idx)
                 .reset_index()
                 .to_excel(
                     writer, sheet_name=sheet_name, header=False, index=False,
                     startrow=4, na_rep=na_values
                     )
                  )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class TOA5_concatenator():

    def __init__(self, master_file):
        """
        Class to concatenate backup files with the master file currently
        being written to.

        Parameters
        ----------
        master_file : str or pathlib.Path
            Absolute file path.

        Raises
        ------
        FileNotFoundError
            Raised if file does not exist.
        TypeError
            Raised if the file is not of type *.dat.

        Returns
        -------
        None.

        """

        self.master_file = pathlib.Path(master_file)
        if not self.master_file.exists():
            raise FileNotFoundError(f'File "{self.master_file}" does not exist!')
        if not self.master_file.suffix == '.dat':
            raise TypeError('File must be of type ".dat"')
        self.file_path = self.master_file.parent
        self.file_name = self.master_file.name
        self.interval = get_TOA5_interval(file=self.master_file)
        self.merge_file_list = self._get_merge_file_list()

    #--------------------------------------------------------------------------
    ### BEGIN PRIVATE METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_input_file(self, file):
        """
        Check whether user input is correct for file parameter.

        Parameters
        ----------
        file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Raises
        ------
        FileNotFoundError
            Raised if file does not exist or is not in the list of available
            files.

        Returns
        -------
        pathlib.Path object
            Returns the full absolute path to the file, regardless of user input
            (this provides the file input for other methods).

        """

        if file in self.merge_file_list:
            return file
        if not file in [x.name for x in self.merge_file_list]:
            raise FileNotFoundError(f'File "{file}" not found!')
        return self.file_path / file
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _compare_units_stats_header(self, with_file, which):
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

        with_file = self._check_input_file(file=with_file)
        common_vars = (
            self.compare_variable_header(with_file=with_file)
            ['common_variables']
            )
        compare_df = pd.concat(
            [(TOA5_data_handler(self.master_file).get_header_df()
              .rename({which: f'master_{which}'}, axis=1)
              .loc[common_vars, f'master_{which}']
              ),
             (TOA5_data_handler(self.file_path / with_file).get_header_df()
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
            'Variables with mismatched units -> '
            f'{mismatched_unit_vars}\n'
            )
        mismatched_sampling_vars = ', '.join(merge_dict['stat_mismatch'])
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
    def _get_merge_file_list(self):
        """
        Get the list of files eligible for merge

        Raises
        ------
        RuntimeError
            Raised if no files.

        Returns
        -------
        files : list
            List of files (pathlib.Path object of absolute file path).

        """

        files = list(self.file_path.glob(f'{self.master_file.name}*'))
        files.remove(self.master_file)
        if not files:
            raise RuntimeError('No files to concatenate!')
        return files
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _post_merge_checks(self, df):
        """
        This method should check for bad things in the data as opposed the
        header alone. At present, it has only one condition - whether there are
        duplicate indices with non-duplicate data (should be rare). If there are,
        keeps the first and dumps all subsequent (which is potentially dangerous).

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            The concatenated dataframe to check.

        Returns
        -------
        dict
            Contains the corrected data (key='data') and a report listing all
            duplicate indices (if any; key='duplicate indices').

        """

        post_merge_str = 'Duplicate indices with non-duplicate data-> '
        if len(df) == len(df[~df.index.duplicated()]):
            return {'data': df, 'duplicate_indices': post_merge_str + 'None'}
        else:
            idx = df[df.index.duplicated()].index
            df = df[~df.index.duplicated(keep='first')]
            return {
                'data': df, 'duplicate_indices': (
                    [post_merge_str + 'Multiple (keeping first instance):\n'] +
                    [x.strftime('    - %Y-%m-%d %H:%M\n') for x in idx.to_pydatetime()]
                        )
                    }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _write_merge_report(self, write_list):
        """
        Does the underlying write to disk of the merge report.

        Parameters
        ----------
        write_list : list
            List containing assessment of merge.

        Returns
        -------
        None.

        """

        write_list = (
            [f'Merge report for master file {self.file_name}\n\n'] +
            write_list
            )
        out_name = self.file_name.replace('.dat', '')
        with open(self.file_path / f'merge_report_{out_name}.txt', 'w') as f:
            f.writelines(write_list)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### END PRIVATE METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_data_interval(self, with_file):
        """
        Cross-check that a file for concatenation has the same frequency as the
        master file.

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

        self._check_input_file(file=with_file)
        file_interval = get_TOA5_interval(file=self.file_path / with_file)
        if not self.interval == file_interval:
            return {'interval_merge_legal': False}
        return {'interval_merge_legal': True}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_header_lines(self, with_file, which=None):
        """
        Convenience method to pull all header line comparison methods together
        and allow them to be called with keywords.

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).
        which : str or list, optional
            Either the individual header line to compare, or a list thereof.
            Valid options are 'info', 'variables', 'units' and 'stats'. The
            default is None.

        Raises
        ------
        TypeError
            Raised if 'which' is not list or str.

        Returns
        -------
        rslt_dict :
            DESCRIPTION.

        """

        line_dict = {'info': self.compare_info_header,
                     'variables': self.compare_variable_header,
                     'units': self.compare_units_header,
                     'stats': self.compare_stats_header}
        if not which:
            which = line_dict.keys()
        else:
            if isinstance(which, str):
                which = [which]
            if not isinstance(which, list):
                raise TypeError('"which" kwarg must be str or list of str')
        rslt_dict = {}
        for line in which:
            rslt_dict.update(line_dict[line](with_file=with_file))
        return rslt_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_info_header(self, with_file):
        """
        Cross-check the elements of the first header line ('info') of a backup
        file for concatenation with the master file. If any of file format,
        logger type or table name are different, merge is deemed illegal. Other
        non-matching elements are noted in the merge assessment but do not stop
        merge.

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        dict
            Assessment of legality and list of elements that differ.

        """

        with_file = self._check_input_file(file=with_file)
        illegal_list = ['format', 'logger_type', 'table_name']
        master_info = TOA5_data_handler(self.master_file).get_info()
        merge_info = TOA5_data_handler(with_file).get_info()
        legal = True
        msg_list = []
        for key in master_info:
            if master_info[key] != merge_info[key]:
                if key in illegal_list:
                    msg_list.append(f'Illegal mismatch in header info line {key}')
                    legal = False
                    continue
                msg_list.append(
                    'difference in station info header - '
                    f'{key} was {master_info[key]} in master file, '
                    f'{merge_info[key]} in merge file'
                    )
        if not msg_list:
            msg_list = [    ' - None']
        return {'info_merge_legal': legal, 'msg_list': msg_list}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_stats_header(self, with_file):
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

        return self._compare_units_stats_header(with_file=with_file, which='stat')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_units_header(self, with_file):
        """
        Wrapper for comparison of units header line (merge is deemed illegal if
        there are unit changes).

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

        units_dict = (
            self._compare_units_stats_header(with_file=with_file, which='units')
            )
        if len(units_dict['units_mismatch']) == 0:
            units_dict['units_merge_legal'] = False
        else:
            units_dict['units_merge_legal'] = True
        return units_dict
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

        with_file = self._check_input_file(file=with_file)
        master_df = TOA5_data_handler(self.master_file).get_header_df()
        backup_df = TOA5_data_handler(with_file).get_header_df()
        common_variables = [x for x in master_df.index if x in backup_df.index]
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
    def get_data_df(self, usecols=None, write_merge_report=True, resample_intvl=None):
        """
        Get the concatenated dataframe (ensuring that the dataframe columns are
        aligned to the concatenated header)

        Parameters
        ----------
        write_merge_report : Bool, optional
            Whether to write merge report to the target directory for the
            master file. The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the concatenated data.

        """

        legals = [
            'info_merge_legal', 'units_merge_legal', 'interval_merge_legal'
            ]
        master_handler = TOA5_data_handler(file=self.master_file)
        data_list = [master_handler.get_data_df()]
        header_list = [master_handler.get_header_df()]
        report_list = []

        # Iterate over files (skip concatenation if illegal, but report)
        for file in self.merge_file_list:
            rslt_dict = self.get_merge_assessment(with_file=file)
            report_list += self._get_merge_assessment_as_text(merge_dict=rslt_dict)
            if not all([rslt_dict[x] for x in legals]):
                continue
            handler = TOA5_data_handler(file=file)
            data_list.append(handler.get_data_df())
            header_list.append(handler.get_header_df())

        # Concatenate data
        checks_dict = self._post_merge_checks(df=
            pd.concat(data_list)
            .drop_duplicates()
            .sort_index()
            )

        # Write merge report if requested
        report_list += checks_dict['duplicate_indices']
        if write_merge_report:
            self._write_merge_report(write_list=report_list)

        # Align headers and return
        header_df = pd.concat(header_list)
        cols_to_keep = header_df[~header_df.index.duplicated()].index.tolist()
        cols_to_keep.remove('TIMESTAMP')
        return checks_dict['data'][cols_to_keep]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_date_span(self):
        """
        Get the earliest start date and latest end date across all files (this
        mimics the functionality of the single file handler but extends it to
        the concatenated file case). Mainly used to provide a common
        accessibility method with the single file handler.

        Returns
        -------
        dict
            Start (key='start_date') and end (key='end_date') dates.

        """

        df = self.get_file_dates()
        return {
            'start_date': df.start_date.min(), 'end_date': df.end_date.max()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_dates(self, legals_only=True):
        """
        Get the start and finish date for all files (including both master and
        backups).

        Parameters
        ----------
        legals_only : TYPE, optional
            Whether to only include merge-legal files or all. The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe cointaining filename as index and start and end date as
            columns.

        """

        iter_list = (
            [self.master_file] +
            self.get_legal_files() if legals_only else self.merge_file_list
            )
        return pd.DataFrame([get_file_dates(x) for x in iter_list], index=iter_list)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_header_df(self):
        """
        Get a dataframe containing the variable, units and statistical sampling
        method of the concatenated file.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the above.

        """

        legals = [
            'info_merge_legal', 'units_merge_legal', 'interval_merge_legal'
            ]
        header_list = [TOA5_data_handler(file=self.master_file).get_header_df()]
        for file in self.merge_file_list:
            rslt_dict = self.get_merge_assessment(with_file=file)
            if not all([rslt_dict[x] for x in legals]):
                continue
            header_list.append(TOA5_data_handler(file=file).get_header_df())
        header_df = pd.concat(header_list)
        return header_df[~header_df.index.duplicated()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_illegal_files(self):
        """
        Return a list of the files which cannot be legally concatenated.

        Returns
        -------
        list
            The files that cannot be legally concatenated.

        """

        return [x for x in self.merge_file_list if not x in self.get_legal_files()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_info(self, as_dict=True):
        """
        Return the info line for the concatenated file. Same as the method in
        the single file handler except that elements that differed across files
        are replaced with the string 'NA: merged file'.

        Parameters
        ----------
        as_dict : Bool, optional
            If True, return dictionary with descriptive keys for info line
            elements, else a list of the elements. The default is True.

        Returns
        -------
        dict or list
            The modified elements of the info line.

        """

        master_dict = TOA5_data_handler(file=self.master_file).get_info()
        file_list = [self.master_file] + self.merge_file_list
        info_list = [list(master_dict.values())]
        for f in file_list[1:]:
            info_list.append(list(
                TOA5_data_handler(file=f).get_info().values()
                ))
        name_list = [x.name for x in file_list]
        df = pd.DataFrame(info_list, index=name_list, columns=master_dict.keys()).T
        df['same'] = df.eq(df.iloc[:,0], axis=0).all(1)
        out_df = (df[df.same]
                  .reindex(df.index)
                  .fillna('NA: merged_file')
                  [self.master_file.name]
                  )
        if not as_dict:
            return out_df.to_list()
        return out_df.to_dict()
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
            merge_dict = self.get_merge_assessment(with_file=file)
            if all([merge_dict[x] for x in legals]):
                legal_list.append(file)
        return legal_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_merge_assessment(self, with_file):
        """
        Compares all header lines (same as 'compare_header_lines' method but
        also compares file interval).

        Parameters
        ----------
        with_file : str or pathlib.Path object
            Either filename alone (str) or absolute path to file (pathlib).

        Returns
        -------
        rslt_dict : dict
            Result of all comparisons.

        """

        with_file = self._check_input_file(file=with_file)
        rslt_dict = {'file_name': with_file.name}
        rslt_dict.update(self.compare_header_lines(with_file=with_file))
        rslt_dict.update(self.compare_data_interval(with_file=with_file))
        return rslt_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_merge_report(self):
        """
        Write report to target directory master file.

        Returns
        -------
        None.

        """

        write_list = []
        for file in self.merge_file_list:
            merge_dict = self.get_merge_assessment(with_file=file)
            write_list += self._get_merge_assessment_as_text(merge_dict=merge_dict)
        self._write_merge_report(write_list)
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

        self.file = check_file_path(file)
        self.interval = get_TOA5_interval(file=file)

    #--------------------------------------------------------------------------
    # Get rid of this?
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
            lines_dict[var] = self._format_line(line=headers[line])
        idx = pd.Index(lines_dict.pop('variable'), name='variable')
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
            keys if true, else just the raw string. The default is True.

        Returns
        -------
        str or dict
            Station info header line.

        """

        info = get_file_headers(file=self.file)[0]
        info_elements = self._format_line(line=info)
        if not as_dict:
            return info_elements
        info_list = [
            'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
            'program_name', 'program_sig', 'table_name'
            ]
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
    def get_date_span(self):

        return get_file_dates(file=self.file)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_df(self, usecols=None, resample_intvl=None):

        df = get_TOA5_data(file=self.file, usecols=usecols)
        if not resample_intvl:
            return df
        return df.resample(resample_intvl).pad()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _format_line(self, line):

        return [x.replace('"', '') for x in line.strip().split('","')]
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_header_line(line):

    return [x.replace('"', '') for x in line.strip().split('","')]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_file_handler(file):
    """
    Return the concatenator class if there are files for concatenation,
    otherwise return single file data handler class

    Parameters
    ----------
    file : str or pathlib.Path object
        Absolute path to file (pathlib).

    Returns
    -------
    file handler
        Concatenator or single file handler class.

    """

    check_file_path(file=file)
    try:
        return TOA5_concatenator(master_file=file)
    except RuntimeError:
        return TOA5_data_handler(file=file)
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
### PUBLIC FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_file_path(file):
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

    if isinstance(file, str):
        file = pathlib.Path(file)
    if not file.parent.exists():
        raise FileNotFoundError('Invalid directory!')
    if not file.exists():
        raise FileNotFoundError('Directory is valid but file name is not!')
    return file
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

    if usecols:
        if not 'TIMESTAMP' in usecols:
            usecols += ['TIMESTAMP']
    df = pd.read_csv(
        file, skiprows=[0,2,3], usecols=usecols, parse_dates=['TIMESTAMP'],
        index_col=['TIMESTAMP'], na_values='NAN', sep=',', engine='c',
        on_bad_lines='warn', low_memory=False
        )
    non_nums = df.select_dtypes(include='object')
    for col in non_nums.columns:
        df[col]=pd.to_numeric(non_nums[col], errors='coerce')
    df.drop_duplicates(inplace=True)
    df.sort_index(inplace=True)
    if not len(df[df.index.duplicated()]) == 0:
        print('Warning: duplicate indices with non-duplicate data (keeping first)!')
    return df[~df.index.duplicated(keep='first')]
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

    constructor = L1_constructor(site=site)
    constructor.write_to_excel()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def resample_dataframe(df, resample_to):

    return df.resample(resample_to).interpolate()
#------------------------------------------------------------------------------