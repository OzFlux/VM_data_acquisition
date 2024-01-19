#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh

"""

### Standard modules ###
import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import pathlib
import sys

### Custom modules ###
import file_handler as fh
import file_io as io
import met_functions as mf
import paths_manager as pm
import variable_mapper_2 as vm
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
import sparql_site_details as sd

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

PATHS = pm.paths()
SITE_DETAILS = sd.site_details()
# table_df = vm.make_table_df()

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------



###############################################################################
### BEGIN FILE MERGER CLASS ###
###############################################################################


class FileMerger():

    #--------------------------------------------------------------------------
    def __init__(self, site):
        """
        Initialise class with variable map.

        Parameters
        ----------
        site : str
            Site to parse.

        Returns
        -------
        None.

        """

        self.var_map = vm.mapper(site=site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_and_headers(self, file, write_concatenation_report=False):
        """


        Parameters
        ----------
        file : str
            Name of file for which to get data.
        write_concatenation_report : bool, optional
            Whether to write the outcome of bacup concatenation to the file
            directory. The default is False.

        Raises
        ------
        FileNotFoundError
            Raised if file does not exist.

        Returns
        -------
        data : pd.core.frame.DataFrame
            Dataframe containing data with names converted to standard names,
            units converted to standard units and broad range limits applied
            (NOT a substitute for QC, just an aid for plotting).
        headers : pd.core.frame.DataFrame
            Dataframe containing headers with names converted to standard names,
            units converted to standard units.

        """

        # Check the file exists, and get the handler
        if not file in self.var_map.get_file_list():
            raise FileNotFoundError('File not listed!')
        handler = fh.DataHandler(
            file=self.var_map.path / file, concat_files=True
            )

        # If requested, write the results of the concatenation report to file
        if write_concatenation_report:
            self._write_concatenation_report(handler=handler)


        # Get the dictionary to map site anmes to standard names
        translation_dict = self.var_map.map_variable_translation(file=file)

        # Use handler, variable map and met functions to translate
        # data and apply conversions
        data = (
            handler.get_conditioned_data(
                usecols=translation_dict,
                monotonic_index=True,
                drop_non_numeric=True,
                resample_intvl='30T'
                )
            .pipe(
                self._do_unit_conversions,
                conversion_frame=self.var_map.get_variables_to_convert(
                    source_field='translation_name', file=file
                    )
                )
            )

        # Use handler to translate headers
        headers = handler.get_conditioned_headers(
            usecols=translation_dict,
            drop_non_numeric=True
            )

        return data, headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_all(self, timestamp_as_var=True):
        """
        Merge all to form a single data df and a single header df

        Parameters
        ----------
        timestamp_as_var : bool, optional
            If true, output the data with a 'TIMESTAMP' entyr in the dataframe
            and the timestamp inserted into the dataframe as a column.
            If false, it remains as dataframe index and does not appear in the
            headers. The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            The merged data with missing data inserted and range limits applied.
        pd.core.frame.DataFrame
            The corresponding headers data.

        """

        # Iterate through the file list and get data and headers
        data_list, headers_list = [], []
        for file in self.var_map.get_file_list():

            data, headers = self.get_data_and_headers(file=file)
            data_list.append(data)
            headers_list.append(headers)

        # Concatenate the data and header lists, then construct_missing
        # variables
        data, headers = self._construct_missing_variables(
            data=pd.concat(data_list, axis=1),
            headers=pd.concat(headers_list),
            )

        # Apply limits
        self._apply_limits(data)

        # Return data and headers without the index in the df and headers
        if not timestamp_as_var:
            return data, headers

        # Return data and headers with the index in the df and headers
        return (
            self._convert_table_index(data=data),
            self._convert_headers(headers=headers)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_all_and_write(self, abs_file_path=None):
        """
        Call the merge method and write the output to file.

        Parameters
        ----------
        abs_file_path : str or pathlib.Path, optional
            The absolute path to the file to write. If none, the file is
            written to the data host directory. The default is None.

        Returns
        -------
        None.

        """

        data, headers = self.merge_all()
        if abs_file_path is None:
            abs_file_path = (
                self.var_map.path / f'{self.var_map.site}_merged_test.dat'
                )
            io.write_data_to_file(
                headers=headers,
                data=data,
                abs_file_path=abs_file_path
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _write_concatenation_report(self, handler):
        """
        Write the concatenation report if requested.

        Parameters
        ----------
        handler : handler.
            the data handler object.

        Returns
        -------
        None.

        """

        handler.write_concatenation_report(
            abs_file_path=self.var_map.path /
            f'merge_report_{self.var_map.path}.txt'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_unit_conversions(self, data, conversion_frame):
        """
        Convert from site-based to standard units.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            data to be converted.
        conversion_frame : pd.core.frame.DataFrame
            Dataframe containing the information for conversion.

        Returns
        -------
        data : pd.core.frame.DataFrame
            The converted data.

        """

        for variable in conversion_frame.index:
            from_units=conversion_frame.loc[variable, 'site_units']
            conversion_var = conversion_frame.loc[variable, 'standard_name']
            conversion_func = mf.convert_variable(conversion_var)
            data[variable] = data[variable].apply(
                conversion_func, from_units=from_units
                )
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _construct_missing_variables(self, data, headers):
        """
        Generate any missing variables (where possible) from existing variables.
        Where not possible, insert NaN.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.
        headers : pd.core.frame.DataFrame
            The headers.

        Returns
        -------
        data : pd.core.frame.DataFrame
            The amended data.
        headers : pd.core.frame.DataFrame
            The amended headers.

        """

        missing_variables = self.var_map.get_missing_variables()
        for variable in missing_variables:
            data[variable] = (
                mf.calculate_variable_from_std_frame(variable=variable, df=data)
                )
            headers.loc[variable] = {
                'units':
                    self.var_map.get_variable_attributes(
                        variable=variable, source_field='translation_name'
                        ).standard_units
                    }
        return data, headers
    #--------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    def _convert_headers(self, headers):
        """
        Insert the 'TIMESTAMP' variable and units into the headers.

        Parameters
        ----------
        headers : pd.core.frame.DataFrame
            The header data.

        Returns
        -------
        pd.core.frame.DataFrame
            The modified headers.

        """

        return pd.concat([
            pd.DataFrame(
                data=[['TS', '']],
                index=pd.Index(['TIMESTAMP'], name='variable'),
                columns=['units', 'sampling']
                ),
            headers
            ])
    #------------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _convert_table_index(self, data):
        """
        Convert the data so that the index becomes the 'TIMESTAMP' variable.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data for conversion.

        Returns
        -------
        pd.core.frame.DataFrame
            Converted data.

        """

        return data.reset_index().rename({'DATETIME': 'TIMESTAMP'}, axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, data):
        """
        Apply range limits based on the maxima and minima documented in the
        variable map.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.

        Returns
        -------
        None.

        """

        for variable in data.columns:
            attrs = self.var_map.get_variable_attributes(
                variable=variable,
                source_field='translation_name'
                )
            filter_bool = (
                (data[variable] < attrs.Min.item()) |
                (data[variable] > attrs.Max.item())
                )
            data.loc[filter_bool, variable] = np.nan
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END FILE MERGER CLASS ###
###############################################################################



###############################################################################
### BEGIN L1 GENERATOR CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1Constructor():

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
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )
        self.table_df = vm.make_table_df(site=site)
        if not site == 'Tumbarumba':
            self.time_step = int(SITE_DETAILS.get_single_site_details(
                site=site, field='time_step'
                ))
        else:
            self.time_step = 30

    #--------------------------------------------------------------------------
    def get_inclusive_date_index(self, start_override=None):
        """
        Get an index that spans the earliest and latest dates across all files
        to be included in the collection.
        Note: some data e.g. soil loggers do not have the same time step as the
        eddy covariance logger. This complicates the acquisition of a universal
        time index, because the start and/or end times may not match the
        expected time step for the site e.g. 15 minute files may have :15 or
        :45 minute start / end times. We therefore force the start and end
        timestamps of all parsed files to conform to the site time interval.

        Returns
        -------
        pandas.core.indexes.datetimes.DatetimeIndex
            Index with correct (site-specific) time step.

        """

        dict_list = []
        for file in self.table_df.full_path:
            file_list = [file] + io.get_eligible_concat_files(file=file)
            for sub_file in file_list:
                dates = io.get_start_end_dates(file=sub_file)
                mnt_remain = np.mod(dates['start_date'].minute, self.time_step)
                if mnt_remain:
                    dates['start_date'] += dt.timedelta(
                        minutes=int(self.time_step - mnt_remain)
                        )
                mnt_remain = np.mod(dates['end_date'].minute, self.time_step)
                if mnt_remain:
                    dates['end_date'] -= dt.timedelta(
                        minutes=int(mnt_remain)
                        )
                dict_list.append(dates)
        return pd.date_range(
            start=np.array([x['start_date'] for x in dict_list]).min(),
            end=np.array([x['end_date'] for x in dict_list]).max(),
            freq=f'{self.time_step}T'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_excel(self, concat_backups=True, na_values=''):
        """
        Write table files out to separate tabs in an xlsx file.

        Parameters
        ----------
        concat_backups : bool, optional
            Whether to concatenate backup files residing in same directory.
            The default is True.
        na_values : str, optional
            The na values to fill nans with. The default is ''.

        Returns
        -------
        None.

        """

        # Set the destination
        dest = self.path / f'{self.site}_L1.xlsx'

        # Get inclusive date index to reindex all files to
        date_idx = self.get_inclusive_date_index()

        # Iterate over all files and write to separate excel workbook sheets
        with pd.ExcelWriter(path=dest) as writer:
            for file in self.table_df.full_path:

                # Name the tab after the file (drop the dat) - it is necessary
                # to use file name and not just the table name, because for
                # some sites data is drawn from different loggers with same
                # table name
                sheet_name = file.stem

                # Get the data handler (concatenate backups by default)
                handler = fh.DataHandler(
                    file=file, concat_files=concat_backups
                    )

                # Write info
                (pd.DataFrame(handler.file_info.values())
                 .T
                 .to_excel(
                     writer, sheet_name=sheet_name, header=False, index=False,
                     startrow=0
                     )
                 )

                # Write header
                (handler.headers
                 .reset_index()
                 .T
                 .to_excel(
                     writer, sheet_name=sheet_name, header=False, index=False,
                     startrow=1
                     )
                 )

                # Write data
                (
                    handler.get_conditioned_data(
                        resample_intvl=f'{int(self.time_step)}T'
                        )
                    .reindex(date_idx)
                    .to_excel(
                        writer, sheet_name=sheet_name, header=False,
                        index=False, startrow=4, na_rep=na_values
                        )
                    )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END L1 GENERATOR CLASS ###
###############################################################################


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

    logging.info('Generating site details file')

    # Get the site info
    details = SITE_DETAILS.get_single_site_details(site=site).copy()

    # Get the name of the flux file
    mapper = vm.mapper(site=site)
    flux_file = mapper.get_flux_file(abs_path=True)

    # Get the EC logger info
    logger_info = (
        mapper.get_flux_file_attributes()
        [['station_name', 'logger_type', 'serial_num', 'OS_version',
         'program_name']]
        )

    # Get the pct missing data
    handler = handler=fh.DataHandler(file=flux_file)
    missing = pd.Series(
        {'pct_missing': handler.get_missing_records()['%_missing']}
        )

    # Build additional details
    midnight_time = dt.datetime.combine(
        dt.datetime.now().date(), dt.datetime.min.time()
        )
    time_getter = mf.TimeFunctions(
        lat=details.latitude, lon=details.longitude, elev=details.elevation,
        date=midnight_time)
    extra_details = pd.Series({
        'start_year': str(details.date_commissioned.year),
        'sunrise': time_getter.get_next_sunrise().strftime('%H:%M'),
        'sunset': time_getter.get_next_sunset().strftime('%H:%M'),
        '10Hz_file': get_latest_10Hz_file(site=site)
        })

    # Make the data
    data = (
        pd.concat([
            pd.Series({'TIMESTAMP': midnight_time}),
            details,
            extra_details,
            logger_info,
            missing
            ])
        .to_frame()
        .T
        )

    # Make the headers
    headers = pd.DataFrame(
        data={'Units': 'unitless', 'Samples': 'Smp'},
        index=pd.Index(data.columns, name='variables')
        )
    headers.loc['TIMESTAMP']=['TS', '']

    # Make the info
    info = dict(zip(
        io.INFO_FIELDS,
        ['TOA5', site, 'CR1000', '9999', 'cr1000.std.99.99',
         'CPU:noprogram.cr1', '9999', 'site_details']
        ))

    # Set the output path
    output_path = (
        PATHS.get_local_path(resource='site_details') / f'{site}_details.dat'
        )

    # Write to file
    io.write_data_to_file(
        headers=headers, data=data, abs_file_path=output_path, info=info)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteDataParser():
    """
    This class parses the data for a given site, based on information in the
    underlying variable map (accessed as var_map attribute). Since this is a
    high-level class sitting on lower-level classes, it uses ONLY file names
    (not absolute path) and ONLY translation names (not site names).
    """

    def __init__(self, site, concat_files=True):
        """
        Initiate the data parser.

        Parameters
        ----------
        site : str
            Site to parse.

        Returns
        -------
        None.

        """

        self.site = site
        self.var_map = vm.mapper(site=site)
        self._site_time_offset =  dt.timedelta(
             hours=
             10 - SITE_DETAILS.get_single_site_details(site, 'UTC_offset')
             )
        self.concat_files = concat_files

    ###########################################################################
    ### METHODS BY FILE-BASED QUERY ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def get_record_stats_by_file(self, file='flux'):
        """
        Get the statistics for the file records.

        Parameters
        ----------
        file : str, optional
            Either the name of the file (no absolute path) or the magic word
            'flux'. The default is 'flux'.

        Returns
        -------
        pd.core.frame.DataFrame
            The statistics.

        """

        file = self._parse_file_name(file=file)
        site_time = dt.datetime.now() - self._site_time_offset
        sub_series = self.var_map.get_file_attributes(file=file)
        handler = fh.DataHandler(
            sub_series.full_path, concat_files=self.concat_files
            )
        rslt_dict = handler.get_missing_records()
        rslt_dict.pop('gap_distribution')
        rslt_dict.update(
            {'duplicate_records':
                 len(handler.get_duplicate_records(as_dates=True)),
             'duplicate_indices':
                 len(handler.get_duplicate_indices(as_dates=True)),
             'days_since_last_record': (
                 (site_time - handler.data.index[-1].to_pydatetime())
                 .days
                 )
             }
            )
        return pd.concat([
            sub_series.drop(['full_path', 'format']),
            pd.Series(rslt_dict)
            ])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_file(
            self, file='flux', rng_chk=True, convert=True, incl_headers=False
            ):
        """
        Get the data for all mapped variables in the file.

        Parameters
        ----------
        file : str, optional
            Either the name of the file (no absolute path) or the magic word
            'flux'. The default is 'flux'.
        rng_chk : Bool, optional
            Whether to apply the relevant range checks
            (maxima and minima sourced from the variable map).
            The default is True.
        convert : Bool, optional
            Whether to convert from site to standard units. The default is True.

        Returns
        -------
        pd.core.frame.DataFrame
            The data.

        """

        file = self._parse_file_name(file=file)
        return (
            self.get_data_by_variable(
                variable_list=self.var_map.site_df.loc[
                    (~self.var_map.site_df.Missing) &
                    (self.var_map.site_df.file_name==file),
                    'translation_name'
                    ],
                rng_chk=rng_chk,
                convert=convert
                )
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _parse_file_name(self, file):
        """
        Utility function to check / retrieve the correct file name.

        Parameters
        ----------
        file : str, optional
            Either the name of the file (no absolute path) or the magic word
            'flux'. The default is 'flux'.

        Raises
        ------
        FileNotFoundError
            Raised if file arg is not either the magic word 'flux' or a file
            that is in the variable map.

        Returns
        -------
        str
            The name of the file.

        """

        if file == 'flux':
            return self.var_map.get_flux_file()
        if not file in self.var_map.site_df.file_name:
            raise FileNotFoundError('File not documented in variable map!')
        return file
    #--------------------------------------------------------------------------

    ###########################################################################
    ### METHODS BY VARIABLE-BASED QUERY ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def get_data_by_variable(
            self, variable_list=None, rng_chk=True, convert=True,
            fill_missing=True, incl_headers=False
            ):
        """
        Get data for a given list of variables

        Parameters
        ----------
        variable_list : list or NoneType, optional
            If None, returns all variables defined in the variable map.
            The default is None.
        rng_chk : Bool, optional
            Whether to apply the relevant range checks
            (maxima and minima sourced from the variable map).
            The default is True.
        convert : Bool, optional
            Whether to convert from site to standard units. The default is True.

        Returns
        -------
        data : pd.core.frame.DataFrame
            The data.

        """

        # Get data with or without headers
        which = 'all' if incl_headers else 'data'
        data_etc = self._get_something_by_variable(
            variable_list=variable_list, which=which
            )

        # Split the data out of tuple if headers are included
        if not incl_headers:
            data = data_etc
        else:
            data = data_etc[0]
            headers = data_etc[1]

        # Concatenate the data and apply conversion / range checks if requested
        if convert:
            self._do_unit_conversions(data=data)
        if fill_missing:
            self._construct_missing_variables(data=data)
        if rng_chk:
            self._apply_limits(data=data)

        # Return the things
        if not incl_headers:
            return data
        self._amend_header_units(headers=headers, convert=convert)
        return data, headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_something_by_variable(self, variable_list=None, which='data'):
        """
        Get data for a given list of variables

        Parameters
        ----------
        variable_list : list or NoneType, optional
            If None, returns all variables defined in the variable map.
            The default is None.

        Returns
        -------
        data : pd.core.frame.DataFrame
            The data.

        """

        # Check inputs
        allowed_inputs = ['data', 'headers', 'all']
        if not which in allowed_inputs:
            raise KeyError(
                f'"which" kwarg must be on of {", ".join(allowed_inputs)}'
                )

        # If a list of variables is not passed, set the variable list to all
        # variables defined in the variable map.
        if variable_list is None:
            variable_list = (
                self.var_map.site_df.loc[
                    ~self.var_map.site_df.Missing,
                    'translation_name']
                .tolist()
                )

        # Group variables by file name to minimise file access operations
        grp_obj = (
            self.var_map.site_df
            .reset_index()
            .set_index(keys='translation_name')
            .loc[variable_list]
            .groupby('file_name')
            )

        # Iterate through file objects, and append data / headers
        data_list, header_list = [], []
        for item in grp_obj:
            translation_dict = (
                item[1]
                .reset_index()
                .set_index(keys='site_name')
                .translation_name
                .to_dict()
                )
            handler = fh.DataHandler(
                file=self.var_map.path / item[0],
                concat_files=self.concat_files
                )
            if which == 'data' or 'all':
                data_list.append(
                    handler.get_conditioned_data(
                        usecols=translation_dict, drop_non_numeric=True
                        )
                    )
            if which == 'headers' or 'all':
                header_list.append(
                    handler.get_conditioned_headers(
                        usecols=translation_dict, drop_non_numeric=True
                        )
                    )

        # Return the request
        if which == 'data':
            return pd.concat(data_list, axis=1)
        if which == 'headers':
            return pd.concat(header_list)
        if which == 'all':
            return pd.concat(data_list, axis=1), pd.concat(header_list)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_record_stats_by_variable(self, variable_list=None):
        """
        Get the statistics for variables.

        Parameters
        ----------
        variable_list : list or NoneType, optional
            If None, returns all variables defined in the variable map.
            The default is None.

        Returns
        -------
        pd.core.frame.DataFrame
            The statistics.

        """

        data = self.get_data_by_variable(variable_list=variable_list)
        site_time = dt.datetime.now() - self._site_time_offset
        rslt_list = []
        for variable in data.columns:
            series = data[variable].dropna()
            last_valid_date_time = site_time - series.index[-1]
            last_valid_date_time = series.index[-1]
            days = (site_time - last_valid_date_time).days
            rslt_dict = {
                'last_valid_record': last_valid_date_time.strftime(
                    '%Y-%m-%d %H:%M'
                    ),
                 'value': series.iloc[-1],
                 'days_since_last_valid_record': int(days)
                }
            rslt_list.append(rslt_dict)
        return pd.DataFrame(rslt_list, index=data.columns)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, data):
        """
        Apply range limits based on the maxima and minima documented in the
        variable map.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.

        Returns
        -------
        None.

        """

        for variable in data.columns:
            attrs = self.var_map.get_variable_attributes(
                variable=variable,
                source_field='translation_name'
                )
            filter_bool = (
                (data[variable] < attrs.Min.item()) |
                (data[variable] > attrs.Max.item())
                )
            data.loc[filter_bool, variable] = np.nan
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_unit_conversions(self, data):
        """
        Convert from site-based to standard units.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            data to be converted.

        Returns
        -------
        data : pd.core.frame.DataFrame
            The converted data.

        """

        for variable in data.columns:
            attrs = self.var_map.get_variable_attributes(
                variable=variable,
                source_field='translation_name'
                )
            if attrs.conversion:
                conversion_func = mf.convert_variable(
                    variable=variable
                    )
                data[variable] = data[variable].apply(
                    conversion_func, from_units=attrs.site_units
                    )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _amend_header_units(self, headers, convert=True, add_missing=True):

        units = 'site_units' if not convert else 'standard_units'
        headers.units = (
            self.var_map.site_df
            .set_index(keys='translation_name')
            .loc[headers.index]
            [units]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _construct_missing_variables(self, data):

        """
        Generate any missing variables (where possible) from existing variables.
        Where not possible, insert NaN.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.

        Returns
        -------
        None.

        """

        missing_vars = self.var_map.get_missing_variables()
        if not missing_vars:
            return
        for var in missing_vars:
            data[var] = self.construct_variable(variable=var, data=data)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_variable(self, variable, data=None, raise_error=False):

        req_vars = mf.input_vars[variable]
        if data is None:
            data = self.get_data_by_variable(variable_list=req_vars)
        input_args = {arg: data[arg] for arg in req_vars}
        func = mf.get_function(variable=variable)
        return func(**input_args)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteStatusParser():

    def __init__(self, site):

        self.site = site
        self.run_date_time = dt.datetime.now()
        self.var_map = vm.mapper(site=site)
        self._site_time_offset = dt.timedelta(
             hours=
             10 - SITE_DETAILS.get_single_site_details(site, 'UTC_offset')
             )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_summary(self, file):

        site_time = dt.datetime.now() - self._site_time_offset
        sub_series = self.var_map.get_file_attributes(file=file)
        handler = fh.DataHandler(sub_series.full_path)
        rslt_dict = handler.get_missing_records()
        rslt_dict.pop('gap_distribution')
        rslt_dict.update(
            {'duplicate_records':
                 len(handler.get_duplicate_records(as_dates=True)),
             'duplicate_indices':
                 len(handler.get_duplicate_indices(as_dates=True)),
             'days_since_last_record': (
                 (site_time - handler.data.index[-1].to_pydatetime())
                 .days
                 )
             }
            )
        return pd.concat([
            sub_series.drop(['full_path', 'format']),
            pd.Series(rslt_dict)
            ])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_latest_10Hz_file(self):

        return get_latest_10Hz_file(site=self.site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_all_files_summary(self):

        df = pd.concat(
            [self.get_file_summary(file=file) for file in self.table_df.index],
            axis=1
            )
        df.columns = self.table_df.index
        return df.T
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_summary(self):
        """
        Get the variable information for a site.

        Parameters
        ----------
        site : str
            The site.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe.

        """

        site_time = dt.datetime.now() - self._site_time_offset
        file_merger = FileMerger(site=self.site)
        df = file_merger.merge_all(timestamp_as_var=False)[0]
        rslt_list = []
        for variable in df.columns:
            series = df[variable].dropna()
            var_details = self.var_map.get_variable_attributes(
                variable=variable, source_field='translation_name'
                )
            logger = var_details.logger_name
            table = var_details.table_name
            try:
                np.isnan(logger)
                logger = 'Calculated'
                np.isnan(table)
                table = 'Calculated'
            except TypeError:
                pass
            rslt_dict = {'variable': variable, 'station': logger, 'table': table}
            try:
                last_valid_date_time = series.index[-1]
                days = (site_time - last_valid_date_time).days
                rslt_dict.update(
                    {'last_valid_record': last_valid_date_time.strftime(
                        '%Y-%m-%d %H:%M'
                        ),
                     'value': series.iloc[-1],
                     'days_since_last_valid_record': int(days)}
                    )
            except IndexError:
                rslt_dict.update(
                    {'last_valid_record': None, 'value': None,
                     'days_since_last_record': None}
                    )
            rslt_list.append(rslt_dict)
        return pd.DataFrame(rslt_list)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class DataStatusConstructor():
    """
    Class for retrieving, formatting and outputting data status to file.
    """

    def __init__(self):
        """
        Set critical attributes.

        Returns
        -------
        None.

        """

        index_vars = ['site', 'station_name', 'table_name']
        self.run_date_time = dt.datetime.now()
        self.table_df = (
            vm.make_table_df()
            .reset_index()
            .set_index(keys=index_vars)
            .sort_index()
            )
        self.site_list = (
            self.table_df.index.get_level_values(level='site').unique().tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_data_summary(self, site):
        """
        Get the variable information for a site.

        Parameters
        ----------
        site : str
            The site.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe.

        """

        return SiteStatusParser(site=site).get_data_summary()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_10Hz_table(self):
        """
        Get a summary of most recent 10Hz data.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe.

        """

        files, days = [], []
        for site in self.site_list:
            file = get_latest_10Hz_file(site)
            files.append(file)
            try:
                days.append(
                    (self.run_date_time -
                     dt.datetime.strptime(
                         '-'.join(file.split('.')[0].split('_')[-3:]), '%Y-%m-%d'
                         )
                     )
                    .days - 1
                    )
            except ValueError:
                days.append(None)
        return pd.DataFrame(
            zip(self.site_list, files, days),
            columns=['site', 'file_name', 'days_since_last_record']
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_summary_table(self):
        """
        Get the basic file data PLUS data quality / holes / duplicates etc.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe.

        """

        ref_dict = dict(zip(
            self.table_df.full_path,
            self.table_df.index.get_level_values(level='site')
            ))
        data_list = []
        for file in ref_dict:
            site = ref_dict[file]
            site_time = self._get_site_time(site=site)
            handler = fh.DataHandler(file=file)
            rslt_dict = handler.get_missing_records()
            rslt_dict.pop('gap_distribution')
            rslt_dict.update(
                {'duplicate_records':
                     len(handler.get_duplicate_records(as_dates=True)),
                 'duplicate_indices':
                     len(handler.get_duplicate_indices(as_dates=True)),
                 'days_since_last_record': (
                     (site_time - handler.data.index[-1].to_pydatetime())
                     .days
                     )
                 }
                )
            data_list.append(rslt_dict)
        return pd.concat(
            [self.table_df.drop(['full_path', 'file_name', 'format'], axis=1),
             pd.DataFrame(data_list, index=self.table_df.index)],
            axis=1
            ).reset_index()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_all_sites_summary(self):

        return (
            pd.concat(
                self.get_site_summary(site=site) for site in
                self.table_df.index.get_level_values(level='site').unique()
                )
            .reset_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_file_summary(self, site, file):

        sub_df = self.table_df.loc[self.table_df.file_name==file]
        site_time = self._get_site_time(site=site)
        handler = fh.DataHandler(sub_df.full_path.item())
        rslt_dict = handler.get_missing_records()
        rslt_dict.pop('gap_distribution')
        rslt_dict.update(
            {'duplicate_records':
                 len(handler.get_duplicate_records(as_dates=True)),
             'duplicate_indices':
                 len(handler.get_duplicate_indices(as_dates=True)),
             'days_since_last_record': (
                 (site_time - handler.data.index[-1].to_pydatetime())
                 .days
                 )
             }
            )
        return pd.concat(
                [sub_df.drop(['full_path', 'file_name', 'format'], axis=1),
                 pd.DataFrame(rslt_dict, index=sub_df.index)],
                axis=1
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_summary(self, site):

        return pd.concat(
            self.get_site_file_summary(site=site, file=file)
            for file in self.table_df.loc[site, 'file_name'].tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_key_details(self):
        """
        Get the dataframe that maps the interval (in days) between run time and
        last data.

        Returns
        -------
        pd.core.frame.DataFrame
            The dataframe.

        """

        colours = ['green', 'yellow', 'orange', 'red']
        intervals = ['< 1 day', '1 <= day < 3', '3 <= day < 7', 'day >= 7']
        return (
            pd.DataFrame([colours, intervals], index=['colour', 'interval'])
            .T
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_excel(
            self, dest='E:/Network_documents/Status/site_to_VM_status.xlsx'
            ):
        """
        Write all data to excel spreadsheet

        Parameters
        ----------
        dest : str, optional
            Absolute path to write. The default is 'E:/Sites/site_status.xlsx'.

        Returns
        -------
        None.

        """

        # Get the non-iterated outputs
        iter_dict = {
            'Summary': self.get_all_sites_summary(),
            '10Hz_files': self.get_10Hz_table()
            }
        key_table = self.get_key_details()

        # Write sheets
        with pd.ExcelWriter(path=dest) as writer:

            # For the Summary and 10Hz_files tables...
            for item in iter_dict:

                # Set sheet name
                sheet_name = item

                # Prepend the run date and time to the spreadsheet
                self._write_time_frame(xl_writer=writer, sheet=sheet_name)

                # Output and format the results
                (
                    iter_dict[item].style.apply(self._get_style_df, axis=None)
                    .to_excel(
                        writer, sheet_name=sheet_name, index=False, startrow=1
                        )
                    )
                self._set_column_widths(
                    df=iter_dict[item], xl_writer=writer, sheet=sheet_name
                    )

            # Iterate over sites...
            for site in self.site_list:

                # Set sheet name
                sheet_name = site

                # Prepend the run date and time to the spreadsheet
                self._write_time_frame(
                    xl_writer=writer, sheet=sheet_name, site=site
                    )

                # Output and format the results
                site_df = self.build_site_variable_data(site=site)
                (
                    site_df.style.apply(self._get_style_df, axis=None)
                    .to_excel(
                        writer, sheet_name=sheet_name, index=False, startrow=1
                        )
                    )
                self._set_column_widths(
                    df=site_df, xl_writer=writer, sheet=site
                    )

            # Output the colour key

            # Set sheet name
            sheet_name = 'Key'

            # Output and format the results
            (
                key_table.style.apply(self._get_key_formatter, axis=None)
                .to_excel(writer, sheet_name=sheet_name, index=False)
                )
            self._set_column_widths(
                df=key_table, xl_writer=writer, sheet=sheet_name
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_style_df(self, df, column_name='days_since_last_record'):
        """
        Generate a style df of same dimensions, index and columns, with colour
        specifications in the appropriate column.

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            the dataframe.
        column_name : str, optional
            The column to parse for setting of colour. The default is
            'days_since_last_record'.

        Returns
        -------
        style_df : pd.core.frame.DataFrame
            Formatter dataframe containing empty strings for all non-coloured
            cells and colour formatting for coloured cells.

        """

        style_df = pd.DataFrame('', index=df.index, columns=df.columns)
        style_df[column_name] = df[column_name].apply(_get_colour, xl_format=True)
        return style_df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_key_formatter(self, df):
        """
        Return a dataframe that can be used to format the key spreadsheet.

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            The dataframe to return the formatter for.

        Returns
        -------
        pd.core.frame.DataFrame
            The formatted dataframe.

        """

        this_df = df.copy()
        this_df.loc[:, 'colour']=[0,2,5,7]
        return self._get_style_df(df=this_df, column_name='colour')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_time(self, site):
        """
        Correct server run time to local standard time.

        Parameters
        ----------
        site : str
            Site for which to return local standard time.

        Returns
        -------
        dt.datetime.datetime
            Local standard time equivalent of server run time.

        """

        return (
            self.run_date_time -
            dt.timedelta(
                hours=
                10 -
                SITE_DETAILS.get_single_site_details(site, 'UTC_offset')
                )
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _write_time_frame(self, xl_writer, sheet, site=None):
        """
        Write the time to the first line of the output spreadsheet.

        Parameters
        ----------
        xl_writer : TYPE
            The xlwriter object.
        sheet : str
            Name of the spreadsheet.
        site : str, optional
            Name of site, if passed. The default is None.

        Returns
        -------
        None.

        """

        # Return server time if site not passed,
        # otherwise get local standard site time
        if not site:
            use_time = self.run_date_time
            zone = 'AEST'
        else:
            use_time = self._get_site_time(site=site)
            zone = ''
        frame = pd.DataFrame(
            ['RUN date/time: '
             f'{use_time.strftime("%Y-%m-%d %H:%M")} {zone}'
             ],
            index=[0]
            ).T

        frame.to_excel(
            xl_writer, sheet_name=sheet, index=False, header=False
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _set_column_widths(self, df, xl_writer, sheet, add_space=2):
        """
        Set the column widths for whatever is largest (header or largest
                                                       content).

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            The frame for which to set the widths.
        xl_writer : TYPE
            The xlwriter object.
        sheet : str
            Name of the spreadsheet.
        add_space : int, optional
            The amount of extra space to add. The default is 2.

        Returns
        -------
        None.

        """

        alt_list = ['backups']

        for i, column in enumerate(df.columns):

            # Parse columns with multiple comma-separated values differently...
            # (get the length of the largest string in each cell)
            if column in alt_list:
                col_width = (
                    df[column].apply(
                        lambda x: len(max(x.split(','), key=len))
                        )
                    .max()
                    )
            # Otherwise just use total string length
            else:
                col_width = max(
                    df[column].astype(str).map(len).max(),
                    len(column)
                    )
            xl_writer.sheets[sheet].set_column(i, i, col_width + add_space)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_colour(n, xl_format=False):
    """
    Get the formatted colour based on value of n.

    Parameters
    ----------
    n : int or float
        Number.

    Returns
    -------
    str
        Formatted colour for pandas styler.

    """

    if np.isnan(n):
        return ''
    if n < 1:
        colour = 'green'
    if 1 <= n < 3:
        colour = 'blue'
    if 3 <= n < 5:
        colour = 'magenta'
    if 5 <= n < 7:
        colour = 'orange'
    if n >= 7:
        colour = 'red'
    if xl_format:
        return f'background-color: {colour}'
    return colour
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_time(site, time):
    """
    Correct server run time to local standard time.

    Parameters
    ----------
    site : str
        Site for which to return local standard time.

    Returns
    -------
    dt.datetime.datetime
        Local standard time equivalent of server run time.

    """

    return (
        time -
        dt.timedelta(
            hours=
            10 -
            SITE_DETAILS.get_single_site_details(site, 'UTC_offset')
            )
        )
#------------------------------------------------------------------------------
