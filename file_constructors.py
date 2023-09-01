#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh

Note - the merge function needs to be checked to ensure that the mergin of data
and headers is being done safely! It probably needs refactoring, since is is
not clear how this is happening!
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
import met_functions as mf
import paths_manager as pm
import toa5_handler as toa5
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
class TableMerger():

    def __init__(self, site, concat_backups=True):
        """
        Class to generate a standardised dataset from source tables; note -
        currently contains a hack to stop Tumbarumba being processed in
        60-minute blocks (but it should be - seems raw data from the Campbell
                          system is only running at 30)

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
        self.concat_backups = concat_backups
        self.table_map = vm.make_table_df(site=site)
        self.var_map = vm.mapper(site=site)

    #--------------------------------------------------------------------------
    def get_table_data(self, file):
        """
        Get the data for a particular table

        Parameters
        ----------
        file : str
            Name of file for which to get data.

        Returns
        -------
        df : pd.core.frame.DataFrame
            Dataframe containing data with names converted to standard names,
            units converted to standard units and broad range limits applied
            (NOT a substitute for QC, just an aid for plotting).

        """

        # Get the handler and the name conversion scheme (as dict)
        handler = toa5.get_file_handler(
            file=self.path / file, concat_backups=self.concat_backups
            )

        # Pull in the data and rename it (passing the dictionary containing the
        # name mapping will automatically rename it - see toa5_handler)
        df = (
            handler.get_conditioned_data(
                usecols=self.var_map.get_translation_dict(table_file=file),
                monotonic_index=True,
                resample_intvl=f'{self.time_step}T'
                )
            )

        # Apply unit conversion and range limits, and return
        self._do_unit_conversions(df=df)
        self._apply_limits(df=df)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_unit_conversions(self, df):
        """
        Convert from site-specific measurement units to network standard.

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            Dataframe to which to apply conversions.

        Returns
        -------
        None.

        """

        # Get the list of variables requiring conversion for iteration
        converts_df = self.var_map.get_conversion_variables()

        # Do the conversions if any (get conversion functions from met_functions.py)
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
        """
        Apply range limits based on those specified in the mapping spreadsheet.

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            Dataframe to which to apply limits.

        Returns
        -------
        None.

        """

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
        """
        Pull together variables from different tables, align to encapsulating
        datetimeindex (empty data is NaN) and add requisite variables that are
        missing.

        Returns
        -------
        df : pd.core.frame.DataFrame
            Dataframe containing merged files.

        """

        df_list = []
        for file in self.var_map.get_file_list():
            df_list.append(self.get_table_data(file=file))
        date_idx = pd.date_range(
            start = np.array([x.index[0].to_pydatetime() for x in df_list]).min(),
            end = np.array([x.index[-1].to_pydatetime() for x in df_list]).max(),
            freq=f'{self.time_step}T'
            )
        date_idx.name = 'TIMESTAMP'
        df =  pd.concat(
            [this_df.reindex(date_idx) for this_df in df_list],
            axis=1
            )
        self._make_missing_variables(df=df)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_missing_variables(self, df):
        """
        Use externally-called meteorological functions to create missing
        variables.

        Parameters
        ----------
        df : pd.core.frame.DataFrame
            Dataframe containing the required variables to generate those missing.

        Returns
        -------
        None.

        """

        missing_df = self.var_map.get_missing_variables()
        for variable in missing_df.translation_name:
            df[variable] = (
                mf.calculate_variable_from_std_frame(variable=variable, df=df)
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_headers(self):
        """
        Merge the headers of the separate data tables.

        Returns
        -------
        pd.core.frame.DataFrame
            A dataframe of headers, units and sampling.

        """

        df_list = [
            self.translate_header(file)
            for file in self.var_map.get_file_list()
            ]
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
        """
        Translate the header variable names from what is in the raw files to
        converted names and units.

        Parameters
        ----------
        file : str
            Name of file for which to retrieve translated header.

        Returns
        -------
        pd.core.frame.DataFrame
            DESCRIPTION.

        """

        headers_df = toa5.get_file_handler(
            file=self.path / file, concat_backups=self.concat_backups
            ).headers
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
        """
        Create an output file that looks like a TOA5 file.

        Returns
        -------
        None.

        """

        info_header = [
            'TOA5', self.site, 'CR1000', '9999', 'cr1000.std.99.99',
            'CPU:noprogram.cr1', '9999', 'merged']
        data = self.merge_tables()
        header = self.merge_headers()
        constructor = toa5.TOA5_file_constructor(
            data=data, info_header=info_header, units=header.units.tolist(),
            samples=header.sampling.tolist()
            )
        constructor.write_output_file(dest=self.path / f'{self.site}_merged_std.dat')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

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
        if not site == 'Tumbarumba':
            self.time_step = int(SITE_DETAILS.get_single_site_details(
                site=site, field='time_step'
                ))
        else:
            self.time_step = 30
        self.tables_df = vm.make_table_df(site=site)

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

        dict_list = []
        for file in self.tables_df.index:
            file_list = [file] + toa5.get_backup_files(self.path / file)
            for sub_file in file_list:
                dict_list.append(toa5.get_file_dates(file=self.path / sub_file))

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
            for file in self.tables_df.index:

                # Name the tab after the file (drop the dat) - it is necessary
                # to use file name and not just the table name, because for
                # some sites data is drawn from different loggers with same
                # table name
                sheet_name = file.replace('.dat', '')

                # Get the TOA5 data handler (concatenate backups by default)
                handler = toa5.get_file_handler(
                    file=self.path / file, concat_backups=concat_backups
                    )

                # Write info
                (pd.DataFrame(handler.info.values())
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
                (handler.get_conditioned_data(monotonic_index=True)
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
    flux_file = mapper.get_file_from_variable(
        variable='CO2 flux', abs_path=True
        )[0]

    # Get the EC logger info
    logger_info = (
        vm.make_table_df(site=site)
        .loc[
            flux_file.name,
            ['station_name', 'logger_type', 'serial_num', 'OS_version',
             'program_name']
            ]
        )

    # Get the pct missing data
    handler = toa5.single_file_data_handler(file=flux_file)
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

    # Make the dataframe
    df = (
        pd.concat([details, extra_details, logger_info, missing])
        .to_frame()
        .T
        )

    # Gussy it up for output to TOA5, and then write
    df.index = [midnight_time]
    df.index.name = 'TIMESTAMP'
    info_header = ['TOA5', site, 'CR1000', '9999', 'cr1000.std.99.99',
                   'CPU:noprogram.cr1', '9999', 'site_details']
    constructor = toa5.TOA5_file_constructor(data=df, info_header=info_header)
    output_path = (
        PATHS.get_local_path(resource='site_details') / f'{site}_details.dat'
        )
    constructor.write_output_file(dest=output_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteStatusConstructor():
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
            )
        self.site_list = (
            self.table_df.index.get_level_values(level='site').unique().tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_site_variable_data(self, site):
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

        local_date_time = self._get_site_time(site=site)
        merger = TableMerger(site=site)
        df = merger.merge_tables()
        rslt_list = []
        for variable in df.columns:
            series = df[variable].dropna()
            var_details = merger.var_map.get_field_from_variable(
                variable=variable, from_field='translation_name'
                )
            logger = var_details.logger_name.item()
            table = var_details.table_name.item()
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
                days = (local_date_time - last_valid_date_time).days
                rslt_dict.update(
                    {'last_valid_record': last_valid_date_time.strftime(
                        '%Y-%m-%d %H:%M'
                        ),
                     'value': series[-1],
                     'days_since_last_record': int(days)}
                    )
            except IndexError:
                rslt_dict.update(
                    {'last_valid_record': None, 'value': None,
                     'days_since_last_record': None}
                    )
            rslt_list.append(rslt_dict)
        return pd.DataFrame(rslt_list)
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
                    .days
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
            handler = toa5.single_file_data_handler(file=file)
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
    def write_to_excel(self, dest='E:/Sites/site_status.xlsx'):
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
            'Summary': self.get_summary_table(),
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
    def _get_colour(self, n):
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
            colour = 'yellow'
        if 3 <= n < 7:
            colour = 'orange'
        if n >= 7:
            colour = 'red'
        return 'background-color: {}'.format(colour)
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
        style_df[column_name] = df[column_name].apply(self._get_colour)
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
