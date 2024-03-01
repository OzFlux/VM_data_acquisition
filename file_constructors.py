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
import data_parser as dp
import file_handler as fh
import file_io as io
import met_functions as mf
import paths_manager as pm
import data_mapper as dm
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
import sparql_site_details as sd

#------------------------------------------------------------------------------
### GLOBAL CLASSES ###
#------------------------------------------------------------------------------

PATHS = pm.paths()
SITE_DETAILS = sd.site_details()

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------



###############################################################################
### BEGIN STATUS CONSTRUCTOR CLASS ###
###############################################################################

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

        self.site_list = dm.get_mapped_site_list()
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
            file = dm.get_latest_10Hz_file(site=site)
            files.append(file)
            try:
                days.append(
                    (dt.datetime.now() -
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

        output_path = output_directory = PATHS.get_local_path(
            resource='data', stream='flux_slow', as_str=True
            )
        table_df = dm.make_table_df(logger_info=True, extended_info=True)
        data_list = []
        for file in table_df.index:
            site = table_df.loc[file, 'site']
            full_path = pathlib.Path(
                output_path.replace(PATHS._placeholder, site)
                ) / file
            site_time = self._get_site_time(site=site)
            data_list.append(
                dp.get_file_record_stats(
                    file=full_path,
                    site_time=site_time
                    )
                )
        return (
            pd.concat(
                [table_df, pd.DataFrame(data_list, index=table_df.index)],
                axis=1
                )
            .reset_index()
            .drop(['file_name', 'format'], axis=1)
            .set_index(keys=['site', 'station_name', 'table_name'])
            .reset_index()
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
            self, dest='E:/Network_documents/Status/site_to_vm_status.xlsx'
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
            'Summary': self.get_summary_table(),
            '10Hz_files': self.get_10Hz_table()
            }
        key_table = self.get_key_details()

        # Write sheets
        with pd.ExcelWriter(path=dest) as writer:

            # For the Summary and 10Hz_files tables...
            for sheet_name, data in iter_dict.items():

                # Prepend the run date and time to the spreadsheet
                self._write_time_frame(xl_writer=writer, sheet=sheet_name)


                # Output and format the results
                (
                    data.style.apply(self._get_style_df, axis=None)
                    .to_excel(
                        writer, sheet_name=sheet_name, index=False, startrow=1
                        )
                    )
                self._set_column_widths(
                    df=data, xl_writer=writer, sheet=sheet_name
                    )

            # Iterate over sites...
            for site in self.site_list:

                print (site)
                # Prepend the run date and time to the spreadsheet
                self._write_time_frame(
                    xl_writer=writer, sheet=site, site=site
                    )

                # Output and format the results
                site_df = (
                    dp.SiteDataParser(site=site).get_record_stats_by_variable()
                    )
                (
                    site_df.style.apply(
                        self._get_style_df,
                        column_name='days_since_last_valid_record',
                        axis=None
                        )
                    .to_excel(
                        writer, sheet_name=site, startrow=1, index=False
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
            dt.datetime.now() -
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
            use_time = dt.datetime.now()
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

    try:
        n = int(n)
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
    except ValueError:
        if isinstance(n, str):
            colour = 'red'
        elif np.isnan(n):
            colour = None
    if colour == None:
        return ''
    if xl_format:
        return f'background-color: {colour}'
    return colour
#------------------------------------------------------------------------------

###############################################################################
### END STATUS CONSTRUCTOR CLASS ###
###############################################################################



###############################################################################
### BEGIN GENERAL PUBLIC FUNCTIONS ###
###############################################################################


###############################################################################
### BEGIN L1 CONSTRUCTOR FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def construct_l1(site, concat_backups=True, na_values=''):
    """
    Function for generating the collated L1 file from individual TOA5 files,
    including concatenation of backup files where legal.

    Parameters
    ----------
    site : str
        Site name.
    concat_backups : Bool, optional.
        If true, finds all backup files and concatenates the data nwith the
        primary file. The default is True.
    na_values : str
        The fill values for the spreadsheet. The default is ''.

    Returns
    -------
    None.

    """


    file_mngr = dm.FileManager(site=site)
    time_step = SITE_DETAILS.get_single_site_details(
        site=site, field='time_step'
        )
    dest = file_mngr.path / f'{site}_L1.xlsx'
    date_idx = _get_inclusive_date_index(site=site, time_step=time_step)

    # Iterate over all files and write to separate excel workbook sheets
    with pd.ExcelWriter(path=dest) as writer:

        for file in file_mngr.file_list:

            # Create full path
            full_path = file_mngr.path / file

            # Name the tab after the file (drop the dat) - it is necessary
            # to use file name and not just the table name, because for
            # some sites data is drawn from different loggers with same
            # table name
            sheet_name = full_path.stem

            # Get the data handler (concatenate backups by default)
            handler = fh.DataHandler(
                file=full_path, concat_files=concat_backups
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
            (handler.get_conditioned_headers(output_format='TOA5')
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
                    resample_intvl=f'{int(time_step)}T',
                    output_format='TOA5'
                    )
                .to_excel(
                    writer, sheet_name=sheet_name, header=False,
                    index=False, startrow=4, na_rep=na_values
                    )
                )
#------------------------------------------------------------------------------

#--------------------------------------------------------------------------
def _get_inclusive_date_index(site, time_step):
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

    file_mngr = dm.FileManager(site=site)
    dates_list = []
    for file in file_mngr.file_list:
        dates = file_mngr.get_file_start_end_dates(
            file=file, incl_backups=True
            )
        mnt_remain = np.mod(dates['start_date'].minute, time_step)
        if mnt_remain:
            dates['start_date'] += dt.timedelta(
                minutes=int(time_step - mnt_remain)
                )
        mnt_remain = np.mod(dates['end_date'].minute, time_step)
        if mnt_remain:
            dates['end_date'] -= dt.timedelta(
                minutes=int(mnt_remain)
                )
        dates_list.append(dates)
    return pd.date_range(
        start=min(dates['start_date'] for dates in dates_list),
        end=max(dates['end_date'] for dates in dates_list),
        freq=f'{time_step}T'
        )
#--------------------------------------------------------------------------

###############################################################################
### END L1 CONSTRUCTOR FUNCTIONS ###
###############################################################################


#------------------------------------------------------------------------------
def merge_site_data(site, concat_files=False, truncate_to_flux=False):

    # Get parser
    parser = dp.SiteDataParser(site=site, concat_files=concat_files)

    # Get the data, and truncate it to the datetime bounds of the flux file if
    # requested
    data, headers = parser.get_data_by_variable(
        fill_missing=True, incl_headers=True, output_format='TOA5'
        )
    if truncate_to_flux:
        flux_dates = parser.Files.get_file_start_end_dates(
            file=parser.Files.flux_file,
            incl_backups=concat_files
            )
        data = data.loc[flux_dates['start_date']: flux_dates['end_date']]

    # Configure for output
    info = (
        dict(zip(io.INFO_FIELDS, io.FILE_CONFIGS['TOA5']['dummy_info']))
        )
    info.update({'table_name': 'merged'})
    output_path = output_directory = PATHS.get_local_path(
        resource='data', stream='flux_slow', site=site
        ) / f'{site}_merged_std.dat'

    # Now output the data
    io.write_data_to_file(
        headers=headers,
        data=data,
        abs_file_path=output_path,
        info=info,
        output_format='TOA5'
        )
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
    file_mngr = dm.FileManager(site=site)

    # Get the EC logger info
    logger_info = (
        file_mngr.get_file_attributes(file=file_mngr.flux_file)
        [['station_name', 'logger_type', 'serial_num', 'OS_version',
         'program_name']]
        )

    # Get the pct missing data
    handler = handler=fh.DataHandler(file=file_mngr.path / file_mngr.flux_file)
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
        '10Hz_file': file_mngr.get_latest_10Hz_file()
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
