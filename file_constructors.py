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

PATHS = pm.Paths()
SITE_DETAILS = sd.site_details()


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
    output_path = PATHS.get_local_path(
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

# #------------------------------------------------------------------------------
# def get_latest_10Hz_file(site):

#     data_path = PATHS.get_local_path(
#         resource='data', stream='flux_fast', site=site, subdirs=['TOB3']
#         )
#     try:
#         return max(data_path.rglob('TOB3*.dat'), key=os.path.getctime).name
#     except ValueError:
#         return 'No files'
# #------------------------------------------------------------------------------

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
