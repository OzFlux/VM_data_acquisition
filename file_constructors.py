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
        self.time_step = int(SITE_DETAILS.get_single_site_details(
            site=site, field='time_step'
            ))
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
    constructor = toa5.TOA5_file_constructor(data=df, info_header=info_header)
    output_path = (
        PATHS.get_local_path(resource='site_details') / f'{site}_details.dat'
        )
    constructor.write_output_file(dest=output_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_logger_TOA5(site):

    table_df=(
        vm.make_table_df(site=site)
        [['station_name', 'logger_type', 'serial_num', 'OS_version',
          'program_name', 'program_sig']]
        .reset_index()
        .set_index(keys='station_name')
        )
    table_df = (
        table_df[~table_df.index.duplicated()]
        .reset_index()
        )
    new_idx = pd.date_range(
        start=dt.datetime.now().date() - dt.timedelta((len(table_df) - 1)),
        periods=len(table_df),
        freq='D'
        )
    new_idx.name = 'TIMESTAMP'
    table_df.index = new_idx
    TOA5_maker = toa5.TOA5_file_constructor(data=table_df)
    output_path = PATHS.get_local_path(resource='site_details')
    TOA5_maker.write_output_file(dest=output_path / f'{site}_logger_details.dat')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_info_excel():

    df = vm.make_table_df().reset_index()
    new_index = pd.MultiIndex.from_frame(df[['site', 'file_name']])
    df.index = new_index
    df.drop(['format', 'site', 'file_name'], axis=1, inplace=True)
    records_list = []
    for entry in df.index:
        handler = toa5.single_file_data_handler(file=df.loc[entry, 'full_path'])
        rslt_dict = handler.get_missing_records()
        rslt_dict.pop('gap_distribution')
        rslt_dict.update(
            {'duplicate_records': any(handler.get_duplicate_records()),
             'duplicate_indices': any(handler.get_duplicate_indices())
             }
            )
        records_list.append(rslt_dict)
    return pd.concat([df, pd.DataFrame(records_list, index=new_index)], axis=1)
    df.to_excel('E:/Sites/test.xlsx')
#------------------------------------------------------------------------------

