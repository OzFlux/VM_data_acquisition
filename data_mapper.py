#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:05:35 2022

@author: imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import numpy as np
import os
import pandas as pd
import pathlib
import sys

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import paths_manager as pm
import file_io as io
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
from sparql_site_details import site_details as sd


#------------------------------------------------------------------------------
PATHS = pm.paths()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileManager():
    """
    Class that manages and returns attributes of data files for a given site.
    Note that 'file' argument for various methods must not be passed with an
    absolute path. The absolute path is predetermined and retrieved by the
    paths_manager module.
    """

    #--------------------------------------------------------------------------
    def __init__(self, site):
        """
        Initialise the file manager with site name.

        Parameters
        ----------
        site : str
            The site.

        Returns
        -------
        None.

        """

        self.site = site
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )
        df = _get_file_df()
        self.file_list = df.loc[df.site==site].index.tolist()
        self.flux_file = get_flux_file(site=site)
        self.variable_lookup_table = pd.concat([
            io.get_header_df(file=self.path / file)
            .assign(file=file)
            for file in self.file_list
            ])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(self, file, include_extended=False):
        """
        Return the set of attributes for a given file.

        Parameters
        ----------
        file : str
            Name of file for which to retrieve attributes.
        include_extended : Bool, optional
            Returns extended data. If false, only information pulled from the
            file header will be included. If true, further interrogation of the
            file data content occurs (start and end dates, interval or time step),
            and backup files eligible for concatenation. The default is False.

        Returns
        -------
        pd.core.series.Series
            Series containing the requested attributes.

        """

        if not include_extended:
            return pd.Series(
                _get_basic_file_attrs(path_to_file=self.path / file)
                )
        return pd.Series(_get_all_file_attrs(path_to_file=self.path / file))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_abs_file_path(self, file):
        """
        Return the absolute file path for the requested file name.

        Parameters
        ----------
        file : str
            Name of file for which to retrieve absolute_path.

        Raises
        ------
        FileNotFoundError
            Raised if invalid file name requested.

        Returns
        -------
        pathlib.Path
            The full file path.

        """

        if not file in file_list:
            raise FileNotFoundError(f'Could not find file {file}')
        return self.path / file
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_all_file_attributes(self, include_extended=False):
        """
        Convenience method that iterates over the 'file_list' attribute,
        returning the results of get_file_attributes method.

        Parameters
        ----------
        include_extended : TYPE, optional
            See parent method docstring. The default is False.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        return (
        pd.concat(
            [self.get_file_attributes(
                file=file, include_extended=include_extended
                ) for file in self.file_list],
            axis=1
            )
        .set_axis(self.file_list, axis=1)
        ).T
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_start_end_dates(self, file, incl_backups=False):
        """
        Get the beginning and end dates of the file.

        Parameters
        ----------
        file : str
            Name of file for which to retrieve beginning and end dates.
        incl_backups : Bool, optional
            Returns the earliest and latest dates across the primary AND
            backup files if True, othertwise just the primary file.
            The default is False.

        Returns
        -------
        dict
            Start and end dates.

        """

        base_rslt = io.get_start_end_dates(file=self.path / file)
        if not incl_backups:
            base_rslt
        rslt_list = [base_rslt] + [
            io.get_start_end_dates(file=file)
            for file in self.get_eligible_concat_files(file=file)
            ]
        return {
            'start_date': min(rslt['start_date'] for rslt in rslt_list),
            'end_date': max(rslt['end_date'] for rslt in rslt_list)
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_latest_10Hz_file(self):
        """
        Return the latest 10Hz file for site (only implemented for CSI-based
        sites using standard naming convention).

        Returns
        -------
        str
            File name (no absolute path) of most recent raw 10Hz file.

        """

        return get_latest_10Hz_file(site=self.site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_raw_variables_in_file(self, file, list_only=False):
        """
        Get the raw variables found in a given file.

        Parameters
        ----------
        file : str
            Name of file for which to retrieve the variables.
        list_only : Bool, optional
            If false, return unit and sampling (if TOA5) details held for the
            file. If true, just return a list of variable names.
            The default is False.

        Returns
        -------
        pd.core.frame.DataFrame or list
            The frame or list.

        """

        df = (
            self.variable_lookup_table.loc[
                self.variable_lookup_table.file==file
                ]
            .drop('file', axis=1)
            )
        if list_only:
            return df.index.tolist()
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_from_raw_variable(self, variable):
        """
        

        Parameters
        ----------
        variable : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        return self.variable_lookup_table.loc[variable, 'file']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_interval(self, file):

        return io.get_file_interval(file=self.path / file)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_eligible_concat_files(self, file):

        return io.get_eligible_concat_files(file=self.path / file)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_raw_variable_units(self, variable):

        return self.variable_lookup_table.loc[variable, 'units']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_raw_variable_sampling(self, variable):

        try:
            return self.variable_lookup_table.loc[variable, 'sampling']
        except KeyError:
            return None
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
# class ExtendedSiteDataManager(SiteDataManager):

#     def __init__(self, site):

#         super().__init__(site)

class MappedDataManager():

    def __init__(self, site, default_source_field=None):

        if default_source_field is None:
            self.source_field = 'site_name'
        else:
            self._check_index_field(field=default_source_field)
            self.source_field = default_source_field
        self.return_field = self._get_return_field(field=self.source_field)
        self.translation_table = make_site_df(site=site)

    #--------------------------------------------------------------------------
    def get_variable_list(
            self, return_field=None, by_file=None, exclude_missing=False
            ):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        return_field = (
            self._parse_index_field(field=return_field, which='return')
            )

        # Do stuff
        df = self.translation_table.copy()
        if by_file:
            df = df.loc[df.file_name==by_file]
        if exclude_missing:
            df = df.loc[~df.Missing]
        return (
            df
            .reset_index()
            [return_field]
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_mapped_file_list(self):

        return self.translation_table.file_name.dropna().unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_name_translation(self, variable=None, source_field=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        source_field = self._parse_index_field(
            field=source_field, which='source'
            )

        return_field = self._get_return_field(field=source_field)
        df = self._get_indexed_df(field=source_field)
        if variable is None:
            return df[return_field].to_dict()
        return {variable: df.loc[variable, return_field]}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_unit_translation(self, variable=None, source_field=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        source_field = self._parse_index_field(
            field=source_field, which='source'
            )

        # If variable passed, return the unit translation for that variable only
        df = self._get_indexed_df(field=source_field)
        if variable:
            return df.loc[variable, ['site_units', 'standard_units']].to_dict()

        # If no variable passed, return a dictionary containing each variable
        # requiring trasnlation as key, and the trasnlation dict as value
        return {
            var:
                df.loc[var, ['site_units', 'standard_units']].to_dict()
                for var in self.get_variable_conversion_list(
                        return_field=source_field
                        )
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(self, variable, source_field=None, attr=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        source_field = self._parse_index_field(
            field=source_field, which='source'
            )

        df = self._get_indexed_df(field=source_field)
        if not attr:
            return df.loc[variable]
        return df.loc[variable, attr]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_conversion_list(self, return_field=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        return_field = self._parse_index_field(
            field=return_field, which='return'
            )

        return self._get_missing_or_converted(
            which='conversion', return_field=return_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_missing_list(self, return_field=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        return_field = self._parse_index_field(
            field=return_field, which='return'
            )

        return self._get_missing_or_converted(
            which='Missing', return_field=return_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_missing_or_converted(self, which, return_field):

        return_field = self._parse_index_field(
            field=return_field, which='return'
            )

        return (
            self.translation_table
            .loc[self.translation_table[which]]
            .reset_index()
            [return_field]
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_measured_list(self, return_field=None):

        # If nothing passed, use instance default source attribute;
        # if overriden, check conformity of string
        return_field = self._parse_index_field(
            field=return_field, which='return'
            )

        return (
            self.translation_table
            .loc[~self.translation_table.Missing]
            .reset_index()
            [return_field]
            .tolist()
            )

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_soil_moisture_variables(self, return_field=None):

        return self.get_variable_by_long_name(
            variable='Soil water content', return_field=return_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_soil_heat_flux_variables(self, return_field=None):

        return self.get_variable_by_long_name(
            variable='Soil heat flux at depth z', return_field=return_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_soil_temperature_variables(self, return_field=None):

        return self.get_variable_by_long_name(
            variable='Soil temperature', return_field=return_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_conversion(self, variable, source_field=None):

        source_field = self._parse_index_field(
            field=source_field, which='source'
            )

        df = self.get_variables_to_convert(source_field=source_field)
        return df.loc[variable, ['site_units', 'standard_units']].to_dict()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_by_long_name(self, variable, return_field=None):

        return_field = self._parse_index_field(
            field=return_field, which='return'
            )

        rslt = (
            self.translation_table
            .reset_index()
            .set_index('long_name')
            .loc[variable, return_field]
            )
        if isinstance(rslt, str):
            return rslt
        return rslt.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_long_name_list(self):

        return self.translation_table.long_name.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_indexed_df(self, field):

        self._check_index_field(field=field)
        if field == self.translation_table.index.name:
            return self.translation_table.copy()
        return (
            self.translation_table
            .reset_index()
            .set_index(keys=field)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_return_field(self, field):

        if field == 'site_name':
            return 'translation_name'
        if field == 'translation_name':
            return 'site_name'
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _parse_index_field(self, field, which):

        if field is None:
            if which == 'source':
                return self.source_field
            if which == 'return':
                return self.return_field

        self._check_index_field(field=field)
        return field
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_index_field(self, field):

        ALLOWED_INDEX_FIELDS = ['site_name', 'translation_name']
        if not field in ALLOWED_INDEX_FIELDS:
            raise KeyError(
                'Variable source field (arg "field") must be one of '
                f'{", ".join(ALLOWED_INDEX_FIELDS)}!'
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_site_df(site, table_df=None):
    """
    Create the dataframe that contains the data to allow mapping from
    site variable names to standard variable names

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe.

    """

    # Get stuff
    map_path = PATHS.get_local_path(resource='xl_variable_map')
    if table_df is None:
        table_df = (
            make_table_df(site=site, logger_info=True).drop('site', axis=1)
            )

    # Create the site dataframe and rename, then drop disabled variables
    site_renamer = {
        'Long name': 'long_name', 'Label': 'site_label',
        'Variable name': 'site_name', 'Variable units': 'site_units',
        'File name': 'file_name'
        }
    site_list = ['Disable']
    site_df = (
        pd.read_excel(
            io=map_path,
            sheet_name=site,
            usecols=site_list + list(site_renamer.keys()),
            converters={'Variable units': lambda x: x if len(x) > 0 else None},
            )
        .rename(site_renamer, axis=1)
        .set_index(keys='long_name')
        )
    site_df = site_df.loc[np.isnan(site_df.Disable)]
    site_df.drop('Disable', axis=1, inplace=True)

    # Add the names of the data tables and loggers extracted directly from the
    # first line of the data file header (and written into the table_df)
    site_df = site_df.assign(
        table_name=table_df.loc[site_df.file_name, 'table_name'].tolist(),
        logger_name=table_df.loc[site_df.file_name, 'station_name'].tolist()
        )

    # Make the master dataframe and rename variables
    master_renamer = {
        'Long name': 'long_name', 'Variable name': 'standard_name',
        'Variable units': 'standard_units',
        }
    master_list = ['Required', 'Max', 'Min']
    master_df = (
        pd.read_excel(
            io=map_path,
            sheet_name='master_variables',
            usecols=master_list + list(master_renamer.keys()),
            converters={'Variable units': lambda x: x if len(x) > 0 else None,
                        'Required': lambda x: True if x==1 else False}
            )
        .rename(master_renamer, axis=1)
        .set_index(keys='long_name')
        )

    # Join frames and generate variables that require input from both sources
    site_df = site_df.join(master_df)
    site_df = site_df.assign(
        conversion=site_df.site_units!=site_df.standard_units,
        translation_name=np.where(
            ~pd.isnull(site_df.site_label),
            site_df.site_label, site_df.standard_name
            )
        )

    # Add critical variables that are missing from the primary datasets
    missing_list = list(
        set(master_df[master_df.Required].index) - set(site_df.index)
        )
    site_df = pd.concat([site_df, master_df.loc[missing_list]])
    site_df = site_df.assign(Missing=pd.isnull(site_df.site_name))
    site_df.loc[site_df.Missing, 'conversion'] = False
    for var in missing_list:
        site_df.loc[var, ['site_name', 'translation_name']] = (
            site_df.loc[var, 'standard_name']
            )

    # Aaaaaaand we're done...
    return site_df.reset_index().set_index(keys='site_name')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_flux_file(site):
    """
    Get the name of the file containing the flux data.

    Parameters
    ----------
    site : str
        Site for which to return the flux file.

    Returns
    -------
    str
        The file name.

    """

    rslt = (
        pd.read_excel(
            io=PATHS.get_local_path(resource='xl_variable_map'),
            sheet_name='file_list'
            )
        .set_index('Site')
        .loc[[site]]
        )
    return rslt.loc[rslt['Is flux file']==True, 'File name'].item()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_table_df(site=None, logger_info=False, extended_info=False):
    """
    Generate a dataframe that ties file names to data tables and logger info

    Parameters
    ----------
    site : str, optional
        Site for which to return the table information. The default is None.

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe.

    """

    # Get the table list from excel file
    df = _get_file_df()

    # If user-supplied sitename, drop all except that
    if site: df = df.loc[df.site==site.replace(' ','')]

    # If user doesn't want the logger info, just return the list of files for
    # the site
    if not logger_info:
        return df.index.tolist()

    # Create full file paths as iterator for data retrieval
    parent_path = PATHS.get_local_path(
        resource='data', stream='flux_slow', as_str=True
        )
    paths_list = (
        df.site.apply(
            lambda x: pathlib.Path(parent_path.replace('<site>', x))
            ) /
        df.index
        )

    # Set the retrieval function
    if not extended_info:
        func = _get_basic_file_attrs
    else:
        func = _get_all_file_attrs

    # Add the logger-generated info fields and return
    return df.join(
        pd.DataFrame(
            data=[func(path_to_file=file) for file in paths_list],
            index=df.index
            )
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_mapped_site_list():

    xl = pd.ExcelFile(PATHS.get_local_path(resource='xl_variable_map'))
    op_sites = sd().get_operational_sites(site_name_only=True)
    return [x for x in xl.sheet_names if x in op_sites]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_latest_10Hz_file(site):

    data_path = PATHS.get_local_path(
        resource='data', stream='flux_fast', site=site,
        subdirs=['TOB3']
        )
    try:
        return max(data_path.rglob('TOB3*.dat'), key=os.path.getctime).name
    except ValueError:
        return 'No files'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_extended_file_attrs(path_to_file):

    return (
        io.get_start_end_dates(file=path_to_file) |
        {'backups': ','.join(
            f.name for f in io.get_eligible_concat_files(file=path_to_file)
            )
            } |
        {'interval': io.get_file_interval(file=path_to_file)}
        )
#------------------------------------------------------------------------------
def _get_basic_file_attrs(path_to_file):

    return io.get_file_info(file=path_to_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_all_file_attrs(path_to_file):

    return (
        _get_basic_file_attrs(path_to_file=path_to_file) |
        _get_extended_file_attrs(path_to_file=path_to_file)
        )
#------------------------------------------------------------------------------

def _get_file_df():

    # Get the table list from excel file
    renamer = {'Site': 'site', 'File name': 'file_name'}
    return (
        pd.read_excel(
            io=PATHS.get_local_path(resource='xl_variable_map'),
            sheet_name='file_list',
            usecols=list(renamer.keys()),
            converters={'Site': lambda x: x.replace(' ', '')},
            )
        .rename(renamer, axis=1)
        .set_index(keys='file_name')
        )
