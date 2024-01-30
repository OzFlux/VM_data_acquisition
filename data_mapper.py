#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:05:35 2022

@author: imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import pathlib
import numpy as np
import pandas as pd
import sys

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import paths_manager as pm
import file_io as io
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
import sparql_site_details as sd

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

ALLOWED_INDEX_FIELDS = ['site_name', 'translation_name']

#------------------------------------------------------------------------------
PATHS = pm.paths()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteDataMapper():

    def __init__(self, site):
        """
        Class to generate mapping functions from site-specific variables to
        universal standard (where possible, nomenclature is based on the
                            guidance in the PFP Wiki)

        Parameters
        ----------
        site : str
            The site for which to generate the map.

        Returns
        -------
        None.

        """

        self.site = site
        self.path = PATHS.get_local_path(
            resource='data', stream='flux_slow', site=site
            )
        self.table_df = make_table_df(site=site).drop('site', axis=1)
        self.site_df = make_site_df(site=site, table_df=self.table_df)
        self.site_details = (
            sd.site_details().get_single_site_details('Calperum')
            )
        self._check_files_exist()

    #--------------------------------------------------------------------------
    ### INIT METHODS ###
    #--------------------------------------------------------------------------
    def _check_files_exist(self):

        for file in self.get_file_list(abs_path=True):
            try:
                assert file.exists()
            except AssertionError:
                raise FileNotFoundError(
                    f'Could not find file {file.name} in '
                    f'directory {str(file.parent)}'
                    )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### PUBLIC METHODS ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_available_variables(self, source_field='site_name', by_file=False):

        local_df = self._get_indexed_df(field=source_field)
        if not by_file:
            return local_df[~local_df.Missing].index.tolist()

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_df(self):

        return self.site_df.copy()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_field_from_variable(self, variable, source_field, return_field=None):

        _check_index_field(field=source_field)
        if return_field:
            if not return_field in self.site_df.reset_index().columns:
                raise KeyError(f'Return field {return_field} not found!')
        if return_field == source_field:
            raise KeyError('return_field must be different from from_field!')
        local_df = self.site_df.reset_index().set_index(keys=source_field)
        if return_field is None:
            return local_df.loc[variable]
        return local_df.loc[variable, return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_list(self, abs_path=False):

        files = self.site_df.file_name.dropna().unique().tolist()
        if not abs_path:
            return files
        return [self.path / file for file in files]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_flux_file_attributes(self):

        return self.table_df.loc[self.get_flux_file()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(self, file):

        return self.table_df.loc[file]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_flux_file(self, abs_path=False):

        file_name = get_flux_file(site=self.site)
        if not abs_path:
            return file_name
        return self.path / file_name
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_variables(self, source_field='site_name'):

        local_df = self._get_indexed_df(field=source_field)
        return local_df[local_df.Missing].index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(self, variable, source_field='site_name'):

        return self._get_indexed_df(field=source_field).loc[variable]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_conversion(self, variable, source_field='site_name'):

        df = self.get_variables_to_convert(source_field=source_field)
        return df.loc[variable, ['site_units', 'standard_units']].to_dict()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variables_to_convert(
            self, source_field='site_name', file=None, return_fmt='frame'
            ):

        local_df = self._get_indexed_df(field=source_field)
        local_df = local_df[local_df.conversion]
        if file:
            local_df = local_df.loc[local_df.file_name == file]
        if return_fmt == 'list':
            return local_df[local_df.conversion].index.tolist()
        if return_fmt == 'dict':
            return {
                variable: self.get_variable_conversion(
                    variable=variable,
                    source_field=source_field
                    )
                for variable in local_df.index
                }
        if return_fmt == 'frame':
            return local_df.loc[local_df.conversion]
        raise TypeError('return_fmt must be one of "list", "dict" or "frame"')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_list(self, source_field='site_name', file=None):

        return (
            self._get_indexed_df(field=source_field)
            [self._get_return_field(field=source_field)]
            .index
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_translation(
            self, variable, source_field='site_name', as_dict=False
            ):

        result_dict = self.map_variable_translation(source_field=source_field)
        if not as_dict:
            return result_dict[variable]
        return {variable: result_dict[variable]}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_variable_translation(
            self, source_field='site_name', file=None
            ):

        return_field = self._get_return_field(field=source_field)
        local_df = self._get_indexed_df(field=source_field)
        if file:
            local_df = local_df.loc[local_df.file_name == file]
        return local_df[return_field].to_dict()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_return_field(self, field):

        if field == 'site_name':
            return 'translation_name'
        if field == 'translation_name':
            return 'site_name'
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_indexed_df(self, field):

        _check_index_field(field=field)
        if field == self.site_df.index.name:
            return self.site_df.copy()
        return (
            self.site_df
            .reset_index()
            .set_index(keys=field)
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_index_field(field):

    if not field in ALLOWED_INDEX_FIELDS:
        raise KeyError(
            'Variable source field (kwarg from_field) must be one of '
            f'{", ".join(ALLOWED_INDEX_FIELDS)}!'
            )
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
        table_df = make_table_df(site=site).drop('site', axis=1)

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
        .loc[site, 'Flux file name']
        )
    try:
        return rslt.dropna().item()
    except AttributeError:
        return rslt
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_table_df(site=None):
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
    renamer = {'Site': 'site', 'File name': 'file_name'}
    df = (
        pd.read_excel(
            io=PATHS.get_local_path(resource='xl_variable_map'),
            sheet_name='file_list',
            usecols=list(renamer.keys()),
            converters={'Site': lambda x: x.replace(' ', '')},
            )
        .rename(renamer, axis=1)
        )

    # If user-supplied sitename, drop all except that
    if site: df = df.loc[df.site==site.replace(' ','')]

    # Create and write full file paths
    parent_path = PATHS.get_local_path(
        resource='data', stream='flux_slow', as_str=True
        )
    df = (
        df
        .assign(full_path=df.site.apply(
            lambda x: pathlib.Path(parent_path.replace('<site>', x))
            ) / df.file_name
            )
        )

    # Assign additional variables (table info, start and end dates) and return
    return (
        df
        .join(pd.DataFrame(
            data=[
                io.get_file_info(file=file) |
                io.get_start_end_dates(file=file) |
                {'backups':
                 ','.join(
                     f.name for f in io.get_eligible_concat_files(file=file)
                     )
                 } |
                {'interval': io.get_file_interval(file=file)}
                for file in df.full_path
                ],
            index=df.index
            ))
        .set_index(keys='file_name')
        .sort_index()
        )
#------------------------------------------------------------------------------