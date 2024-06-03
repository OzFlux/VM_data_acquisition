# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

Todo:
    - test mode 'a' for appending data

@author: jcutern-imchugh
"""

import datetime as dt
import pandas as pd
import pathlib
import sys
import xarray as xr

import config_getters as cg
import file_handler as fh
import metadata_handlers as mh

#------------------------------------------------------------------------------
SITE_DETAIL_ALIASES = {'elevation': 'altitude'}
SITE_DETAIL_SUBSET = [
    'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step', 'time_zone'
    ]
VAR_METADATA_SUBSET = [
    'height', 'instrument', 'long_name', 'standard_name', 'statistic_type',
    'units'
    ]
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#------------------------------------------------------------------------------

###############################################################################
### BEGIN DATA CONSTRUCTOR CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1DataBuilder():

    #--------------------------------------------------------------------------
    def __init__(self, site, md_mngr=None, concat_files=False):

        self.site = site
        if md_mngr is None:
            md_mngr = mh.MetaDataManager(site=site)
        self.md_mngr = md_mngr
        self.data = merge_data_by_manager(
            site=site, md_mngr=self.md_mngr, concat_files=concat_files
            )
        self.data_years = self.data.index.year.unique().tolist()
        self.global_attrs = self._get_site_global_attrs()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_global_attrs(self) -> dict:
        """


        Args:
            site (TYPE): DESCRIPTION.

        Returns:
            TYPE: DESCRIPTION.

        """

        global_attrs = cg.get_nc_global_attrs()
        new_dict = {
            'metadata_link':
                global_attrs['metadata_link'].replace('<site>', self.site),
            'site_name': self.site
            }
        global_attrs.update(new_dict)

        site_specific_attrs = (
            self.md_mngr.site_details
            [SITE_DETAIL_SUBSET]
            .rename(SITE_DETAIL_ALIASES)
            .to_dict()
            )
        return global_attrs | site_specific_attrs
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _augment_global_attrs(self, df) -> dict:
        """
        Augment global attributes.

        Args:
            df: pandas dataframe containing merged data.
            md_mngr: VariableManager class used to access variable attributes.

        Returns:
            Dict containg global attributes.

        """

        func = lambda x: x.to_pydatetime().strftime(TIME_FORMAT)
        date = dt.datetime.now()
        year = date.strftime('%Y')
        month = date.strftime('%b')

        return (
            self.global_attrs |
            {
                'date_created': date.strftime(TIME_FORMAT),
                'nc_nrecs': len(df),
                'history': f'{month} {year} processing',
                'time_coverage_start': func(df.index[0]),
                'time_coverage_end': func(df.index[-1]),
                'irga_type': self.md_mngr.irga_type,
                'sonic_type': self.md_mngr.sonic_type
                }
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_complete(self) -> xr.Dataset:
        """
        Build an xarray dataset constructed from all of the raw data.

        Returns:
            The dataset.

        """

        return self._build_xarray_dataset(df=self.data)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_by_slice(
            self, start_date: dt.datetime | str=None,
            end_date: dt.datetime | str=None
            ) -> xr.Dataset:
        """
        Build an xarray dataset constructed from a discrete time slice.

        Args:
            start_date (optional): the start date for the data. If None,
            starts at the beginning of the merged dataset. Defaults to None.
            end_date (optional): the end date for the data. If None,
            ends at the end of the merged dataset. Defaults to None.

        Returns:
            The dataset.

        """

        if start_date is None and end_date is None:
            return self.build_xarray_dataset_complete()
        if start_date is None:
            return self._build_xarray_dataset(
                df=self.data.loc[: end_date]
                )
        if end_date is None:
            return self._build_xarray_dataset(
                df=self.data.loc[start_date:]
                )
        return self._build_xarray_dataset(
            df=self.data.loc[start_date: end_date]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_by_year(self, year: int) -> xr.Dataset:

        """
        Build an xarray dataset constructed from a given year.

        Args:
            year: the data year to return.

        Returns:
            The dataset.

        """

        if not year in self.data_years:
            years = ', '.join([str(year) for year in self.data_years])
            raise IndexError(
                f'Data year is not available (available years: {years})'
                )
        bounds = self._get_year_bounds(
            year=year,
            time_step=self.global_attrs['time_step']
            )
        return self._build_xarray_dataset(
            df=self.data.loc[bounds[0]: bounds[1]]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_year_bounds(self, year: int, time_step: int) -> dict:
        """


        Args:
            year (int): DESCRIPTION.
            time_step (int): DESCRIPTION.

        Returns:
            dict: DESCRIPTION.

        """

        return (
                (
                (dt.datetime(year, 1, 1) + dt.timedelta(minutes=time_step))
                .strftime(TIME_FORMAT)
                ),
            dt.datetime(year + 1, 1, 1).strftime(TIME_FORMAT)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_xarray_dataset(self, df: pd.core.frame.DataFrame) -> xr.Dataset:
        """
        Convert the dataframe to an xarray dataset and apply global attributes.

        Args:
            df: the dataframe to convert to xr dataset.

        Returns:
            ds: xarray dataset.

        """

        # Create xarray dataset
        ds = (
            df.assign(
                latitude=self.global_attrs['latitude'],
                longitude=self.global_attrs['longitude']
                )
            .reset_index()
            .set_index(keys=['time', 'latitude', 'longitude'])
            .to_xarray()
            )

        # Assign global and variable attrs and flags
        ds.attrs = self._augment_global_attrs(df)
        self._assign_dim_attrs(ds=ds)
        self._set_dim_encoding(ds=ds)
        self._assign_variable_attrs(ds=ds)
        self._assign_variable_flags(ds=ds)

        return ds
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_dim_attrs(self, ds: xr.Dataset):
        """
        Apply dimension attributes (time, lattitude and longitude).

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        dim_attrs = cg.get_nc_dim_attrs()
        for dim in ds.dims:
            ds[dim].attrs = dim_attrs[dim]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _set_dim_encoding(self, ds: xr.Dataset):
        """
        Apply (so far only) time dimension encoding.

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        ds.time.encoding['units']='days since 1800-01-01 00:00:00.0'
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_variable_attrs(self, ds: xr.Dataset):
        """
        Assign the variable attributes to the existing dataset.

        Args:
            ds: xarray dataset.
            md_mngr: VariableManager class used to access variable attributes.

        Returns:
            None.

        """

        var_list = [var for var in ds.variables if not var in ds.dims]
        for var in var_list:
            ds[var].attrs = (
                self.md_mngr.get_variable_attributes(variable=var)
                [VAR_METADATA_SUBSET]
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_variable_flags(self, ds: xr.Dataset):
        """
        Assign the variable QC flags to the existing dataset.

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        var_list = [var for var in ds.variables if not var in ds.dims]
        for var in var_list:
            ds[f'{var}_QCFlag'] = (
                ['time', 'latitude', 'longitude'],
                pd.isnull(ds[var]).astype(int),
                {'long_name': f'{var}QC flag', 'units': '1'}
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_netcdf(self, year=None):
        """
        Output data to netcdf file.

        Args:
            year (optional): year subset for file. Defaults to None.

        Returns:
            None.

        """

        ds = self.build_xarray_dataset(year=year)
        name_list = [self.site]
        if not year is None:
            name_list.append(str(year))

        path = self.md_mngr.data_path / f'{"_".join(name_list)}'
        ds.to_netcdf(path=path, mode='w')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END DATA CONSTRUCTOR CLASS ###
###############################################################################



###############################################################################
### BEGIN NC WRITERS ###
###############################################################################

#------------------------------------------------------------------------------
def make_nc_file(site: str, split_by_year: bool=True):
    """
    Merge all data and metadata sources to create a netcdf file.

    Args:
        site: name of site.
        split_by_year (optional): write discrete year files. Defaults to True.

    Returns:
        None.

    """

    data_builder = L1DataBuilder(site=site, concat_files=True)
    if not split_by_year:
        ds = data_builder.build_xarray_dataset_complete()
        output_path = data_builder.md_mngr.data_path / f'{site}_L1.nc'
        ds.to_netcdf(path=output_path, mode='w')
        return
    for year in data_builder.data_years:
        ds = data_builder.build_xarray_dataset_by_year(year=year)
        output_path = data_builder.md_mngr.data_path / f'{site}_{year}_L1.nc'
        ds.to_netcdf(path=output_path, mode='w')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_nc_year_file(site: str, year: int):
    """
    Merge all data and metadata sources to create a single year netcdf file.

    Args:
        site: name of site.
        year: year to write.

    Raises:
        IndexError: raised if dataset does not contain passed year.

    Returns:
        None.

    """

    data_builder = L1DataBuilder(site=site, concat_files=True)
    if not year in data_builder.data_years:
        raise IndexError('No data available for current data year!')
    ds = data_builder.build_xarray_dataset_by_year(year=year)
    output_path = data_builder.md_mngr.data_path / f'{site}_{year}_L1.nc'
    ds.to_netcdf(path=output_path, mode='w')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_current_nc_file(site: str):
    """
    Check for current year nc file, and if exists, append. If not, create.

    Args:
        site: name of site.

    Returns:
        None.

    """

    md_mngr = mh.MetaDataManager(site=site)
    expected_year = dt.datetime.now().year
    expected_file = md_mngr.data_path / f'{site}_{expected_year}_L1.nc'
    if not expected_file.exists:
        make_nc_year_file(site=site, year=expected_year)
    else:
        append_to_nc(site=site, nc_file=expected_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_nc(site: str, nc_file: pathlib.Path | str):
    """
    Generic append function. Only appends new data.

    Args:
        site: name of site.
        nc_file: nc file to which to append data.

    Returns:
        None.

    """

    ds = xr.open_dataset(nc_file)
    last_nc_date = pd.Timestamp(ds.time.values[-1]).to_pydatetime()
    md_mngr = mh.MetaDataManager(site=site)
    last_raw_date = min(
        [md_mngr.get_file_attrs(x)['end_date'] for x in md_mngr.list_files()]
        )
    if not last_raw_date > last_nc_date:
        print('No new data to append!')
        return
    data_builder = L1DataBuilder(site=site, md_mngr=md_mngr)
    date_iloc = data_builder.data.index.get_loc(last_nc_date) + 1
    new_ds = data_builder.build_xarray_dataset_by_slice(
        start_date=data_builder.data.index[date_iloc]
        )
    combined_ds = xr.concat([ds, new_ds], dim='time', combine_attrs='override')
    ds.close()
    combined_ds.attrs['time_coverage_end'] = (
        pd.to_datetime(combined_ds.time.values[-1])
        .strftime(TIME_FORMAT)
        )
    combined_ds.attrs['nc_nrecs'] = len(combined_ds.time)
    combined_ds.to_netcdf(path=nc_file, mode='w')
#------------------------------------------------------------------------------

###############################################################################
### END NC WRITERS ###
###############################################################################



###############################################################################
### BEGIN MERGE FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def merge_data_by_manager(
        site: str, md_mngr: mh.MetaDataManager=None, concat_files=False
        ) -> pd.core.frame.DataFrame:
    """


    Args:
        site (str): DESCRIPTION.
        md_mngr (mh.MetaDataManager, optional): DESCRIPTION. Defaults to None.

    Returns:
        TYPE: DESCRIPTION.

    """

    if md_mngr is None:
        md_mngr = mh.MetaDataManager(site=site)
    merge_dict = {
        file: md_mngr.translate_variables_by_table(table=table)
        for table, file in md_mngr.map_tables_to_files(abs_path=True).items()
        }
    return fh.merge_data(files=merge_dict, concat_files=concat_files)
#------------------------------------------------------------------------------

###############################################################################
### END MERGE FUNCTIONS ###
###############################################################################
