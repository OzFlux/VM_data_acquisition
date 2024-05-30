# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: jcutern-imchugh
"""

import datetime as dt
import pandas as pd
import sys
import xarray as xr

import config_getters as cg
import file_handler as fh
import metadata_handlers as mh
from sparql_site_details import site_details


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

#------------------------------------------------------------------------------
class NetCDFBuilder():

    #--------------------------------------------------------------------------
    def __init__(self, site):

        self.site = site
        self.md_mngr = mh.MetaDataManager(site=site)
        self.data = merge_data_by_manager(site=site, md_mngr=self.md_mngr)
        self.global_attrs = get_site_global_attrs(site=site)
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
    def build_xarray_dataset(self, year: int=None) -> xr.Dataset:
        """
        Convert the dataframe to an xarray dataset and apply global attributes.

        Args:
            year (optional): year for which to subset data.

        Returns:
            ds: xarray dataset.

        """

        # Subset the dataset by year (or not)
        df = self.data.copy()
        if year is not None:
            bounds = get_year_bounds(
                year=year,
                time_step=self.global_attrs['time_step']
                )
            df = df.loc[bounds[0]: bounds[1]]

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
        name_list.append('L1.nc')
        path = self.md_mngr.data_path / f'{"_".join(name_list)}'
        ds.to_netcdf(path=path, mode='w')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_year_bounds(year: int, time_step: int) -> dict:

    return (
            (
            (dt.datetime(year, 1, 1) + dt.timedelta(minutes=time_step))
            .strftime(TIME_FORMAT)
            ),
        dt.datetime(year + 1, 1, 1).strftime(TIME_FORMAT)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_global_attrs(site: str) -> dict:
    """


    Args:
        site (TYPE): DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """

    global_attrs = cg.get_nc_global_attrs()
    new_dict = {
        'metadata_link': global_attrs['metadata_link'].replace('<site>', site),
        'site_name': site
        }
    global_attrs.update(new_dict)

    site_specific_attrs = (
        site_details().get_single_site_details(site=site)
        [SITE_DETAIL_SUBSET]
        .rename(SITE_DETAIL_ALIASES)
        .to_dict()
        )
    return global_attrs | site_specific_attrs
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def merge_data_by_manager(
        site: str, md_mngr: mh.MetaDataManager=None
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
    return fh.merge_data(files=merge_dict)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_nc(site, year=None):

    nc_builder = NetCDFBuilder(site=site)
    nc_builder.write_to_netcdf(year=year)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_nc():

    pass
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# if __name__ == '__main__':

#     site = sys.argv[1]

#     make_dataset(site=sys.argv[1])


# #------------------------------------------------------------------------------