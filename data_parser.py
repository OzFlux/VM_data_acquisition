# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 10:00:28 2024

@author: jcutern-imchugh
"""

### Standard modules ###
import datetime as dt
import numpy as np
import pandas as pd

### Custom modules ###
import file_handler as fh
import file_io as io
import met_functions as mf
from data_mapper import SiteDataMapper as sdm

#------------------------------------------------------------------------------
class SiteDataParser():
    """
    This class parses the data for a given site, based on information in the
    underlying variable map (accessed as data_map attribute). Since this is a
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
        self.data_map = sdm(site=site)
        self._site_time_offset = dt.timedelta(
             hours=10 - self.data_map.site_details.UTC_offset
             )
        self.concat_files = concat_files

    ###########################################################################
    ### METHODS BY FILE-BASED QUERY ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def get_file_list(self):

        return self.data_map.get_file_list()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(self, file='flux'):

        file = self._parse_file_name(file=file)
        return self.data_map.get_file_attributes(file)
    #--------------------------------------------------------------------------

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
        sub_series = self.data_map.get_file_attributes(file=file)
        rslt_dict = get_file_record_stats(
            file=self.data_map.path / file,
            site_time=site_time
            )
        return pd.concat([
            sub_series.drop(['full_path', 'format']),
            pd.Series(rslt_dict)
            ])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_record_stats_all_files(self):

        return pd.concat(
            [(pd.DataFrame(self.get_record_stats_by_file(file=file))
              .T
              .set_index(keys=['station_name', 'table_name'])
              )
             for file in self.get_file_list()
             ]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_file(
            self, file='flux', rng_chk=True, convert=True, incl_headers=False,
            ):
        """
        Get the data for all mapped variables in the file. No handling of
        missding variables occurs here.

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
                variable_list=self.data_map.site_df.loc[
                    (~self.data_map.site_df.Missing) &
                    (self.data_map.site_df.file_name==file),
                    'translation_name'
                    ],
                rng_chk=rng_chk,
                convert=convert,
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
            return self.data_map.get_flux_file()
        if not file in self.get_file_list():
            raise FileNotFoundError('File not documented in variable map!')
        return file
    #--------------------------------------------------------------------------

    ###########################################################################
    ### METHODS BY VARIABLE-BASED QUERY ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def get_variable_list(self):
        """


        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        return self.data_map.get_variable_list(source_field='translation_name')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(self, variable):
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

        return self.data_map.get_variable_attributes(
            variable=variable, source_field='translation_name'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_variable(
            self, variable_list=None, rng_chk=True, convert=True,
            fill_missing=False, incl_headers=False
            ):
        """
        Get data for a given list of variables. If the list includes missing
        variables, an attempt will be made to fill these, which requires that
        the data inputs for calculation of the missing variable are present.
        If they are not, you will get NaNs.

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
            headers=None
        else:
            data = data_etc[0]
            headers = data_etc[1]

        # Concatenate the data and apply conversion / range checks if requested
        if convert:
            self._do_unit_conversions(data=data, headers=headers)
        if fill_missing:
            self._construct_missing_variables(data=data, headers=headers)
        if rng_chk:
            self._apply_limits(data=data)

        # Return the things
        if not incl_headers:
            return data
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
                self.data_map.site_df.loc[
                    ~self.data_map.site_df.Missing,
                    'translation_name']
                .tolist()
                )

        # Group variables by file name to minimise file access operations
        grp_obj = (
            self.data_map.site_df
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
                file=self.data_map.path / item[0],
                concat_files=self.concat_files,
                )
            if which == 'data' or 'all':
                data_list.append(
                    handler.get_conditioned_data(
                        usecols=translation_dict,
                        drop_non_numeric=True,
                        )
                    )
            if which == 'headers' or 'all':
                header_list.append(
                    handler.get_conditioned_headers(
                        usecols=translation_dict,
                        drop_non_numeric=True,
                        )
                    )

        # Return the request
        if which == 'data':
            return pd.concat(data_list, axis=1)
        if which == 'headers':
            return pd.concat(header_list).fillna('')
        if which == 'all':
            return (
                pd.concat(data_list, axis=1),
                pd.concat(header_list).fillna('')
                )
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
            attrs = self.data_map.get_variable_attributes(
                variable=variable, source_field='translation_name'
                )
            rslt_dict = {
                'variable': variable,
                'station': attrs.logger_name,
                'table': attrs.table_name,
                }
            series = data[variable].dropna()
            if len(series) > 0:
                last_valid_date_time = series.index[-1]
                days = int((site_time - last_valid_date_time).days)
                rslt_dict.update({
                    'last_valid_record': last_valid_date_time.strftime(
                        '%Y-%m-%d %H:%M'
                        ),
                    'value': series.iloc[-1],
                    'days_since_last_valid_record': days
                    })
            else:
                rslt_dict.update({
                    'last_valid_record': 'None',
                    'value': 'None',
                    'days_since_last_valid_record': 'N/A'
                    })
            rslt_list.append(rslt_dict)
        return pd.DataFrame(
            rslt_list, index=pd.Index(data.columns, name='variable')
            )
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
            attrs = self.data_map.get_variable_attributes(
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
    def _do_unit_conversions(self, data, headers=None):
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
            attrs = self.data_map.get_variable_attributes(
                variable=variable,
                source_field='translation_name'
                )
            if attrs.conversion:
                conversion_func = mf.convert_variable(
                    variable=attrs.standard_name
                    )
                data[variable] = data[variable].apply(
                    conversion_func, from_units=attrs.site_units
                    )

        if not headers is None:
            headers.units = (
                self.data_map.site_df
                .set_index(keys='translation_name')
                .loc[headers.index]
                ['standard_units']
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _construct_missing_variables(self, data, headers=None):

        """
        Generate any missing variables (where possible) from existing variables.
        Where not possible, insert NaN.

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.
        headers : pd.core.frame.DataFrame, optional
            The headers to which to add the variable names.
        Returns
        -------
        None.

        """

        missing_vars = self.data_map.get_missing_variables()
        if not missing_vars:
            return
        for var in missing_vars:
            try:
                data[var] = self.construct_variable(variable=var, data=data)
            except KeyError:
                data[var] = pd.Series(np.tile(np.nan, len(data)))
            if not headers is None:
                headers.loc[var] = {
                    'units': self.data_map.get_variable_attributes(
                        variable=var, source_field='translation_name'
                        ).standard_units,
                    'sampling': ''
                    }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def construct_variable(self, variable, data=None):
        """


        Parameters
        ----------
        variable : TYPE
            DESCRIPTION.
        data : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        req_vars = mf.input_vars[variable]
        if data is None:
            data = self.get_data_by_variable(variable_list=req_vars)
        input_args = {arg: data[arg] for arg in req_vars}
        func = mf.get_function(variable=variable)
        return func(**input_args)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteDataMerger():

    def __init__(self, site):

        self.site = site
        self.sdp = SiteDataParser(site=site)
        self.default_output_directory = self.sdp.data_map.path

    #--------------------------------------------------------------------------
    def merge_all_as_TOA5(self):

        data, headers = self.sdp.get_data_by_variable(
            fill_missing=True, incl_headers=True
            )
        return (
            self._convert_table_index(data=data),
            self._convert_headers(headers=headers),
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_all_as_TOA5(self, abs_file_path=None):

        info = (
            dict(zip(io.INFO_FIELDS, io.FILE_CONFIGS['TOA5']['dummy_info']))
            )
        info.update({'table_name': 'merged'})
        if not abs_file_path:
            abs_file_path = (
                self.default_output_directory / f'{self.site}_merged_std.dat'
                )
        data, headers = self.merge_all_as_TOA5()
        io.write_data_to_file(
            headers=headers,
            data=data,
            abs_file_path=abs_file_path,
            info=info
            )
    #--------------------------------------------------------------------------

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
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_record_stats(file, site_time, concat_files=True):
    """


    Parameters
    ----------
    file : TYPE
        DESCRIPTION.
    site_time : TYPE
        DESCRIPTION.
    concat_files : TYPE, optional
        DESCRIPTION. The default is True.

    Returns
    -------
    rslt_dict : TYPE
        DESCRIPTION.

    """

    handler = fh.DataHandler(file=file)
    rslt_dict = handler.get_missing_records()
    rslt_dict.pop('gap_distribution')
    days_since_last_rec = (
        site_time - handler.data.index[-1].to_pydatetime()
        ).days
    if days_since_last_rec < 0:
        days_since_last_rec = 0
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
    return rslt_dict
#------------------------------------------------------------------------------
