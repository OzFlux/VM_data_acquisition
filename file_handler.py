# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 11:52:46 2023

@author: jcutern-imchugh
"""


import numpy as np
import pandas as pd

import file_functions as ff
import file_concatenators as fc

###############################################################################
### CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class DataHandler():

    #--------------------------------------------------------------------------
    def __init__(self, file, concat_files=False):
        """
        Set attributes of handler.

        Parameters
        ----------
        file : str or pathlib.Path
            Absolute path to file for which to create the handler.
        concat_files : Boolean or list, optional
            If False, the content of the passed file is parsed in isolation.
            If True, any available backup (TOA5) or string-matched EddyPro
            files stored in the same directory are concatenated.
            If list, the files contained therein will be concatenated with the
            main file.
            The default is False.

        Returns
        -------
        None.

        """

        rslt = _get_handler_elements(file=file, concat_files=concat_files)
        for key, value in rslt.items():
            setattr(self, key, value)

    #--------------------------------------------------------------------------
    def get_conditioned_data(self,
            usecols=None, monotonic_index=False, raise_if_dupe_index=False,
            resample_intvl=None
            ):
        """
        Generate a conditioned version of the data. Duplicate data are dropped.

        Parameters
        ----------
        usecols : list or dict, optional
            The columns to include in the output. If a dict is passed, then
            the columns are renamed, with the mapping from existing to new
            defined by the key (old name): value (new_name) pairs.
            The default is None.
        monotonic_index : bool, optional
            Align the data to a monotonic index. The default is False.
        raise_if_dupe_index : bool, optional
            Raise an error if duplicate indices are found with non-duplicate
            data. The default is False.
        resample_intvl : date_offset, optional
            The time interval to which the data should be resampled.
            The default is None.

        Raises
        ------
        RuntimeError
            Raised if duplicate indices are found with non-duplicate data.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with altered data (returns the existing data if all
                                         options are default).

        """

        # Check for duplicate indices (ie. where records aren't duplicated
        # but index is) and create duplicates mask
        dupe_records = self.get_duplicate_records()
        dupe_indices = self.get_duplicate_indices()
        if any(dupe_indices) and raise_if_dupe_index:
            raise RuntimeError(
                'Duplicate indices with non-duplicate data!'
                )
        dupes_mask = dupe_indices | dupe_records

        # Apply duplicate mask, column subset and rename
        output_data = (
            self._rename_data_or_headers(
                usecols=usecols,
                which='data')
            .loc[~dupes_mask]
            )

        # Resample (use the existing interval for monotonic index)
        if monotonic_index and not resample_intvl:
            resample_intvl = f'{self.interval}T'
        if resample_intvl:
            return output_data.resample(resample_intvl).interpolate(limit=1)

        return output_data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_records(self, as_dates=False):
        """
        Get a representation of duplicate records (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate records occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.data.reset_index().duplicated().set_axis(self.data.index)
        if as_dates:
            return self.data[records].index.to_pydatetime().tolist()
        return records
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_indices(self, as_dates=False):
        """
        Get a representation of duplicate indices (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate indices occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.get_duplicate_records()
        indices = ~records & self.data.index.duplicated()
        if as_dates:
            return self.data[indices].index.to_pydatetime().tolist()
        return indices
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_gap_distribution(self):
        """
        List of gap lengths and their frequencies (counts).

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with number of records as index and count as column.

        """

        # If file is TOA5, it will have a column AND an index named TIMESTAMP,
        # so dump the variable if so
        local_data = self.data.copy()
        try:
            local_data.drop('TIMESTAMP', axis=1, inplace=True)
        except KeyError:
            pass

        # Get instances of duplicate indices OR records, and remove
        dupes = self.get_duplicate_indices() | self.get_duplicate_records()
        local_data = (
            local_data[~dupes]
            .reset_index()
            ['TIMESTAMP']
            )

        # Get gaps
        gap_series = (
            (local_data - local_data.shift())
            .astype('timedelta64[m]')
            .replace(self.interval_as_num, np.nan)
            .dropna()
            )
        gap_series /= self.interval_as_num
        unique_gaps = gap_series.unique().astype(int)
        counts = [len(gap_series[gap_series==x]) for x in unique_gaps]
        return (
            pd.DataFrame(
                data=zip(unique_gaps - 1, counts),
                columns=['n_records', 'count']
                )
            .set_index(keys='n_records')
            .sort_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_records(self):
        """
        Get simple statistics for missing records

        Returns
        -------
        dict
            Dictionary containing the number of missing cases, the % of missing
            cases and the distribution of gap sizes.

        """

        dupes = self.get_duplicate_indices() | self.get_duplicate_records()
        local_data = self.data[~dupes]
        complete_index = pd.date_range(
            start=local_data.index[0],
            end=local_data.index[-1],
            freq=f'{self.interval}T'
            )
        n_missing = len(complete_index) - len(local_data)
        return {
            'n_missing': n_missing,
            '%_missing': round(n_missing / len(complete_index) * 100, 2),
            'gap_distribution': self._get_gap_distribution()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_date_span(self):
        """
        Get start and end dates

        Returns
        -------
        dict
            Dictionary containing start and end dates (keys "start" and "end").

        """

        return {
            'start_date': self.data.index[0].to_pydatetime(),
            'end_date': self.data.index[-1].to_pydatetime()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_list(self):
        """
        Gets the list of variables in the TOA5 header line

        Returns
        -------
        list
            The list.

        """

        return self.headers.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_units(self, variable):
        """
        Gets the units for a given variable

        Parameters
        ----------
        variable : str
            The variable for which to return the units.

        Returns
        -------
        str
            The units.

        """

        return self.headers.loc[variable, 'units']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_variable_units(self):
        """
        Get a dictionary cross-referencing all variables in file to their units

        Returns
        -------
        dict
            With variables (keys) and units (values).

        """

        return dict(zip(
            self.headers.index.tolist(),
            self.headers.units.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_sampling_units(self):
        """
        Get a dictionary cross-referencing all variables in file to their
        sampling methods

        Returns
        -------
        dict
            With variables (keys) and sampling (values).

        """

        if self.file_type == 'EddyPro':
            raise NotImplementedError(
                f'No station info available for file type "{self.file_type}"')
        return dict(zip(
            self.headers.index.tolist(),
            self.headers.sampling.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_concatenation_report(self, abs_file_path):
        """
        Write the concatenation report to file.

        Parameters
        ----------
        abs_file_path : str or pathlib.Path
            Absolute path to the file.

        Raises
        ------
        TypeError
            Raised if no concatenated files.

        Returns
        -------
        None.

        """

        if not self.concat_report:
            raise TypeError(
                'Cannot write a concatenation report if there are no '
                'concatenated files!'
                )
        fc._write_text_to_file(
            line_list=self.concat_report,
            abs_file_path=abs_file_path
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_conditioned_data(self, abs_file_path, output_format, **kwargs):

        # Get data and headers
        data = self.get_conditioned_data(**kwargs)
        headers = self._rename_data_or_headers(which='headers', **kwargs)

        # Configure the output headers and data
        # ... for TOA5
        if self.file_type == 'TOA5':

            # ... to TOA5
            if output_format == 'TOA5':
                header_lines = _format_TOA5_output_headers(
                    info=self.file_info,
                    headers=headers
                    )

            # ... to EddyPro (Why?)
            if output_format == 'EddyPro':
                raise NotImplementedError('No!')

        # ... for EddyPro
        if self.file_type == 'EddyPro':

            # ... to EddyPro
            if output_format == 'EddyPro':
                header_lines = _format_EddyPro_output_headers(
                    info=self.file_info,
                    headers=headers
                    )

            # ... to TOA5
            if output_format == 'TOA5':
                header_lines = _convert_EddyPro_to_TOA5_headers(
                    info=self.file_info,
                    headers=headers
                    )
                data = data.reset_index()

        # Write everything to file
        _write_data_to_file(
            data=data,
            header_lines=header_lines,
            output_format=output_format,
            abs_file_path=abs_file_path
            )

        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _rename_data_or_headers(self, which, usecols=None):

        # Set the subsetting list and rename dict, depending on type
        if not usecols:
            subset_list, rename_dict = self.data.columns, {}
        elif isinstance(usecols, dict):
            subset_list, rename_dict = list(usecols.keys()), usecols.copy()
        elif isinstance(usecols, list):
            subset_list, rename_dict = usecols.copy(), {}
        else:
            raise TypeError('usecols arg must be None, list or dict')

        # Get the file-specific time variables and make sure they are in the
        # subset list, but at the start and only once; and remove them from the
        # rename dictionary.
        dt_names = [
            x for x in
            ff.get_file_type_configs(file_type=self.file_type)
            ['time_variables'].keys()
            ]
        subset_list = (
            dt_names +
            list(filter(lambda x: not x in dt_names, subset_list))
            )
        for key in dt_names:
            rename_dict.pop(key, None)

        # Return the subsetted / renamed data
        if which == 'data':
            return self.data[subset_list].rename(rename_dict, axis=1)
        if which == 'headers':
            return (
                self.headers
                .T
                [subset_list]
                .rename(rename_dict, axis=1)
                .T
                )

        # Raise if you made it this far
        raise KeyError('which arg must be either "data" or "headers"!')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def _convert_EddyPro_to_TOA5_headers(info, headers):

    new_headers = (
        pd.concat([
            pd.DataFrame(
                data='TS',
                index=pd.Index(['TIMESTAMP'], name='variable'),
                columns=['units']
                ),
            headers
            ])
        )
    new_headers['sampling'] = ''
    return _format_TOA5_output_headers(
        info=info, headers=new_headers
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_EddyPro_output_headers(info, headers):

    headers_no_idx = headers.reset_index()
    formatter = ff.get_formatter(file_type='EddyPro', which='write')
    lists = []
    for var in headers_no_idx.columns:
        lists.append(formatter(headers_no_idx[var]))
    return lists
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_TOA5_output_headers(info, headers):

    headers_no_idx = headers.reset_index()
    formatter = ff.get_formatter(file_type='TOA5', which='write')
    lists = [formatter(list(info.values()))]
    for var in headers_no_idx.columns:
        lists.append(formatter(headers_no_idx[var]))
    return lists
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_data_to_file(data, header_lines, output_format, abs_file_path):

    file_configs = ff.get_file_type_configs(file_type=output_format)
    with open(abs_file_path, 'w', newline='\n') as f:
        for line in header_lines:
            f.write(line)
        data.to_csv(
            f, header=False, index=False, na_rep=file_configs['na_values'],
            sep=file_configs['separator'], quoting=file_configs['quoting']
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_handler_elements(file, concat_files=False):
    """
    Get elements required to populate file handler for either single file or
    multi-file concatenated data.

    Parameters
    ----------
    file : str or pathlib.Path
        Absolute path to master file.
    concat_files : boolean or list

    Returns
    -------
    dict
        Contains data (key 'data'), headers (key 'headers') and concatenation
        report (key 'concat_report').

    """

    # Get the file type
    file_type = ff.get_file_type(file=file)

    # Set list empty if no concat requested
    if concat_files is False:
        concat_list = []
        concat_report = []

    # Generate list if concat requested
    if concat_files is True:
        if file_type == 'TOA5':
            concat_list = ff.get_TOA5_backups(file=file)
        if file_type == 'EddyPro':
            concat_list = ff.get_EddyPro_files(file=file)
        if not concat_files:
            concat_list = []
            concat_report = ['No eligible files found!']

    # Populate list if (non-empty) file list passed
    if isinstance(concat_files, list) and len(concat_files) > 0:
        concat_list = concat_files

    # Get data from single or concatenated file(s)
    if not concat_list:
        data = ff.get_data(file=file, file_type=file_type)
        headers = ff.get_header_df(file=file, file_type=file_type)
        file_info = ff.get_file_info(file=file, file_type=file_type)
    else:
        concatenator = fc.FileConcatenator(
            master_file=file,
            file_type=file_type,
            concat_list=concat_list
            )
        data = concatenator.get_concatenated_data()
        headers = concatenator.get_concatenated_header()
        concat_report = concatenator.get_concatenation_report(as_text=True)
        file_info = ff.get_file_info(
            file=file, file_type=file_type, dummy_override=True
            )

    # Get file interval regardless of provenance (single or concatenated)
    interval = ff.get_datearray_interval(
        datearray=np.array(data.index.to_pydatetime())
        )

    # Return the dictionary
    return {
        'file_type': file_type,
        'file_info': file_info,
        'data': data,
        'headers': headers,
        'interval': interval,
        'concat_list': concat_list,
        'concat_report': concat_report
        }
#------------------------------------------------------------------------------
