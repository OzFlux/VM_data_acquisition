# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 11:52:46 2023

@author: jcutern-imchugh
"""


import numpy as np
import pandas as pd

import file_io as io
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

    #--------------------------------------------------------------------------
    def get_conditioned_data(self,
            usecols=None, output_format=None, drop_non_numeric=False,
            monotonic_index=False, resample_intvl=None,
            raise_if_dupe_index=False
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
        output_format : str, optional
            The format for data output (None, TOA5 or EddyPro).
            The default is None.
        drop_non_numeric : bool, optional
            Purge the non-numeric columns from the conditioned data. If false,
            the non-numeric columns will be included even if excluded from
            usecols. If true they will be dropped even if included in usecols.
            The default is False.
        monotonic_index : bool, optional
            Align the data to a monotonic index. The default is False.
        resample_intvl : date_offset, optional
            The time interval to which the data should be resampled.
            The default is None.
        raise_if_dupe_index : bool, optional
            Raise an error if duplicate indices are found with non-duplicate
            data. The default is False.

        Raises
        ------
        RuntimeError
            Raised if duplicate indices are found with non-duplicate data.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe with altered data.

        """

        # Apply column subset and rename
        subset_list, rename_dict = self._subset_or_translate(usecols=usecols)
        output_data = self.data[subset_list].rename(rename_dict, axis=1)

        # Apply duplicate mask
        dupe_records = self.get_duplicate_records()
        dupe_indices = self.get_duplicate_indices()
        if any(dupe_indices) and raise_if_dupe_index:
            raise RuntimeError(
                'Duplicate indices with non-duplicate data!'
                )
        dupes_mask = dupe_indices | dupe_records
        output_data = output_data.loc[~dupes_mask]

        # Do the resampling
        if monotonic_index and not resample_intvl:
            resample_intvl = f'{self.interval}T'
        if resample_intvl:
            output_data = output_data.resample(resample_intvl).asfreq()

        # Convert the file format (just fill date fields if no change in format)
        self._format_data_as(data=output_data, file_type=output_format)

        # Drop non-numeric cols
        if drop_non_numeric:
            for var in self._configs['non_numeric_cols']:
                try:
                    output_data.drop(var, axis=1, inplace=True)
                except KeyError:
                    pass

        # Return data
        return output_data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _format_data_as(self, data, file_type):
        """
        Reformat the data depending on type and fill all dates
        (regardless of type)

        Parameters
        ----------
        data : pd.core.frame.DataFrame
            The data.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro").

        Returns
        -------
        None.


        """

        # If file type is None, set to self.file_type
        if file_type is None:
            file_type = self.file_type

        # Get the output format date variables
        dt_vars = (
            io.get_file_type_configs(file_type=file_type)
            ['time_variables']
            )

        # If file type not changing, remove the time variables;
        # If it is changing, drop all file format-specific variables
        if file_type == self.file_type:
            drop_vars = self._configs['time_variables'].keys()
        else:
            drop_vars = self._configs['non_numeric_cols']
        data.drop(
            drop_vars,
            axis=1,
            inplace=True
            )

        # Get the date output formatter and drop new dates into file
        formatter = io.get_formatter(file_type=file_type, which='write_date')
        date_series = pd.Series(data.index.to_pydatetime(), index=data.index)
        for dt_var in dt_vars.keys():
            series = date_series.apply(formatter, which=dt_var)
            data.insert(dt_vars[dt_var], dt_var, series)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_conditioned_headers(
            self, usecols=None, output_format=None, drop_non_numeric=False
            ):
        """


        Parameters
        ----------
        usecols : list or dict, optional
            The columns to include in the output. If a dict is passed, then
            the columns are renamed, with the mapping from existing to new
            defined by the key (old name): value (new_name) pairs.
            The default is None.
        output_format : str, optional
            The format for header output (None, TOA5 or EddyPro).
            The default is None.
        drop_non_numeric : bool, optional
            Purge the non-numeric headers from the conditioned data. If false,
            the non-numeric headers will be included even if excluded from
            usecols. If true they will be dropped even if included in usecols.
            The default is False.

        Returns
        -------
        output_headers : pd.core.frame.DataFrame
            Dataframe with altered headers.

        """

        # Apply column subset and rename
        subset_list, rename_dict = self._subset_or_translate(usecols=usecols)
        output_headers = self.headers.loc[subset_list].rename(rename_dict)

        # Convert the date fields (leave them if no change in format)
        output_headers = self._format_headers_as(
            headers=output_headers, file_type=output_format
            )

        # Drop non-numeric cols
        if drop_non_numeric:
            for var in self._configs['non_numeric_cols']:
                try:
                    output_headers.drop(var, inplace=True)
                except KeyError:
                    pass

        return output_headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _format_headers_as(self, headers, file_type):
        """
        Returns the unaltered headers unless the input format is EddyPro and the
        output format is TOA5.

        Parameters
        ----------
        headers : pd.core.frame.DataFrame
            The file headers.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro").

        Returns
        -------
        pd.core.frame.DataFrame
            The file headers, either unchanged or formatted to look like TOA5.

        """

        if file_type is None:
            file_type = self.file_type
        if file_type == self.file_type:
            return headers
        if file_type == 'TOA5':
            return (
                pd.concat([
                    pd.DataFrame(
                        data='TS',
                        index=pd.Index(['TIMESTAMP'], name='variable'),
                        columns=['units']
                        ),
                        headers
                        ])
                    .assign(sampling='')
                    .drop(self._configs['non_numeric_cols'])
                    )
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
            ['DATETIME']
            )

        # Get gaps
        gap_series = (
            (local_data - local_data.shift())
            .astype('timedelta64[s]')
            .replace(self.interval, np.nan)
            .dropna()
            )
        gap_series /= self.interval
        unique_gaps = gap_series.unique().astype('int64')
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
    def get_missing_records(self, raise_if_single_record=False):
        """
        Get simple statistics for missing records

        Returns
        -------
        dict
            Dictionary containing the number of missing cases, the % of missing
            cases and the distribution of gap sizes.

        """

        if self.interval is None:
            if raise_if_single_record:
                raise TypeError('Analysis not applicable to single record!')
            return {
                'n_missing': np.nan,
                '%_missing': np.nan,
                'gap_distribution': np.nan
                }
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
    def get_non_numeric_variables(self):

        return self._configs['non_numeric_cols']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_numeric_variables(self):

        return [
            col for col in self.data.columns if not col in
            self.get_non_numeric_variables()
            ]
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
    def write_conditioned_data(
            self, abs_file_path, usecols=None, output_format=None,
            drop_non_numeric=False, **kwargs
            ):

        if output_format is None:
            output_format = self.file_type

        io.write_data_to_file(
            headers=self.get_conditioned_headers(
                usecols=usecols,
                output_format=output_format,
                drop_non_numeric=drop_non_numeric,
                ),
            data=self.get_conditioned_data(
                usecols=usecols,
                output_format=output_format,
                drop_non_numeric=drop_non_numeric,
                **kwargs
                ),
            abs_file_path=abs_file_path,
            output_format=output_format,
            info=self.file_info
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _subset_or_translate(self, usecols):

        # Set the subsetting list and rename dict, depending on type
        if usecols is None:
            subset_list, rename_dict = self.data.columns, {}
        elif isinstance(usecols, dict):
            subset_list, rename_dict = list(usecols.keys()), usecols.copy()
        elif isinstance(usecols, list):
            subset_list, rename_dict = usecols.copy(), {}
        else:
            raise TypeError('usecols arg must be None, list or dict')

        # Get the file-specific critical variables, keep them in the column
        # list and purge from the rename dictionary!
        critical_cols = self._configs['non_numeric_cols']
        subset_list = (
            self._configs['non_numeric_cols'] +
            [col for col in subset_list if not col in critical_cols]
            )
        for key in critical_cols:
            try:
                rename_dict.pop(key, None)
            except KeyError:
                pass

        # Return the subset list and the renaming dictionary
        return subset_list, rename_dict
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### FUNCTIONS ###
###############################################################################

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
        See concat_files description in __init__ docstring for DataHandler.

    Returns
    -------
    dict
        Contains data (key 'data'), headers (key 'headers') and concatenation
        report (key 'concat_report').

    """

    # Set an emptry concatenation list
    concat_list = []

    # If boolean passed...
    if concat_files is True:
        concat_list = io.get_eligible_concat_files(file=file)

    # If list passed...
    if isinstance(concat_files, list):
        concat_list = concat_files

    # If concat_list has no elements, get single file data
    if len(concat_list) == 0:
        fallback = False if not concat_files else True
        data_dict = _get_single_file_data(file=file, fallback=fallback)

    # If concat_list has elements, use the concatenator
    if len(concat_list) > 0:
        data_dict = _get_concatenated_file_data(
            file=file,
            concat_list=concat_list
            )

    # Get file interval regardless of provenance (single or concatenated)
    data_dict.update(
        {'interval': io.get_datearray_interval(
            datearray=np.array(data_dict['data'].index.to_pydatetime())
            )
            }
        )

    # Return the dictionary
    return data_dict
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_concatenated_file_data(file, concat_list):

    file_type = io.get_file_type(file=file)
    configs = io.get_file_type_configs(file_type=file_type)
    concatenator = fc.FileConcatenator(
        master_file=file,
        file_type=file_type,
        concat_list=concat_list
        )
    return {
        'file_type': file_type,
        'file_info': io.get_file_info(
            file=file, file_type=file_type, dummy_override=True
            ),
        'data': concatenator.get_concatenated_data(),
        'headers': concatenator.get_concatenated_header(),
        'concat_list': concat_list,
        'concat_report': concatenator.get_concatenation_report(as_text=True),
        '_configs': configs
        }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_single_file_data(file, fallback=False):

    file_type = io.get_file_type(file=file)
    configs = io.get_file_type_configs(file_type=file_type)
    return {
        'file_type': file_type,
        'file_info': io.get_file_info(file=file, file_type=file_type),
        'data': io.get_data(file=file, file_type=file_type),
        'headers': io.get_header_df(file=file, file_type=file_type),
        'concat_list': [],
        'concat_report': [] if not fallback else ['No eligible files found!'],
        '_configs': configs
        }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_io_implemented(input_format, output_format=None):

    configs = io.get_file_type_configs()
    if not input_format in configs:
        raise NotImplementedError(
            f'Input format {input_format} is not supported!'
            )

    if not output_format:
        return

    if not output_format in configs:
        raise NotImplementedError(
            f'Output format {output_format} is not supported!'
            )

    if input_format == 'TOA5':
        if output_format == 'EddyPro':
            raise NotImplementedError('No!')
#------------------------------------------------------------------------------
