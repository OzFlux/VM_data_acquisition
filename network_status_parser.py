# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 10:38:32 2024

@author: jcutern-imchugh
"""

import datetime as dt
import numpy as np
import pathlib

import geojson
import pandas as pd

import data_mapper as dm
import data_parser as dp
import paths_manager as pm
import sparql_site_details as sd


VARIABLE_LIST = ['Fco2', 'Fh', 'Fe', 'Fsd']
site_list = dm.get_mapped_site_list()


def make_status_geojson(write=False):
    """
    Generate a geojson file with feature collection containing coordinates and
    record / flux status for each feature (site), and append a run date time
    as a foreign member.

    Parameters
    ----------
    write : bool, optional
        If true, write to default location without return.
        The default is False.

    Returns
    -------
    json_obj : geojson.feature.FeatureCollection
        The geojson feature collection.

    """

    site_list = dm.get_mapped_site_list()
    reference_data = sd.make_df()
    json_obj = geojson.FeatureCollection(
        [
            geojson.Feature(
                id=site,
                geometry=geojson.Point(coordinates=[
                    reference_data.loc[site, 'latitude'],
                    reference_data.loc[site, 'longitude']
                    ]),
                properties=_get_station_status(site)
                )
            for site in site_list
            ]
        )
    json_obj['metadata'] = {
        'rundatetime':
            dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
            }
    if not write:
        return json_obj
    _write_status_geojson(rslt=json_obj)

def _write_status_geojson(rslt):
    """
    Write to default location.

    Parameters
    ----------
    rslt : geojson object
        The data to be written, in geojson format.

    Returns
    -------
    None.

    """

    output_path = (
        pm.GenericPaths().local_resources.network_status / 'network_status.json'
        )
    with open(file=output_path, mode='w', encoding='utf-8') as f:
        geojson.dump(
            rslt,
            f,
            indent=4
            )

def _get_station_status(site):
    """
    Get status of called site (days since last record, days since good flux).

    Parameters
    ----------
    site : str
        The site / station for which to return the status report.

    Returns
    -------
    rslt_dict : dict
        The status for last record and fluxes.

    """

    parser = dp.SiteDataParser(site=site, concat_files=False)
    rslt_dict = {
        'days_since_last_record': (
            parser.get_record_stats_by_file()
            ['days_since_last_record']
            )
        }
    rslt_dict.update(
        parser.get_record_stats_by_variable(variable_list=VARIABLE_LIST)
        ['days_since_last_valid_record']
        .to_dict()
        )
    return rslt_dict

#--------------------------------------------------------------------------
def get_10Hz_status():
    """
    Get a summary of most recent 10Hz data.

    Returns
    -------
    pd.core.frame.DataFrame
        The dataframe.

    """

    files, days = [], []
    for site in site_list:
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
        zip(site_list, files, days),
        columns=['site', 'file_name', 'days_since_last_record']
        )
#--------------------------------------------------------------------------


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
        Set critical attributes. Just server time, as reference for site-based
        time since records.

        Returns
        -------
        None.

        """

        self.server_time = dt.datetime.now()

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

        paths = pm.Paths()
        temp_path = paths.get_local_path(
            resource='data', stream='flux_slow', as_str=True
            )
        table_df = dm.make_table_df(logger_info=True, extended_info=True)
        data_list = []
        for file in table_df.index:
            site = table_df.loc[file, 'site']
            full_path = pathlib.Path(
                temp_path.replace(paths._placeholder, site)
                ) / file
            data_list.append(
                dp.get_file_record_stats(
                    file=full_path,
                    site_time=_get_site_time(site=site, time=self.server_time),
                    concat_files=False
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
            '10Hz_files': get_10Hz_status()
            }
        key_table = _get_key_details()

        # Write sheets
        with pd.ExcelWriter(path=dest) as writer:

            # For the Summary and 10Hz_files tables...
            for sheet_name, data in iter_dict.items():

                # Prepend the run date and time to the spreadsheet
                _write_time_frame(
                    xl_writer=writer,
                    sheet=sheet_name,
                    time=self.server_time
                    )

                # Output and format the results
                (
                    data.style.apply(_get_style_df, axis=None)
                    .to_excel(
                        writer, sheet_name=sheet_name, index=False, startrow=1
                        )
                    )
                _set_column_widths(
                    df=data, xl_writer=writer, sheet=sheet_name
                    )

            # Iterate over sites...
            for site in site_list:

                print (site)
                # Prepend the run date and time to the spreadsheet
                _write_time_frame(
                    xl_writer=writer,
                    sheet=site,
                    time=_get_site_time(site=site, time=self.server_time)
                    )

                # Output and format the results
                site_df = (
                    dp.SiteDataParser(site=site, concat_files=False)
                    .get_record_stats_by_variable()
                    )
                (
                    site_df.style.apply(
                        _get_style_df,
                        column_name='days_since_last_valid_record',
                        axis=None
                        )
                    .to_excel(
                        writer, sheet_name=site, startrow=1, index=False
                        )
                    )
                _set_column_widths(
                    df=site_df, xl_writer=writer, sheet=site
                    )

            # Output the colour key

            # Set sheet name
            sheet_name = 'Key'

            # Output and format the results
            (
                key_table.style.apply(_get_key_formatter, axis=None)
                .to_excel(writer, sheet_name=sheet_name, index=False)
                )
            _set_column_widths(
                df=key_table, xl_writer=writer, sheet=sheet_name
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
            use_time = _get_site_time(site=site, time=use_time)
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

#------------------------------------------------------------------------------
### Begin time handling section ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_time(site, time):
    """
    Correct server time to site-based local standard time.

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
        time -
        dt.timedelta(
            hours=
            10 -
            sd.site_details().get_single_site_details(site, 'UTC_offset')
            )
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### End time handling section ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Begin Excel formatters section ###
#------------------------------------------------------------------------------

#--------------------------------------------------------------------------
def _get_key_details():
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

#------------------------------------------------------------------------------
def _get_style_df(df, column_name='days_since_last_record'):
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_key_formatter(df):
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
    return _get_style_df(df=this_df, column_name='colour')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_column_widths(df, xl_writer, sheet, add_space=2):
    """
    Set the xl column widths for whatever is largest (header or content).

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
#------------------------------------------------------------------------------

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

#------------------------------------------------------------------------------
def _write_time_frame(xl_writer, sheet, time, zone=None):
    """
    Write the time to the first line of the output spreadsheet.

    Parameters
    ----------
    xl_writer : TYPE
        The xlwriter object.
    sheet : str
        Name of the spreadsheet.
    time : pydatetime
        Time to write to spreadsheet
    zone : str, optional
        Time zone, if passed. The default is None.

    Returns
    -------
    None.

    """

    if zone is None:
        zone = ''
    frame = (
        pd.DataFrame(
            [f'RUN date/time: {time.strftime("%Y-%m-%d %H:%M")} {zone}'],
            index=[0]
            )
        .T
        )
    frame.to_excel(
        xl_writer, sheet_name=sheet, index=False, header=False
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### End Excel formatters section ###
#------------------------------------------------------------------------------

###############################################################################
### END STATUS CONSTRUCTOR CLASS ###
###############################################################################
