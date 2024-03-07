# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 16:09:49 2024

@author: jcutern-imchugh
"""

import datetime as dt
import json
import requests

import pandas as pd

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ALLOWED_QUERY_MODES = [
    'most-recent', 'date-range', 'since-time', 'since-record', 'backfill'
    ]
ENCAPS_CMD_STR = 'http://<IP_addr>/?command=<cmd>&format=json'
GENERIC_DATA_QUERY = 'dataquery&uri=dl:<table><variable>&mode=<mode>'


class CSILoggerMonitor():

    def __init__(self, IP_addr):

        self.IP_addr = IP_addr

    def check_logger_clock(self):
        """
        Check the logger clock.

        Returns
        -------
        dict
            Logger time.

        """

        return _do_request(IP_addr=self.IP_addr, cmd_str='ClockCheck')

    def get_data_by_date_range(
            self, start_date, end_date, table, variable=None
            ):
        """
        Get table data between start and end dates.

        Parameters
        ----------
        start_date : str or pydatetime
            Start date.
        end_date : str or pydatetime
            End date.
        table : str
            Name of table for which to return data.
        variable : str, optional
            Name of variable for which to return data. If None, returns the
            content of the entire table. The default is None.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the data.

        """

        cmd_str = _data_query_builder(
            mode='date-range',
            config_str=(
                f'&p1={_convert_time_to_logger_format(time=start_date)}'
                f'&p2={_convert_time_to_logger_format(time=end_date)}'
                ),
            table=table,
            variable=f'.{variable}' if variable else ''
            )
        return _retrieve_data(IP_addr=self.IP_addr, cmd_str=cmd_str)

    def get_data_since_date(self, start_date, table, variable=None):
        """
        Get table data after given date.

        Parameters
        ----------
        start_date : str or pydatetime
            Start date.
        table : str
            Name of table for which to return data.
        variable : str, optional
            Name of variable for which to return data. If None, returns the
            content of the entire table. The default is None.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the data.

        """

        cmd_str = _data_query_builder(
            mode='since-time',
            config_str = (
                f'&p1={_convert_time_to_logger_format(time=start_date)}'
                ),
            table=table,
            variable=f'.{variable}' if variable else ''
            )
        return _retrieve_data(IP_addr=self.IP_addr, cmd_str=cmd_str)

    def get_n_records_back(self, table, recs_back=1, variable=None):
        """
        Get table data for a given number of records back from present.

        Parameters
        ----------
        table : str
            Name of table for which to return data.
        recs_back : int, optional
            The number of records to step back from present. The default is 1.
        variable : str, optional
            Name of variable for which to return data. If None, returns the
            content of the entire table. The default is None.

        Returns
        -------
        pd.core.frame.DataFrame
            Dataframe containing the data.

        """

        cmd_str = _data_query_builder(
            mode='most_recent',
            config_str=f'&p1={recs_back}',
            table=table,
            variable = f'.{variable}' if variable else ''
            )
        return _retrieve_data(IP_addr=self.IP_addr, cmd_str=cmd_str)

    def _retrieve_data(self, cmd_str):
        """
        Execution function that takes a pre-constructed data query command
        string, calls the request function and assembles the data.

        Parameters
        ----------
        cmd_str : str
            The command string.

        Returns
        -------
        pd.core.frame.DataFrame
            The returned data.

        """

        content = _do_request(IP_addr=self.IP_addr, cmd_str=cmd_str)
        init_df = (
            pd.DataFrame(content['head']['fields'])
            .drop(['type', 'settable'], axis=1)
            .set_index(keys='name')
            .fillna('')
            )
        var_list = ['TIMESTAMP', 'RECORD'] + init_df.index.tolist()
        data_list = []
        for record in content['data']:
            time = _convert_time_from_logger_format(time_str=record['time'])
            record_n = int(record['no'])
            data_list.append([time, record_n] + record['vals'])
        return (
            pd.DataFrame(
                data=data_list, columns=var_list
                )
            .set_index(keys='TIMESTAMP')
            )

    def get_tables(self, list_only=True):
        """
        Get a list of the tables available on the logger.

        Parameters
        ----------
        list_only : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        return self._retrieve_tables(list_only=list_only)

    def get_table_variables(self, table, list_only=True):

        return self._retrieve_tables(table=table, list_only=list_only)

    def _retrieve_tables(self, table=None, list_only=True):

        if table is None:
            table = ''
        cmd_str = f'browsesymbols&uri=dl:{table}'
        response = _do_request(IP_addr=self.IP_addr, cmd_str=cmd_str)
        data = (
            pd.DataFrame(response['symbols'])
            .drop('type', axis=1)
            .set_index(keys='name')
            )
        if list_only:
            return data.index.tolist()
        return data

def get_data_by_date_range(
    IP_addr, start_date, end_date, table, variable=None
    ):

    cmd_str = _data_query_builder(
        mode='date-range',
        config_str=(
            f'&p1={_convert_time_to_logger_format(time=start_date)}'
            f'&p2={_convert_time_to_logger_format(time=end_date)}'
            ),
        table=table,
        variable=f'.{variable}' if variable else ''
        )
    return _retrieve_data(IP_addr=IP_addr, cmd_str=cmd_str)

def _retrieve_data(IP_addr, cmd_str):
    """
    Takes a pre-constructed data query command string, calls the request
    function and assembles the data.

    Parameters
    ----------
    IP_addr : str
        The IP address of the logger.
    cmd_str : str
        The command string to be executed.

    Returns
    -------
    pd.core.frame.DataFrame
        The returned data.

    """
    content = _do_request(IP_addr=IP_addr, cmd_str=cmd_str)
    var_list = (
        ['TIMESTAMP', 'RECORD'] +
        pd.DataFrame(content['head']['fields']).name.tolist()
        )
    data_list = []
    for record in content['data']:
        time = _convert_time_from_logger_format(time_str=record['time'])
        record_n = int(record['no'])
        data_list.append([time, record_n] + record['vals'])
    return (
        pd.DataFrame(
            data=data_list, columns=var_list
            )
        .set_index(keys='TIMESTAMP')
        )

def _do_request(IP_addr, cmd_str):

    rslt = requests.get(
        (
            ENCAPS_CMD_STR
            .replace('<IP_addr>', IP_addr)
            .replace('<cmd>', cmd_str)
            ),
        stream=True
        )
    if not rslt.status_code == 200:
        raise RuntimeError(f'Failed (status code {rslt.status_code})!')
    return json.loads(rslt.content)

def _convert_time_to_logger_format(time):

    if isinstance(time, str):
        time = dt.datetime.strptime(time, TIME_FORMAT)
    format_str = TIME_FORMAT.replace(' ', 'T')
    return dt.datetime.strftime(time, format_str)

def _convert_time_from_logger_format(time_str):

    return dt.datetime.strptime(
        time_str.replace('T', ' '),
        TIME_FORMAT
        )

def _data_query_builder(mode, config_str, table, variable):

    return (
        GENERIC_DATA_QUERY
        .replace('<table>', table)
        .replace('<variable>', variable)
        .replace('<mode>', mode)
        + config_str
        )
