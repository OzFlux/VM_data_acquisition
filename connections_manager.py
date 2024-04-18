# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh
"""

import pandas as pd

import file_io as io
from paths_manager import GenericPaths as gp
import logger_functions as lf

MODEM_FIELDS = [
    'modem_type', 'modem_user', 'modem_pass', 'SIM_supplier', 'SIM_provider',
     'SIM_num', 'VPN_IP_addr', 'subnet_IP_addr', 'routable'
     ]
LOGGER_FIELDS = [
    'logger', 'logger_model', 'logger_serial_num', 'logger_MAC_addr',
    'logger_IP_addr', 'logger_TCP_port', 'logger_Pakbus_addr'
    ]

class ConnectionsManager():

    def __init__(self):

        df = (
              io.read_excel(
                  file=gp().local_resources.xl_connections_manager,
                  sheet_name='Connections'
                  )
              .set_index(keys='Site')
              .sort_index()
              )
        self.sites = df.index.unique().dropna().tolist()
        self.modem_fields = MODEM_FIELDS
        self.modem_table = df[MODEM_FIELDS]
        self.logger_fields = (
            [LOGGER_FIELDS[0]] +
            [x.replace('logger_', '') for x in LOGGER_FIELDS[1:]]
            )
        self.logger_table = (
            df[LOGGER_FIELDS]
            .rename(dict(zip(LOGGER_FIELDS, self.logger_fields)), axis=1)
            )

    def get_site_logger_list(self, site):

        return self.logger_table.loc[[site], 'logger'].tolist()

    def get_site_logger_details(self, site, logger=None, field=None):

        sub_df = self.logger_table.loc[[site]].set_index(keys='logger')
        if not logger is None:
            sub_df = sub_df.loc[logger]
        if not field is None:
            sub_df = sub_df[[field]]
        if not field:
            return sub_df
        return sub_df[field]

    def get_routable_sites(self):

        return (
            self.modem_table[self.modem_table.routable!=0]
            .index
            .unique()
            .tolist()
            )

class LoggerDataManager():

    def __init__(self, site):

        conns = ConnectionsManager()
        self.logger_details = conns.get_site_logger_details(site=site)
        self.lookup_table = self._build_expanded_lookup_table(
            )

    def _build_expanded_lookup_table(self):

        df_list = []
        for logger in self.logger_details.index:
            df = lf.build_lookup_table(
                IP_addr=self.logger_details.loc[logger, 'IP_addr']
                )
            df['logger'] = logger
            df_list.append(df)
        return (
            pd.concat(df_list)
            [['units', 'process', 'table', 'logger']]
            .fillna('')
            )

    def get_variable_attributes(self, variable, field=None):

        if not field:
            return self.lookup_table.loc[variable]
        return self.lookup_table.loc[variable, field]

    def get_table_variables(self, table, list_only=False):

        sub_df = (
            self.lookup_table.loc[self.lookup_table.table==table]
            .drop('table', axis=1)
            )
        if list_only:
            return sub_df.index.tolist()
        return sub_df
