# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh
"""

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
        self.modem_table = df[MODEM_FIELDS]
        self.modem_fields = MODEM_FIELDS
        self.logger_table = (
            df[LOGGER_FIELDS]
            .rename(
                dict(zip(
                    LOGGER_FIELDS[1:],
                    [x.replace('logger_', '') for x in LOGGER_FIELDS[1:]]
                    )),
                axis=1
                )
            )
        self.logger_fields = LOGGER_FIELDS

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

    def __init__(self, site, logger):

        conns = ConnectionsManager()
        self.logger_details = conns.get_site_logger_details(
            site=site, logger=logger
            )
        self.lookup_table = lf.build_lookup_table(
            IP_addr=self.logger_details['IP_addr']
            )

    # def
