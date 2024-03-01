# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 16:09:49 2024

@author: jcutern-imchugh
"""

import json
import requests

import pandas as pd

class CSILoggerMonitor():

    def __init__(self, IP_addr):

        self.IP_addr = IP_addr

    def check_logger_clock(self):

        response = requests.get(
            self._construct_command_string(cmd='ClockCheck')
            )
        return json.loads(response.content)

    def get_table_entries(self, table, recs_back=1):

        response = requests.get(
            self._construct_command_string(
                cmd=f'dataquery&uri=dl:{table}&mode=most-recent&p1={recs_back}'
                )
            )
        return (
            pd.concat(
                [pd.DataFrame(json.loads(response.content)['head']['fields']),
                 pd.DataFrame(
                     {'data': json.loads(response.content)['data'][0]['vals']}
                     )
                 ],
                axis=1
                )
            .set_index(keys='name')
            .drop('settable', axis=1)
            )



    def _construct_command_string(self, cmd):

        return f'http://{self.IP_addr}/?command={cmd}&format=json'