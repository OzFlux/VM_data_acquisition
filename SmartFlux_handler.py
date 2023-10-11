# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 09:19:21 2023

@author: jcutern-imchugh
"""

import pathlib

import numpy as np
import pandas as pd

path = 'E:/Scratch/eddypro_exp_full_output_2023-10-03T003056_exp.csv'

class file_data_handler():

    def __init__(self, file):

        self.headers = get_header_df(file=file)

    def get_header_groups(self):

        return (
            self.headers
            .index
            .get_level_values(level='group')
            .unique()
            .tolist()
            )

    def get_variable_list(self, group=None):

        if not group:
            return (
                self.headers
                .index
                .get_level_values(level='variable')
                .unique()
                .tolist()
                )
        return (
            self.headers.loc[group]
            .index
            .tolist()
            )

    def get_variable_units(self, variable):

        return (
            self.headers
            .droplevel(level='group')
            .loc[variable, 'units']
            )

    def map_variable_units(self, group=None):

        if not group:
            return dict(zip(
                self.headers.index.get_level_values(level='variable').tolist(),
                self.headers.units.tolist()
                ))
        return dict(zip(
            self.headers.loc[group].index.tolist(),
            self.headers.loc[group, 'units'].tolist()
            ))

def get_SmartFlux_data(file):

    return (
        pd.read_csv(path, skiprows=[0,2], parse_dates=[[1,2]])
        .rename({'date_time': 'TIMESTAMP'}, axis=1)
        .set_index(keys='TIMESTAMP')
        )

#------------------------------------------------------------------------------
def get_file_headers(file):

    """
    Get a list of the three SmartFlux header strings.

    Returns
    -------
    headers : list
        List of the raw header strings.

    """

    headers = []
    with open(file) as f:
        for i in range(3):
            headers.append(f.readline())
    return headers
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_header_df(file, usecols=None):
    """
    Get a dataframe with variables as index and units and statistical
    sampling type as columns.

    Returns
    -------
    pd.core.frame.DataFrame
        Dataframe as per above.

    """

    GROUP_LINE = 0
    VARIABLE_LINE = 1
    headers = get_file_headers(file=file)
    line_list = [line.strip().split(',') for line in headers]
    group_posns = np.array(
        [[x, i] for i, x in enumerate(line_list[GROUP_LINE]) if not x == '']
        )
    groups = group_posns[:, 0]
    posns = group_posns[:, 1].astype(int)
    n_per_group = (
        np.append(np.roll(posns, -1)[:-1], len(line_list[VARIABLE_LINE])) -
        posns
        )
    idx = (
        np.hstack(
            [np.tile(group_posns[i,0], x) for i, x in enumerate(n_per_group)]
            )
        .tolist()
        )
    return (
        pd.DataFrame(
            data=[idx, line_list[1], line_list[2]],
            index=['group', 'variable', 'units']
            )
        .T
        .set_index(['group', 'variable'])
        )
#------------------------------------------------------------------------------


