#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 20:13:00 2021

@author: imchugh
"""

import datetime as dt
import pandas as pd
import pathlib
import pdb

path = '/home/unimelb.edu.au/imchugh/Temp/site_master.xls'

class TOA5_file_constructor():

    def __init__(self, data, header=None, units=None, samples=None):

        try:
            if units:
                assert len(units) == len(data.columns)
            else:
                units = len(data.columns)
            if samples:
                assert len(samples) == len(units)
            else:
                samples = len(data.columns)
        except AssertionError:
            raise IndexError('Length of units, sampling and dataframe columns '
                             'must match!')
        self.data = data
        self.header = header
        self.units = units
        self.samples = samples

    def make_data(self, as_str=None):

        return make_TOA5_data(data=self.data)

    def make_header(self):

        return make_TOA5_header(header=self.header)

    def make_samples(self):

        return make_TOA5_line(list_or_int=self.samples, line_type='samples')

    def make_units(self):

        return make_TOA5_line(list_or_int=self.units, line_type='units')

    def make_variables(self):

        return make_TOA5_variables(df=self.data)

    def make_file_output(self):

        return [self.make_header(), self.make_variables(), self.make_units(),
                self.make_samples()] + self.make_data()

    def output_file(self, file_path):

        out_list = self.make_file_output()
        path = pathlib.Path(file_path)
        if not path.parent.exists():
            raise FileNotFoundError('Specified directory does not exist')
        with open(file_path, 'w') as f:
            f.writelines(out_list)

def make_TOA5_data(data):

    df = data.copy()
    try:
        date_index = df.index.to_pydatetime()
        df.index = [x.strftime('%Y-%m-%d %H:%M:%S') for x in date_index]
    except AttributeError:
        if len(df) > 1:
            raise RuntimeError('Dataframes with length > 1 must have a '
                               'datetime index!')
        date_index = [dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    date_index = ['"' + x + '"' for x in date_index]
    df.index = date_index
    df.reset_index(inplace=True)
    return [','.join(map(str, x)) + '\n' for x in df.values.tolist()]

def make_TOA5_header(header=None):

    default_header = ['TOA5', 'NoStation', 'CR1000', '9999',
                      'cr1000.std.99.99', 'CPU:noprogram.cr1', '9999',
                      'Site_details']
    if not header:
        header = default_header
    else:
        if not len(header) == 10:
            raise RuntimeError('Header must contain ten elements!')
    return ','.join(['"' + x + '"' for x in header]) + '\n'

def make_TOA5_line(list_or_int, line_type):

    if line_type == 'units':
        default_str = 'unitless'
        time_str = 'ts'
    elif line_type == 'samples':
        default_str = 'Smp'
        time_str = ''
    else:
        raise TypeError('line_type must be either "units" or "samples"')
    if isinstance(list_or_int, int):
        out_list = [time_str] + [default_str] * list_or_int
    elif isinstance(list_or_int, list):
        out_list = list_or_int
    out_list = ['"' + x + '"' for x in out_list]
    return ','.join(out_list) + '\n'

def make_TOA5_variables(df):

    return (
        ','.join(['"TIMESTAMP"'] +
                 ['"' + x + '"' for x in df.columns]) +
        '\n'
        )
