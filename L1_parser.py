# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 15:08:15 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import csv
import datetime as dt
import numpy as np
import os
import pandas as pd
import pathlib
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------



#------------------------------------------------------------------------------
### INITIALISATIONS ###
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

class L1_constructor():
    
    def __init__(self, input_path, file_substr_list, freq=30, output_path=None,
                 n_header_lines=4):
        
        self.substr_list = file_substr_list
        self.freq = freq
        self.n_header_lines = n_header_lines
        self.input_path = pathlib.Path(input_path)
        if not self.input_path.exists():
            raise FileNotFoundError('Input directory not found!')
        if output_path:
            self.output_path = pathlib.Path(output_path)        
            if not self.output_path.exists():
                raise FileNotFoundError('Output directory not found!')
        else:
            self.output_path = self.input_path

    def get_date_min(self):
        
        dates_df = self.get_file_dates()
        return dates_df.start_date.min()
                
    def get_date_max(self):
        
        dates_df = self.get_file_dates()
        return dates_df.end_date.max()
    
    def get_date_range(self):
        
        return pd.date_range(
            start=self.get_date_min(), end=self.get_date_max(), 
            freq=str(self.freq) + 'T'
            )
    
    def get_file_list(self, complete_path=False):
        
        file_list = []
        for f in self.input_path.glob('*.dat'):
            for s in self.substr_list:
                if s in str(f):
                    file_list.append(f)
        if complete_path:
            return file_list
        return [x.name for x in file_list]
    
    def get_file_data(self, file_name, std_dates=False):
        
        self._check_file_name(file_name=file_name)
        df = pd.read_csv(
            self.input_path / file_name, skiprows=[0,2,3], 
            parse_dates=['TIMESTAMP'], index_col=['TIMESTAMP'], 
            na_values='NAN', sep=',', engine='c', on_bad_lines='warn')
        df.drop_duplicates(inplace=True)
        if std_dates:
            new_index = self.get_date_range()
            df = df.reindex(new_index)
        return df

    def get_file_dates(self):
        
        file_list = self.get_file_list(complete_path=True)
        dates_list = []
        for file in file_list:
            dates_list.append(get_file_dates(file=file))
        return pd.DataFrame(data=dates_list, index=[x.name for x in file_list])
        
    def get_file_headers(self, file_name, as_frame=False):
        
        self._check_file_name(file_name)
        headers = []
        with open(self.input_path / file_name) as f:
            for i in range(self.n_header_lines):
                headers.append(f.readline())
        if not as_frame:
            return headers
        header_list = (
            [x.replace('"', '').strip().split(',') for x in headers]
            )
        padded_header = (
            header_list[0] + 
            [''] * (len(header_list[1]) - len(header_list[0]))
            )
        header_list = [padded_header] + header_list[1:]
        headers_df = pd.DataFrame(
            data=header_list, 
            index=['Program Info', 'Name', 'Units', 'Sampling']
            )
        return headers_df
                
    def write_to_excel(self, file_name=None, na_values='', std_dates=True):
        
        if file_name:
            self._check_file_name(file_name=file_name)
            file_list = [file_name]
        else:
            file_list = self.get_file_list()
        with pd.ExcelWriter(self.output_path / 'filename.xlsx') as writer:
            for file in file_list:
                print (f'Parsing file {file}...')
                headers_df = self.get_file_headers(file_name=file, as_frame=True)
                df = self.get_file_data(file_name=file, std_dates=std_dates).reset_index()
                headers_df.to_excel(writer, sheet_name=file, index=False)
                df.to_excel(writer, sheet_name=file, header=False, index=False, 
                            startrow=5, na_rep=na_values)
                
    def _check_file_name(self, file_name):
        
        file_list = self.get_file_list()
        if not file_name in file_list:
            raise FileNotFoundError('File not found!')

def get_file_dates(file):
    
    date_format = '"%Y-%m-%d %H:%M:%S"'
    with open(file, 'rb') as f:
        while True:
            line_list = f.readline().decode().split(',')
            try:
                start_date = (
                    dt.datetime.strptime(line_list[0], date_format)
                    )
                break
            except ValueError:
                pass            
        f.seek(2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line_list = f.readline().decode().split(',')
        end_date = dt.datetime.strptime(last_line_list[0], date_format)
    return {'start_date': start_date, 'end_date': end_date}

in_path = 'E:/Sites/Calperum/Flux/Slow'
out_path = 'E:/Test_site'
substr_list = ['core', 'extras', 'flux', 'met', 'rad', 'soil']
ob = L1_constructor(
    input_path=in_path, output_path=out_path, file_substr_list=substr_list
    )