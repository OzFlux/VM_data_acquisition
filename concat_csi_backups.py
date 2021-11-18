# -*- coding: utf-8 -*-
"""
Created on Fri Sep 10 11:50:27 2021

@author: jcutern-imchugh
"""

import datetime as dt
import pandas as pd
import pathlib
import pdb
import sys

header_list = ['Station', 'Variables', 'Units', 'Operations']
station_list = ['Format', 'Lgrnet_station_name', 'logger_type', 'serial_num', 
                'OS', 'prog_name', 'prog_sig', 'table_name']

class TOA5_maker():
    
    def __init__(self, dataframe):
        
               
        pass
    




def dtstr_constructor(pydt):
    
    return dt.datetime.strftime(pydt, '"%Y-%m-%d"')


class file_grouper():
    
    def __init__(self, file_path, search_str):
        
        self.file_path = pathlib.Path(file_path)
        if not self.file_path.exists(): 
            raise FileNotFoundError('Path does not exist!')
        self.search_str = search_str

    # def compare_variables(self, for_files=None):
        
    #     for 

    def cross_check_level(self, level):
        
        if not level in header_list:
            raise KeyError('"level" arg must be one of: {}'
                           .format(header_list[1:]))
        
        vars_dict = self.get_headers(levels=level)
        # return vars_dict
        element_list = []
        for this_entry in vars_dict:
            element_list.append(vars_dict[this_entry][level].split(','))
        return pd.DataFrame(element_list, index=vars_dict.keys())
            
    def get_files(self, no_dir=False):
        
        if not no_dir:
            return list(self.file_path.glob('*{}*'.format(self.search_str)))
        return [x.name for x in self.file_path.glob('*{}*'.format(self.search_str))]
    
    def get_headers(self, for_files=None, levels=None):

        if not levels:
            level_list = header_list
        else:
            if isinstance(levels, str): levels = [levels]
            level_list = [x for x in levels if x in header_list]
            if not level_list:
                raise KeyError('None of passed level names are valid')
        file_list = self.get_files()
        headers_dict = {}
        for this_file in file_list:
            with open(this_file) as file_obj:
                lines_list = [next(file_obj) for x in range(len(header_list))]
            sub_dict = {x: lines_list[header_list.index(x)] for x in level_list}
            headers_dict[this_file.name] = sub_dict
        return headers_dict

    def get_station_details(self):
        
        stations_dict = self.get_headers(levels='Station')
        details_list = []
        for this_entry in stations_dict:
            details = stations_dict[this_entry]['Station'].split(',')
            details_list.append(dict(zip(station_list, details)))
        return pd.DataFrame(data=details_list, index=stations_dict.keys())