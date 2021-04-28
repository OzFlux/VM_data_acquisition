#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 09:16:49 2021

@author: imchugh

To add:
    - logger and comments to log file
    - move raw file out of TMP directory (delete? rename and archive?)
    - cross_check directories to ensure all files copied 
"""

import datetime as dt
import logging
import numpy as np
import os
import pathlib
import pdb
import subprocess as spc
import sys

#------------------------------------------------------------------------------
### Paths and variables ###
#------------------------------------------------------------------------------
CardConvert_path = r'C:/Program Files (x86)/Campbellsci/CardConvert/CardConvert.exe'
base_ccf_path = r'E:/Cloudstor/Network_documents/CCF_files'
base_data_path = r'E:/Cloudstor/Shared/EcoSystemProcesses/Sites/{}/Data/Flux/Raw/Fast/'
init_dict = {'Format': '0', 'FileMarks': '0', 'RemoveMarks': '0', 'RecNums': '0',
             'Timestamps': '1', 'CreateNew': '0', 'DateTimeNames': '1',
             'Midnight24': '0', 'ColWidth': '263', 'ListHeight': '288',
             'ListWidth': '190', 'BaleCheck': '1', 'CSVOptions': '6619599',
             'BaleStart': '38718', 'BaleInterval': '32875',
             'DOY': '0', 'Append': '0', 'ConvertNew': '0'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CCF exec class ###
#------------------------------------------------------------------------------
class CardConvert_parser():

    """Creates, writes and runs CardConvert format (CCF) files"""    

    def __init__(self, sitename, fmt, data_intvl_mins, overwrite_ccf=True):

        _test_format(fmt)
        self.format = fmt
        self.SiteName = sitename        
        self.files_to_process = self.check_unparsed_files()
        self.overwrite_ccf = overwrite_ccf
        if fmt == 'TOA5': self.bale_interval = int(data_intvl_mins)
        if fmt == 'TOB1': self.bale_interval = 1440

    def check_unparsed_files(self):
        
        """Check for unparsed TOB3 files in TMP directory"""
        
        target_path = self.get_data_path()
        file_list = []
        for f in os.listdir(target_path):
            site = f.split('_')[0]
            if site != self.SiteName: continue
            file_list.append(f)
        if not file_list: 
            raise RuntimeError('No files in TMP directory to parse!')
        return file_list

    def get_bale_interval(self):
        
        """Return the formatted interval for file baling"""
        
        bale_interval = 1 / (1440 / self.bale_interval)
        if bale_interval == 1: return init_dict['BaleInterval']
        if bale_interval > 1:
            raise RuntimeError('Error! Minutes must sum to less than 1 day!')
        return str(int(init_dict['BaleInterval']) - 1 + round(bale_interval, 10))

    def get_ccf_dict(self):
        
        """Return the dictionary with output-ready values"""
        
        if self.format == 'TOA5': format_str = '0'
        if self.format == 'TOB1': format_str = '1'
        write_dict = {'SourceDir': str(self.get_data_path()) + '\\',
                      'TargetDir': str(self.get_data_path()) + '\\'}
        write_dict.update(init_dict)
        write_dict['BaleInterval'] = self.get_bale_interval()
        write_dict['Format'] = format_str
        return write_dict

    def get_ccf_filename(self):

        """Return the site-specific ccf file path"""        
        
        return ( _set_check_path(path=base_ccf_path.format(self.SiteName)) / 
                '{0}_{1}.ccf'.format(self.format, self.SiteName))

    def get_data_path(self):

        """Return the site-specific data path"""        

        return _set_check_path(path=base_data_path.format(self.SiteName),
                               subdirs=['TMP'])

    def run_ccf_file(self):
        
        """Run CardConvert"""
        
        self.write_ccf_file()
        cc_path = str(_set_check_path(CardConvert_path))
        spc_args = [cc_path, 'runfile={}'.format(self.get_ccf_filename())]
        rslt = spc.run(spc_args)
        if not rslt.returncode == 0:
            raise RuntimeError('Command line execution of CardConvert failed!')

    def write_ccf_file(self):

        """Output constructed ccf file to disk"""        

        write_dict = self.get_ccf_dict()
        output_file = self.get_ccf_filename()
        if not self.overwrite_ccf: 
            if output_file.exists(): return
            else: logging.warning('No ccf file found! Creating...')
        with open(output_file, mode='w') as f:
            f.write('[main]\n')
            for this_item in write_dict:
                f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class file_parser():
    
    def __init__(self, sitename, fmt, data_intvl_mins):
        
        self.SiteName = sitename        
        self.format = _test_format(fmt)
        self.data_interval = int(data_intvl_mins)

    def check_unparsed_files(self):
        
        """Check for unparsed files of <format> in TMP directory"""
        
        target_path = self.get_data_path()
        file_list = []
        for f in os.listdir(target_path):
            fmt, site = f.split('_')[0], f.split('_')[1]
            if site != self.SiteName: continue
            if fmt != self.format: continue
            file_list.append(f)
        if not file_list: 
            raise RuntimeError('No {} files in TMP directory to parse!'
                               .format(self.format))
        return file_list
        
    def get_data_path(self):

        """Return the site-specific data path"""        

        return _set_check_path(path=base_data_path.format(self.SiteName),
                               subdirs=['TMP'])

    def get_required_dirs(self):
        
        file_list = self.check_unparsed_files()
        dirs_list = list(set([_get_dirname(f) for f in file_list]))
        return dirs_list
        return [d for d in dirs_list if not d.exists()]
        
    def get_change_map(self):
    
        """Get map of old to new file names"""    
    
        input_file_list = self.check_unparsed_files()
        output_file_list = (
            [_get_dirname(f) / _get_filename(f, self.data_interval)
             for f in input_file_list]
            )
        data_path = self.get_data_path()
        input_file_list = ([data_path / f for f in input_file_list])
        return list(zip(input_file_list, output_file_list))
    
    def move_files(self):
        
        """Rename and transfer files to final directories"""
        for d in self.get_required_dirs():
            try: d.mkdirs()
            except FileExistsError: pass
        change_list = self.get_change_map()
        for this_pair in change_list: os.rename(this_pair[0], this_pair[1])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### File handling functions ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_dirname(file_name):
    
    """Get the required child directory name for the file"""
    
    split_list = file_name.split('_')
    fmt, site = split_list[0], split_list[1]
    month, day = '_'.join(split_list[4:6]), split_list[6]
    sub_dirs_list = [fmt, month]
    if fmt == 'TOA5': sub_dirs_list += [day]
    return _set_check_path(path=base_data_path.format(site), 
                           subdirs=sub_dirs_list, error_if_missing=False)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_filename(file_name, data_interval):
    
    """Get the new filename (roll time forward for TOA5 files)"""
    
    split_list = file_name.split('_')
    split_list = split_list[:3] + split_list[4:]
    fmt, site, freq = split_list[0], split_list[1], split_list[2]
    if fmt == 'TOB1': return '_'.join(split_list[:-1]) + '.dat'
    year, month, day = int(split_list[3]), int(split_list[4]), int(split_list[5])
    time = split_list[-1].split('.')[0]
    hour, minute = int(time[:2]), int(time[2:])
    delta_mins = _get_rounded_mins(int(minute), data_interval)
    py_datetime = (
        dt.datetime(year, month, day, hour, 0) + dt.timedelta(minutes=delta_mins)
        )
    str_datetime = dt.datetime.strftime(py_datetime, '%Y_%m_%d_%H%M')
    return '_'.join([fmt, site, freq, str_datetime]) + '.dat'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_rounded_mins(mins, interval):
    
    """Roll forward minutes to end of relevant interval"""
    
    return (mins - np.mod(mins, interval) + interval).item()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_check_path(path, subdirs=[], error_if_missing=True):
    
    """Check path exists \n
        * args:
            path (formatted path): """
            
    fmt_path = pathlib.Path(path)
    for item in subdirs: fmt_path = fmt_path / item
    if not error_if_missing: return fmt_path
    if not fmt_path.exists():
        raise FileNotFoundError('Specified file path does not exist! '
                                'Check site name or specified subdirectories!')
    return fmt_path
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_logger(site, name=None):
    
    """Create logger and send output to file"""
    
    logger_dir_path = (
        _set_check_path(path=base_data_path.format(site), subdirs=['LOG'])
        )
    logger_file_path = logger_dir_path / '{}_fast_data.log'.format(site)
    if not name: 
        logger = logging.getLogger(name=__name__)
    else:
        logger = logging.getLogger(name=name)
    logger.setLevel(logging.DEBUG)
    if not logger.hasHandlers():
        handler = logging.FileHandler(logger_file_path)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _test_format(fmt_str): 
    
    """Check the passed format argument is valid (only TOA5 or TOB1)"""
    
    try:
        assert fmt_str in ['TOA5', 'TOB1']
    except AssertionError:
        raise TypeError('Error: "fmt" argument must be "TOA5" or "TOB1"')
    return fmt_str
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Main ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__ == "__main__": 

    site, interval = sys.argv[1], sys.argv[2]
    logger = _set_logger(site=site)
    base_site_path = _set_check_path(path=base_data_path.format(site), 
                                     subdirs=['TMP'])
    pdb.set_trace()
    files = [f for f in base_site_path.glob('*.dat') if 'TOA5' or 'TOB1' in f]
    logger.info()
    for out_format in ['TOA5', 'TOB1']:
        try:
            x = CardConvert_parser(sitename=site, fmt=out_format, 
                                   data_intvl_mins=interval)
            x.run_ccf_file()
            logger.info()
        except Exception as e:
            logger.error(e)
            sys.exit()
        try:
            x = file_parser(sitename=site, fmt=out_format, 
                            data_intvl_mins=interval)
            x.move_files()
        except Exception as e:
            print(e)
            sys.exit()