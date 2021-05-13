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
import pathlib
import pdb
import subprocess as spc
import sys
# import warnings
# warnings.filterwarnings("error")

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
        self.overwrite_ccf = overwrite_ccf
        self.process_flag = 0
        if fmt == 'TOA5': self.bale_interval = int(data_intvl_mins)
        if fmt == 'TOB1': self.bale_interval = 1440

    #-------------------------------------------------------------------------
    ### Class methods tied to CardConvert ###
    #-------------------------------------------------------------------------

    def get_unparsed_files(self):
        
        """Check for unparsed TOB3 files in TMP directory"""
        
        raw_files = _get_files_of_type(site=self.SiteName, file_type='TOB3')
        if self.process_flag: return raw_files
        all_files = _get_files_of_type(site=self.SiteName)
        if not len(all_files) == len(raw_files):
            raise RuntimeError('Non-TOB3 files present in TMP directory! '
                               'Please remove these files and try again!')
        return raw_files

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
        
        return ( _check_path(path=base_ccf_path.format(self.SiteName)) / 
                '{0}_{1}.ccf'.format(self.format, self.SiteName))

    def get_data_path(self):

        """Return the site-specific data path"""        

        return _check_path(path=base_data_path.format(self.SiteName),
                           subdirs=['TMP'])

    def get_n_expected_parsed_files(self):
        
        n_input_files = len(self.get_unparsed_files())
        if self.format == 'TOA5': 
            return n_input_files * int(1440 / self.bale_interval) 
        return n_input_files

    def get_parsed_files(self):
        
        return _get_files_of_type(self.SiteName, file_type=self.format)

    def run_ccf_file(self):
        
        """Run CardConvert"""
        
        if self.process_flag: raise RuntimeError('Process has already run!')
        self.write_ccf_file()
        cc_path = str(_check_path(CardConvert_path))
        spc_args = [cc_path, 'runfile={}'.format(self.get_ccf_filename())]
        rslt = spc.run(spc_args)
        if not rslt.returncode == 0:
            raise RuntimeError('Command line execution of CardConvert failed!')
        self.process_flag = 1

    def write_ccf_file(self):

        """Output constructed ccf file to disk"""        
        
        if self.process_flag: raise RuntimeError('Process has already run!'
                                                 'Illegal to change ccf file!')
        write_dict = self.get_ccf_dict()
        output_file = self.get_ccf_filename()
        if not self.overwrite_ccf: 
            if output_file.exists(): return
            else: logging.warning('No ccf file found! Creating...')
        with open(output_file, mode='w') as f:
            f.write('[main]\n')
            for this_item in write_dict:
                f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))
                
    def get_change_map(self):
    
        """Get map of old to new file names"""    
    
        if not self.process_flag: 
            raise RuntimeError('CardConvert processing has not run yet!')
        input_file_list = self.get_parsed_files()
        output_file_list = (
            [_get_dirname(f) / _get_filename(f, self.data_interval)
             for f in input_file_list]
            )
        data_path = self.get_data_path()
        input_file_list = ([data_path / f for f in input_file_list])
        return list(zip(input_file_list, output_file_list))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class rename_parser():
    
    def __init__(self, sitename, fmt, data_intvl_mins):
        
        self.SiteName = sitename        
        self.format = _test_format(fmt)
        self.data_interval = int(data_intvl_mins)

    def get_unparsed_files(self):
        
        """Check for unparsed files of <format> in TMP directory"""

        return _get_files_of_type(self.SiteName, file_type=self.format)
        
    def get_data_path(self):

        """Return the site-specific data path"""        

        return _check_path(path=base_data_path.format(self.SiteName),
                           subdirs=['TMP'])

    def get_required_dirs(self):
        
        file_list = self.get_unparsed_files()
        dirs_list = list(set([_get_dirname(f) for f in file_list]))
        return dirs_list
        return [d for d in dirs_list if not d.exists()]
        
    def get_change_map(self):
    
        """Get map of old to new file names"""    
    
        input_file_list = self.get_unparsed_files()
        output_file_list = (
            [_get_dirname(f) / _get_filename(f, self.data_interval)
             for f in input_file_list]
            )
        data_path = self.get_data_path()
        input_file_list = ([data_path / f for f in input_file_list])
        return list(zip(input_file_list, output_file_list))
    
    def move_files(self, force_remove=False):
        
        """Rename and transfer files to final directories"""
        
        for d in self.get_required_dirs():
            try: d.mkdir(parents=True)
            except FileExistsError: continue
        change_list = self.get_change_map()
        for this_pair in change_list:
            if this_pair[1].is_file():
                if not force_remove: 
                    raise FileExistsError(
                        'File {0} already exists in directory {1}'
                        .format(this_pair[1].name, this_pair[1].parent)
                        )
                this_pair[0].unlink(); continue
            this_pair[0].rename(this_pair[1])
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
    return _check_path(path=base_data_path.format(site), 
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
def _check_path(path, subdirs=[], error_if_missing=True):
    
    """Check path exists \n
        * args:
            path (formatted path): """
            
    fmt_path = pathlib.Path(path)
    for item in subdirs: fmt_path = fmt_path / item
    if not error_if_missing: return fmt_path
    if not fmt_path.exists():
        raise FileNotFoundError('Specified file path ({}) does not exist! '
                                'Check site name or specified subdirectories!'
                                .format(str(fmt_path)))
    return fmt_path
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_logger(site, name=None):
    
    """Create logger and send output to file"""
    
    logger_dir_path = (
        _check_path(path=base_data_path.format(site), subdirs=['LOG'])
        )
    logger_file_path = logger_dir_path / '{}_fast_data.log'.format(site)
    if not name: logger = logging.getLogger(name=__name__)
    else: logger = logging.getLogger(name=name)
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
def _get_files_of_type(site:str, file_type=None, error_if_zero=True) -> list:

    """
    Return a list of the files contained in the TMP directory
    
    The list can be for files of a given format, or None (returns all)
    
    Parameters
    ----------
    site : str 
        site to process
    file_type : None, str
        format of files to be returned (default None; otherwise must be
        TOA5, TOB1, TOB3)
    error_if_zero : bool
        raise error if no files (default True)
    
    Raises
    -------
    RunTimeError
        if there are no files in the directory, unless error_if_zero set to
        false
    
    Returns
    -------
    list
        list of files of file_type in directory
    """    

    files = [x.name for x in _check_path(path=base_data_path.format(site), 
                                         subdirs=['TMP']).glob('*.dat')]
    if not file_type: 
        subset_files = files
    elif file_type == 'TOB3':
        subset_files = [x for x in files if x.split('_')[0] == site]
    else:
        _test_format(fmt_str=file_type)
        subset_files = [x for x in files if x.split('_')[0] == file_type]
    if not subset_files and error_if_zero:
        raise FileNotFoundError('Directory contains no files of type {}!'
                                .format(file_type))
    return subset_files    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_raw(site):
    
    files_to_move = _get_files_of_type(site, file_type='TOB3')
    from_dir =_check_path(path=base_data_path.format(site), subdirs=['TMP'])
    to_dir =_check_path(path=base_data_path.format(site), subdirs=['TOB3'])
    for f in files_to_move: (from_dir / f).rename(to_dir / f)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Main ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__ == "__main__": 

    site, interval = sys.argv[1], sys.argv[2]
    logger = _set_logger(site=site)
    logger.info('Begin processing for site {}'.format(site))
    for out_format in ['TOA5', 'TOB1']:
        logger.info('Running {} format conversion with CardConvert_parser'
                    .format(out_format))
        try:
            cc_parser = CardConvert_parser(sitename=site, fmt=out_format, 
                                           data_intvl_mins=interval)
            cc_parser.run_ccf_file()
            n_expected = cc_parser.get_n_expected_parsed_files()
            n_produced = len(cc_parser.get_parsed_files())
            msg = ('{0} format conversion complete: expected {1} converted '
                   'files, got {2}!'.format(out_format, n_expected, n_produced))
            if n_expected == n_produced: logger.info(msg)
            else: logger.warning(msg)
        except Exception as e:
            logger.error('CardConvert processing failed! Message: {}'
                         .format(e)); sys.exit()
        logger.info('Running file renaming with rename_parser')
        try:
            rnm_parser = rename_parser(sitename=site, fmt=out_format, 
                                       data_intvl_mins=interval)
            rnm_parser.move_files()
            logger.info('File renaming complete')
        except Exception as e:
            logger.error('File renaming failed! Message: {}'
                         .format(e)); sys.exit()
    logger.info('Moving TOB3 files to final storage')
    try:
        move_raw(site=site)
        logger.info('Completed move of raw data... sweet dreams')
    except Exception as e:
        logger.error('File move failed! Message: {}'.format(e)); sys.exit()