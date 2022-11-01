#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 09:16:49 2021

@author: imchugh

To do:
    - implement flexible file naming patterns
    - check works for TOB1 AND for different intervals (TOB1 at daily interval
      needs fixing)
"""

import datetime as dt
import logging
import numpy as np
import pathlib
import pdb
import subprocess as spc
import sys

import paths_manager as pm
sys.path.append('../site_details')
import sparql_site_details as sd

#------------------------------------------------------------------------------
### INITIALISATIONS ###
#------------------------------------------------------------------------------
PATHS = pm.paths()
DETAILS = sd.site_details()
CCF_DICT = {'Format': '0', 'FileMarks': '0', 'RemoveMarks': '0', 
            'RecNums': '0', 'Timestamps': '1', 'CreateNew': '0',
            'DateTimeNames': '1', 'Midnight24': '0', 'ColWidth': '263', 
            'ListHeight': '288', 'ListWidth': '190', 'BaleCheck': '1', 
            'CSVOptions': '6619599', 'BaleStart': '38718', 
            'BaleInterval': '32875', 'DOY': '0', 'Append': '0', 
            'ConvertNew': '0'}
FILENAME_FORMAT = {'Format': 0, 'Site': 1, 'Freq': 2, 'Year': 3, 'Month': 4, 
                   'Day': 5}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

class CardConvert_PreChecker():
    
    def __init__(self, site):
        
        # Assign attributes
        self.site = site
        self.ccf_filepath = PATHS.get_local_path(resource='ccf_config')
        self.ccf_filename = f'TOA5_{site}.ccf'
        self.raw_data_path = PATHS.get_local_path(
            resource='data', stream='flux_fast', subdirs=['TMP'], site=site
            )
        self.time_step = DETAILS.get_single_site_details('Calperum').time_step
        intvl = str(int(
            1000 / int(DETAILS.get_single_site_details('Calperum').freq_hz)
            ))
        self.raw_file_format = f'TOB3_{site}_{intvl}ms_YYYY_MM_DD.dat'
        self.proc_file_format = f'TOA5_{site}_{intvl}ms_YYYY_MM_DD_HHMM.dat'
    
    #--------------------------------------------------------------------------
    def _check_raw_file_name_format(self, file_name):
    
        """Check that file conforms to expected naming - no action if true,
            raise if false"""        
    
        # Split to lists for comparison of elements
        file, ext = file_name.split('.')
        elements = file.split('_')
        expected_elements = self.raw_file_format.split('.')[0].split('_')
        
        # Error checks: 
        # 1) ends with dat
        if not ext == 'dat':
            msg = f'File type unknown for file {file}'
            logging.error(msg) ; raise TypeError(msg)
            
        # 2) correct number of elements
        if not len(elements) == 6:
            msg = (f'File {file} does not conform to expected convention '
                   '({self.raw_file_format})'
                )
            logging.error(msg) ; raise TypeError(msg)

        # 3) elements match
        for i in range(3):
            if not elements[i] == expected_elements[i]:
                msg = (
                    'Element {0} ("{1}") in file name is not of required format '
                    'for file {2}; expected "{3}"'
                    .format(str(i), elements[i], file, expected_elements[i])
                    )
                logging.error(msg) ; raise TypeError()
                    
        # 4) parseable date 
        try:
            dt.datetime(int(elements[3]), int(elements[4]), int(elements[5]))
        except TypeError as e:
            logging.error(str(e)) ; raise
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_raw_files(self):
    
        # Get files (raise error if none)
        logging.info('Getting the raw files to parse...')
        files = [x.name for x in self.raw_data_path.glob('*')]
        if not files:
            msg = 'No raw files to parse!'
            logging.error(msg)
            raise FileNotFoundError(msg)

        # Get any archive files
        archive_path = PATHS.get_local_path(
            resource='data', stream='flux_fast', subdirs=['TOB3'], 
            site=self.site
            )
        processed_files = [x.name for x in archive_path.rglob('TOB3*.dat')]
        
        # Integrity checks
        for this_file in files:
            # Cross check hasn't been processed
            if this_file in processed_files:
                msg = f'File {this_file} already processed!'
                logging.error(msg)
                raise RuntimeError(msg)
            # Check conforms to naming convention
            self._check_raw_file_name_format(file_name=this_file)
        logging.info(
            'The following files will be parsed: {}'.format(', '.join(files))
            )
        return files
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def make_ccf_dict(self):



        # Construct the dictionary   
        src_dir = str(self.raw_data_path) + '\\'
        write_dict = {'SourceDir': src_dir, 'TargetDir': src_dir}
        write_dict.update(CCF_DICT)

        # Get and set the bale interval
        bale_intvl_frac = 1 / (1440 / self.time_step)
        if bale_intvl_frac > 1:
            raise RuntimeError('Error! Minutes must sum to less than 1 day!')
        bale_intvl = (
            str(int(CCF_DICT['BaleInterval']) - 1 + round(bale_intvl_frac, 10))
            )
        write_dict['BaleInterval'] = bale_intvl
        return write_dict
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def write_ccf_file(self, overwrite=True):
        
        output_file = self.ccf_filepath / self.ccf_filename
        if not overwrite:
            if output_file.exists():
                return
        write_dict = self._make_ccf_dict()
        with open(output_file, mode='w') as f:
            f.write('[main]\n')
            for this_item in write_dict:
                f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
### PUBLIC FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_raw_files(path):
    
    logging.info('Moving raw files')
    for file in path.glob('TOB3*.dat'):
        
        logging.info('    - {}'.format(file.name))
        to_dir = _make_dir(file_name=file.name)
        file.rename(to_dir / file.name)
    
    logging.info('Move of raw files complete')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def rename_and_move_proc_files(path, time_step):

    logging.info('Renaming and moving converted files')
    for file in path.glob('TOA5*.dat'):
        
        new_filename = _demangle_filename(file_name=file.name)
        to_dir = _make_dir(file_name=new_filename)
        new_filename = (
            _roll_filename_datetime(
                file_name=new_filename, data_interval=time_step
                )
            )
        logging.info('    {0} -> {1}'.format(file.name, new_filename))
        try:
            file.rename(to_dir / new_filename)
        except FileExistsError as e:
            logging.error(str(e))
            logging.error('Aborting...')
            raise
        
    logging.info('Rename and move of converted files complete')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_CardConvert(CardConvert_config_path):
    
    logging.info('Running TOA5 format conversion with CardConvert_parser:')
    cc_path = str(PATHS.get_application_path(application='CardConvert'))
    spc_args = [cc_path, f'runfile={CardConvert_config_path}']
    rslt = spc.Popen(spc_args, stdout=spc.PIPE, stderr=spc.STDOUT)
    (out, err) = rslt.communicate()
    if out:
        logging.info(out.decode())
        logging.info('CardConvert conversion complete')
    if err:
        logging.error('CardConvert processing failed with the following message:')
        logging.error(err.decode())
        raise spc.CalledProcessError()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _make_dir(file_name, understorey=False):

    """Get the required child directory name(s) for the file"""

    split_list = file_name.split('.')[0].split('_')
    split_dict = {x: split_list[FILENAME_FORMAT[x]] for x in FILENAME_FORMAT}
    year_month = '_'.join([split_dict['Year'], split_dict['Month']])
    sub_dirs_list = [split_dict['Format'], year_month]
    if not split_dict['Format'] == 'TOB3':
        sub_dirs_list += [split_dict['Day']]
    target_path = PATHS.get_local_path(
        resource='data', stream='flux_fast', site=split_dict['Site'], 
        subdirs=sub_dirs_list
        )
    if understorey:
        target_path = _replace_dir_for_understorey(inpath=target_path)
    try: 
        target_path.mkdir(parents=True)
    except (FileExistsError, AttributeError): 
        pass
    return target_path
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _demangle_filename(file_name):

    """Get the new filename (CardConvert prepends the new format and appends
       year, month, day, hour and minute; we drop old format and redundant
       date parts)"""

    mangled_format = {'Format': FILENAME_FORMAT.get('Format')}
    mangled_format.update(
        {x: FILENAME_FORMAT[x] + 1 for x in list(FILENAME_FORMAT.keys())[1:]}
        )
    mangled_format.update({'HrMin': 10})
    mangled_elements = file_name.split('_')
    unmangled_elements = [
        mangled_elements[mangled_format[x]] for x in mangled_format.keys()
        ]
    return '_'.join(unmangled_elements)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _roll_filename_datetime(file_name, data_interval):

    """Correct filename timestamp to represent end rather than beginning
       of period"""

    split_list = file_name.split('_')
    split_dict = {x: split_list[FILENAME_FORMAT[x]] for x in FILENAME_FORMAT}
    HrMin = split_list[-1].split('.')[0]
    split_dict.update({'Hour': HrMin[:2], 'Min': HrMin[2:]})
    prefix = '_'.join([split_dict['Format'], split_dict['Site'], split_dict['Freq']])
    delta_mins = _get_rounded_mins(int(split_dict['Min']), data_interval)
    py_datetime = (
        dt.datetime(int(split_dict['Year']), int(split_dict['Month']),
                    int(split_dict['Day']), int(split_dict['Hour']), 0) +
        dt.timedelta(minutes=delta_mins)
        )
    suffix = dt.datetime.strftime(py_datetime, '%Y_%m_%d_%H%M')
    return '_'.join([prefix, suffix]) + '.dat'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_rounded_mins(mins, interval):

    """Roll forward minutes to end of relevant interval"""

    return (mins - np.mod(mins, interval) + interval).item()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------


def main(site):

    logging.info(f'Begin 10Hz TOB3 file conversion for site {site}')
    ccf_file_path = PATHS.get_local_path(resource='ccf_config')
    logging.info('Fetching site information from database...')
    cc_info = CardConvert_PreChecker(site=site)
    logging.info('Done!')
    try:
        files_to_process=cc_info.get_raw_files()
    except:
        return
    n_expected_files = int(len(files_to_process) * 1440 / cc_info.time_step)
    try:
        run_CardConvert(
            CardConvert_config_path=cc_info.ccf_filepath / cc_info.ccf_filename
            )
    except spc.CalledProcessError:
        return
    temp_path = PATHS.get_local_path(
        resource='data', stream='flux_fast', site='Calperum', subdirs=['TMP']
        )
    move_raw_files(path=temp_path)
    n_yielded_files = int(len(list(temp_path.glob('TOA5*.dat'))))
    msg = f'Expected {n_expected_files}, got {n_yielded_files}!'
    if n_yielded_files == n_expected_files:
        logging.info(msg)
    else:
        logging.warning(msg)
    try:
        rename_and_move_proc_files(path=temp_path, time_step=cc_info.time_step)
    except FileExistsError:
        return
    logging.info(f'Finished file conversion for site {site}')