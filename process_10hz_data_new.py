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
### CONSTANTS ###
#------------------------------------------------------------------------------
base_logger_path = r'E:/Sites/{}/Logs'

FILENAME_FORMAT = {
    'Format': 0, 'Site': 1, 'Freq': 2, 'Year': 3, 'Month': 4, 'Day': 5
    }
PATHS = pm.paths()
DETAILS = sd.site_details()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Functions ###
#------------------------------------------------------------------------------

# Set input and output paths

class CardConvert_PreChecker():
    
    def __init__(self, site, input_dir='TMP'):
        
        # Assign attributes
        self.site = site
        self.ccf_filepath = PATHS.get_local_path(resource='ccf_config')
        self.ccf_filename = f'TOA5_{site}.ccf'
        self.raw_data_path = PATHS.get_local_path(
            resource='data', stream='flux_fast', subdirs=[input_dir], site=site
            )
        self.site_details = DETAILS.get_single_site_details('Calperum')
        self.time_step = self.site_details.time_step
        self.files_to_parse = self._get_raw_files()
        self._write_ccf_file()
    
    def _check_raw_file_name_format(self, file_name):
    
        """Check that file conforms to expected naming - no action if true,
            raise if false"""        
    
        # Construct the expected name
        interval = '100ms' if self.time_step == 30 else '50ms'
        expected_name = (
            'TOB3_{0}_{1}_YYYY_MM_DD.dat'.format(self.site, interval)
            )
    
        # Split to lists for comparison of elements
        file, ext = file_name.split('.')
        elements = file.split('_')
        expected_elements = expected_name.split('.')[0].split('_')
        
        # Error checks: 
        # 1) ends with dat
        if not ext == 'dat':
            raise TypeError(f'File type unknown for file {file}')
            
        # 2) correct number of elements
        if not len(elements) == 6:
            raise TypeError(
                f'File {file} does not conform to expected convention '
                '({expected_name})'
                )
            
        # 3) elements match
        for i in range(3):
            if not elements[i] == expected_elements[i]:
                raise TypeError(
                    'Element {0} ("{1}") in file name is not of required format '
                    'for file {2}; expected "{3}"'
                    .format(str(i), elements[i], file, expected_elements[i])
                    )
        
        # 4) parseable date 
        dt.datetime(int(elements[3]), int(elements[4]), int(elements[5]))   

    def _get_raw_files(self):
    
        files = [x.name for x in self.raw_data_path.glob('*')]
        # raw_files = [x.name for x in self.raw_data_path.glob('TOB3*.dat')]
        archive_path = PATHS.get_local_path(
            resource='data', stream='flux_fast', subdirs=['TOB3'], 
            site=self.site
            )
        processed_files = [x.name for x in archive_path.rglob('TOB3*.dat')]
        if not files:
            raise FileNotFoundError('No raw files to parse!')
        # if not all_files == raw_files:
        #     raise RuntimeError(
        #         'Non-TOB3 files present in TMP directory! '
        #         'Please remove these files and try again!'
        #         )   
        for this_file in files:
            if this_file in processed_files:
                raise RuntimeError(f'File {this_file} already processed!')
            self._check_raw_file_name_format(file_name=this_file)
        return files

    def _make_ccf_dict(self):

        CCF_DICT = {'Format': '0', 'FileMarks': '0', 'RemoveMarks': '0', 
                    'RecNums': '0', 'Timestamps': '1', 'CreateNew': '0',
                    'DateTimeNames': '1', 'Midnight24': '0', 'ColWidth': '263', 
                    'ListHeight': '288', 'ListWidth': '190', 'BaleCheck': '1', 
                    'CSVOptions': '6619599', 'BaleStart': '38718', 
                    'BaleInterval': '32875', 'DOY': '0', 'Append': '0', 
                    'ConvertNew': '0'}

        """Return the dictionary with output-ready values"""

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
    
    def _write_ccf_file(self, overwrite=True):
        
        output_file = self.ccf_filepath / self.ccf_filename
        if not overwrite:
            if output_file.exists():
                return
        write_dict = self._make_ccf_dict()
        with open(output_file, mode='w') as f:
            f.write('[main]\n')
            for this_item in write_dict:
                f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
# def main(site):
site = 'Calperum'
CardConvert_app_path = PATHS.get_application_path(application='CardConvert')
ccf_file_path = PATHS.get_local_path(resource='ccf_config')
try:
    cc_info = CardConvert_PreChecker(site=site)
except Exception as e:
    logging.error(str(e))
logging.info(
    'The following files will be parsed: {}'
    .format(', '.join(cc_info.files_to_parse))
    )
# site = 'Calperum'
# main(site=site)
       



#------------------------------------------------------------------------------
class CardConvert_parser():

    """Creates, writes and runs CardConvert format (CCF) files"""

    def __init__(self, sitename, in_fmt, out_fmt, data_intvl_mins,
                 overwrite_ccf=True, understorey=False, timestamp_is_end=True):

        _enforce_format_naming(from_format=in_fmt, to_format=out_fmt)
        self.understorey_flag = understorey
        self.in_format = in_fmt
        self.out_format = out_fmt
        self.SiteName = (sitename  if not 'Under' in sitename 
                         else sitename.replace('Under', ''))
        self.overwrite_ccf = overwrite_ccf
        self.CardConvert_process_flag = False
        self.rename_process_flag = False
        self.time_step = int(data_intvl_mins)
        self.timestamp_is_end = timestamp_is_end
        self._cross_check_for_proc_files()

    #--------------------------------------------------------------------------
    ### Class methods pre-CardConvert ###
    #--------------------------------------------------------------------------

    def _cross_check_for_proc_files(self):
        
        archive_path = (
            PATHS.get_local_path(
                resource='data', stream='flux_fast', site=self.SiteName,
                subdirs=['TOB3']
                )
            )   
        archived_files = [x.name for x in archive_path.rglob('TOB3*.dat')]
        raw_files = self.get_raw_files()
        dupes_list = [x for x in raw_files if x in archived_files]
        if dupes_list:
            raise FileExistsError(
                'The following files have already been parsed: {}'
                .format(', '.join(dupes_list))
                )

    def get_bale_interval(self):

        """Return the formatted interval for file baling"""

        bale_interval = 1 / (1440 / self.time_step)
        if bale_interval == 1: return INIT_DICT['BaleInterval']
        if bale_interval > 1:
            raise RuntimeError('Error! Minutes must sum to less than 1 day!')
        return str(int(INIT_DICT['BaleInterval']) - 1 + round(bale_interval, 10))

    def get_raw_data_filename_format(self):
        
        fmt = self.in_format        
        site_name = self.SiteName
        if self.understorey_flag: site_name += 'Under'
        interval = '100ms' if self.time_step == 30 else '50ms'
        return (
            '{0}_{1}_{2}_YYYY_MM_DD.dat'.format(fmt, site_name, interval)
            )

    def get_ccf_dict(self) -> dict:

        """Return the dictionary with output-ready values"""

        if self.out_format == 'TOA5': format_str = '0'
        if self.out_format == 'TOB1': format_str = '1'
        write_dict = {'SourceDir': str(self.get_raw_data_path()) + '\\',
                      'TargetDir': str(self.get_raw_data_path()) + '\\'}
        write_dict.update(INIT_DICT)
        write_dict['BaleInterval'] = self.get_bale_interval()
        write_dict['Format'] = format_str
        return write_dict

    def get_ccf_filename(self):

        """Return the site-specific ccf file path"""

        path = PATHS.get_local_path(resource='ccf_config', site=self.SiteName)
        file_name = '{0}_{1}.ccf'.format(self.out_format, self.SiteName)
        return path / file_name    

    def get_raw_data_path(self):

        """Return the site-specific raw data path"""

        data_path = PATHS.get_local_path(
            resource='data', stream='flux_fast', site=self.SiteName, 
            subdirs=['TMP']
            )
        if self.understorey_flag:
            return _replace_dir_for_understorey(inpath=data_path)
        return data_path

    def get_n_expected_converted_files(self) -> int:

        """Return the expected number of output files based on n_input +
           bale interval"""

        return len(self.get_raw_files()) * int(1440 / self.time_step)

    def get_raw_files(self):

        """Check for unparsed TOB3 files in TMP directory"""

        path = self.get_raw_data_path()
        all_files = [x.name for x in path.glob('*')]
        raw_files = [x.name for x in path.glob('TOB3*.dat') 
                     if self.in_format in x.name]
        if not raw_files:
            raise FileNotFoundError('No raw files to parse!')
        if not all_files == raw_files:
            raise RuntimeError('Non-{} files present in TMP directory! '
                               'Please remove these files and try again!'
                                .format(self.in_format))   
        expected_name = self.get_raw_data_filename_format()
        for this_name in raw_files:
            _check_raw_file_name_formatting(file_name=this_name, 
                                            expected_name=expected_name)
        return raw_files               

    def run_CardConvert(self):

        """Run CardConvert and move raw files"""

        if self.CardConvert_process_flag:
            raise RuntimeError('Process has already run!')
        files_to_convert = self.get_raw_files()
        self.write_ccf_file()
        cc_path = str(PATHS.get_application_path(application='CardConvert'))
        spc_args = [cc_path, 'runfile={}'.format(self.get_ccf_filename())]
        rslt = spc.run(spc_args)
        if not rslt.returncode == 0:
            raise RuntimeError('Command line execution of CardConvert failed!')
        from_dir = self.get_raw_data_path()
        for f in files_to_convert:
            to_dir = _make_dir(f)
            try:
                (from_dir / f).rename(to_dir / f)    
            except TypeError:
                pdb.set_trace()
                _make_dir(f)
        self.CardConvert_process_flag = True

    def write_ccf_file(self):

        """Output constructed ccf file to disk"""

        if self.CardConvert_process_flag:
            raise RuntimeError('Process has already run!'
                               'Illegal to change ccf file!')
        write_dict = self.get_ccf_dict()
        output_file = self.get_ccf_filename()
        if not self.overwrite_ccf:
            if output_file.exists(): return
        with open(output_file, mode='w') as f:
            f.write('[main]\n')
            for this_item in write_dict:
                f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))

    #--------------------------------------------------------------------------
    ### Class methods post-CardConvert ###
    #--------------------------------------------------------------------------

    def get_converted_files(self) -> list:

        """Return a list of the files that have been converted using
           CardConvert parser above (execution order enforced - will not run
                                     unless CardConvert has run)"""

        if not self.CardConvert_process_flag:
            raise RuntimeError('CardConvert Process has not run yet!')
        path = self.get_raw_data_path()
        raw_files = [x.name for x in path.glob('{}*.dat'.format(self.out_format)) 
                     if self.in_format in x.name]
        return raw_files       

    def parse_converted_files(self, overwrite_files=False):

        """Do the time-based rename and move of the files generated by
           CardConvert"""

        if not self.CardConvert_process_flag: self.run_CardConvert()
        path = self.get_raw_data_path()
        for filename in self.get_converted_files():
            new_filename = _demangle_filename(filename)
            new_path = _make_dir(new_filename)
            if self.timestamp_is_end:
                new_filename = (
                    _roll_filename_datetime(
                        file_name=new_filename, data_interval=self.time_step
                        )
                    )
            old_file = path / filename
            new_file = new_path / new_filename
            try:
                old_file.rename(new_file)
            except FileExistsError:
                if not overwrite_files: raise
                old_file.replace(new_file)
        self.rename_process_flag = True
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### File handling functions ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------


def _demangle_filename(file_name):

    """Get the new filename (CardConvert prepends the new format and appends
       year, month, day, hour and minute; we drop old format and redundant
       date parts)"""

    local_dict = {'Format': FILENAME_FORMAT.get('Format')}
    local_dict.update(
        {x: FILENAME_FORMAT[x] + 1 for x in list(FILENAME_FORMAT.keys())[1:]}
        )
    split_list = file_name.split('_')
    rebuild_list = [split_list[local_dict[x]] for x in local_dict.keys()]
    rebuild_list += [split_list[-1]]
    return '_'.join(rebuild_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _enforce_format_naming(from_format, to_format):

    """Check the file formats passed"""

    if not from_format in ['TOB3', 'TOB1']:
        raise TypeError('Input format must be either TOB3 or TOB1')
    if not to_format in ['TOB1', 'TOA5']:
        raise TypeError('Input format must be either TOB1 or TOA5')
    if from_format == to_format:
        raise TypeError('Input format must be different to output format')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_rounded_mins(mins, interval):

    """Roll forward minutes to end of relevant interval"""

    return (mins - np.mod(mins, interval) + interval).item()
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
        resource='data', stream='flux_fast', site='Calperum', 
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
def _replace_dir_for_understorey(inpath):
    
    return pathlib.Path(str(inpath).replace('Flux', 'Flux_aux'))
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