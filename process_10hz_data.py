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

#------------------------------------------------------------------------------
### Paths and variables ###
#------------------------------------------------------------------------------
CardConvert_path = r'C:/Program Files (x86)/Campbellsci/CardConvert/CardConvert.exe'
base_ccf_path = r'E:/Cloudstor/Network_documents/CCF_files'
base_data_path = r'E:/Cloudstor/Shared/EcoSystemProcesses/Sites/{}/Data/Flux/Raw/Fast/'
base_logger_path = (r'E:/Cloudstor/Shared/EcoSystemProcesses/Sites/{}/Data/Flux/Logs')
init_dict = {'Format': '0', 'FileMarks': '0', 'RemoveMarks': '0', 'RecNums': '0',
             'Timestamps': '1', 'CreateNew': '0', 'DateTimeNames': '1',
             'Midnight24': '0', 'ColWidth': '263', 'ListHeight': '288',
             'ListWidth': '190', 'BaleCheck': '1', 'CSVOptions': '6619599',
             'BaleStart': '38718', 'BaleInterval': '32875',
             'DOY': '0', 'Append': '0', 'ConvertNew': '0'}
filename_format = {
    'Format': 0, 'Site': 1, 'Freq': 2, 'Year': 3, 'Month': 4, 'Day': 5
    }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Classes ###
#------------------------------------------------------------------------------
class CardConvert_parser():

    """Creates, writes and runs CardConvert format (CCF) files"""

    def __init__(self, sitename, in_fmt, out_fmt, data_intvl_mins,
                 overwrite_ccf=True, timestamp_is_end=True):

        _enforce_format_naming(from_format=in_fmt, to_format=out_fmt)
        self.understorey_flag = False if not 'Under' in sitename else True
        self.in_format = in_fmt
        self.out_format = out_fmt
        self.SiteName = (sitename  if not 'Under' in sitename 
                         else sitename.replace('Under', ''))
        self.overwrite_ccf = overwrite_ccf
        self.CardConvert_process_flag = False
        self.rename_process_flag = False
        self.time_step = int(data_intvl_mins)
        self.timestamp_is_end = timestamp_is_end

    #-------------------------------------------------------------------------
    ### Class methods private ###
    #-------------------------------------------------------------------------

    def _check_path(self, path, subdirs=None, error_if_missing=True):
    
        """Check path exists and return path object"""
    
        fmt_path = pathlib.Path(path)
        if subdirs:
            for item in subdirs: 
                fmt_path = fmt_path / item
        if error_if_missing:                
            if not fmt_path.exists():
                raise FileNotFoundError(
                    'Specified file path ({}) does not exist! '
                    'Check site name or specified subdirectories!'
                    .format(str(fmt_path))
                    )
        return fmt_path
    
    def _check_raw_file_name_formatting(self, file):

        """Check that file conforms to expected naming - no action if true,
           raise if false"""        

        file_name, file_ext = file.split('.')
        if not file_ext == 'dat':
            raise TypeError('File type unknown for file {}'.format(file))
        elements = file_name.split('_')
        if not len(elements) == 6:
            raise TypeError('File {} does not conform to ')
        fmt = self.in_format        
        site_name = self.SiteName
        if self.understorey_flag: site_name += 'Under'
        interval = '100ms' if self.time_step == 30 else '50ms'
        format_str = (
            '{0}_{1}_{2}_year_month_day.dat'.format(fmt, site_name, interval)
            )
        if not elements[0] == fmt:
            raise TypeError('First element in file name is not of specified '
                            'format for file {0}; expected {1}'
                            .format(file, format_str))
        if not elements[1] == site_name:
            raise TypeError('Second element in file name different to '
                            'passed file name for file {}'.format(file))
        if not elements[2] == interval:
            raise TypeError('Third element in file name is not a recognised '
                            'data interval for filename {}'.format(file))
        dt.datetime(int(elements[3]), int(elements[4]), int(elements[5]))    
    
    def _get_files_of_type(self, file_type=None, error_if_zero=True,
                           error_if_other_type=False) -> list:

        """Check for files of type (default None for all files)"""
        
        path = self.get_raw_data_path()
        files = [x.name for x in path.glob('*.dat')]
        if not file_type:
            subset_files = files
        else:
            subset_files = [x for x in files if x.split('_')[0] == file_type]
        if not subset_files:
            raise FileNotFoundError('Directory contains no files of type {}!'
                                    .format(file_type))
        if error_if_other_type and not len(subset_files) == len(files):
            raise RuntimeError('Non-{} files present in TMP directory! '
                               'Please remove these files and try again!'
                               .format(file_type))            
        return subset_files
    
    def _make_dir(self, file):

        """Get the required child directory name(s) for the file"""
    
        split_list = file.split('.')[0].split('_')
        split_dict = {x: split_list[filename_format[x]] for x in filename_format}
        year_month = '_'.join([split_dict['Year'], split_dict['Month']])
        sub_dirs_list = [split_dict['Format'], year_month]
        pdb.set_trace()
        if not split_dict['Format'] == 'TOB3':
            sub_dirs_list += [split_dict['Day']]
        return _check_path(path=base_data_path.format(split_dict['Site']),
                           subdirs=sub_dirs_list, error_if_missing=False)
        
        pass

    #-------------------------------------------------------------------------
    ### Class methods pre-CardConvert ###
    #-------------------------------------------------------------------------

    def get_bale_interval(self):

        """Return the formatted interval for file baling"""

        bale_interval = 1 / (1440 / self.time_step)
        if bale_interval == 1: return init_dict['BaleInterval']
        if bale_interval > 1:
            raise RuntimeError('Error! Minutes must sum to less than 1 day!')
        return str(int(init_dict['BaleInterval']) - 1 + round(bale_interval, 10))

    def get_base_data_path(self):
        
        """Return the site-specific base data path"""
        
        site_data_path = base_data_path.format(self.SiteName)
        if self.understorey_flag:
            site_data_path = site_data_path.replace('Flux', 'Flux_aux')
        return self._check_path(site_data_path)

    def get_ccf_dict(self) -> dict:

        """Return the dictionary with output-ready values"""

        if self.out_format == 'TOA5': format_str = '0'
        if self.out_format == 'TOB1': format_str = '1'
        write_dict = {'SourceDir': str(self.get_raw_data_path()) + '\\',
                      'TargetDir': str(self.get_raw_data_path()) + '\\'}
        write_dict.update(init_dict)
        write_dict['BaleInterval'] = self.get_bale_interval()
        write_dict['Format'] = format_str
        return write_dict

    def get_ccf_filename(self):

        """Return the site-specific ccf file path"""

        path = self._check_path(base_ccf_path.format(self.SiteName))
        file_name = '{0}_{1}.ccf'.format(self.out_format, self.SiteName)
        return path / file_name    

    def get_raw_data_path(self):

        """Return the site-specific raw data path"""

        return self._check_path(path=self.get_base_data_path(), subdirs=['TMP'])

    def get_n_expected_converted_files(self) -> int:

        """Return the expected number of output files based on n_input +
           bale interval"""

        return len(self.get_raw_files()) * int(1440 / self.time_step)

    def get_raw_files(self):

        """Check for unparsed TOB3 files in TMP directory"""

        check_for_other = False if self.CardConvert_process_flag else True
        files = (
            self._get_files_of_type(file_type='TOB3', 
                                    error_if_other_type=check_for_other)
            )
        for this_file in files:
            self._check_raw_file_name_formatting(this_file)
        return files

    def run_CardConvert(self):

        """Run CardConvert"""

        if self.CardConvert_process_flag:
            raise RuntimeError('Process has already run!')
        self.get_raw_files()
        self.write_ccf_file()
        cc_path = str(self._check_path(CardConvert_path))
        spc_args = [cc_path, 'runfile={}'.format(self.get_ccf_filename())]
        rslt = spc.run(spc_args)
        if not rslt.returncode == 0:
            raise RuntimeError('Command line execution of CardConvert failed!')
        self.CardConvert_process_flag = 1

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

    #-------------------------------------------------------------------------
    ### Class methods post-CardConvert ###
    #-------------------------------------------------------------------------

    def get_converted_files(self) -> list:

        """Return a list of the files that have been converted using
           CardConvert parser above (execution order enforced - will not run
                                     unless CardConvert has run)"""

        if not self.CardConvert_process_flag:
            raise RuntimeError('CardConvert Process has not run yet!')
        return self._get_files_of_type(file_type=self.out_format)

    def parse_converted_files(self, overwrite_files=False):

        """Do the time-based rename and move of the files generated by
           CardConvert"""

        if not self.CardConvert_process_flag: self.run_CardConvert()
        path = self.get_raw_data_path()
        for filename in self.get_converted_files():
            new_filename = _demangle_filename(filename)
            new_path = _get_dirname(new_filename)
            if self.timestamp_is_end:
                new_filename = (
                    _roll_filename_datetime(file_name=new_filename,
                                            data_interval=self.time_step)
                    )
            try: new_path.mkdir(parents=True)
            except FileExistsError: pass
            old_file = path / filename
            new_file = new_path / new_filename
            try:
                old_file.rename(new_file)
            except FileExistsError:
                if not overwrite_files: raise
                old_file.replace(new_file)
        self.rename_process_flag = True

    #-------------------------------------------------------------------------
    ### Class methods post-rename and move ###
    #-------------------------------------------------------------------------

    def move_raw_files(self):

        """Move the original raw files to final storage"""

        if not self.CardConvert_process_flag:
            raise RuntimeError('No conversions done yet!')
        if not self.rename_process_flag:
            raise RuntimeError('Conversions done but converted files have '
                               'not been parsed! The following files are yet '
                               'to be parsed: {}'
                               .format(self.get_converted_files()))
        files_to_move = self.get_raw_files()
        from_dir = self.get_raw_data_path()
        for f in files_to_move:
            to_dir = _get_dirname(f)
            try: to_dir.mkdir(parents=True)
            except FileExistsError: pass
            (from_dir / f).rename(to_dir / f)

#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
### File handling functions ###
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _check_path(path, subdirs=None, error_if_missing=True):

    """Check path exists \n
        * args:
            path (formatted path): """

    fmt_path = pathlib.Path(path)
    if subdirs:
        for item in subdirs: fmt_path = fmt_path / item
    if not error_if_missing: return fmt_path
    if not fmt_path.exists():
        raise FileNotFoundError('Specified file path ({}) does not exist! '
                                'Check site name or specified subdirectories!'
                                .format(str(fmt_path)))
    return fmt_path
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _demangle_filename(file_name):

    """Get the new filename (CardConvert prepends the new format and appends
       year, month, day, hour and minute; we drop old format and redundant
       date parts)"""

    local_dict = {'Format': filename_format.get('Format')}
    local_dict.update(
        {x: filename_format[x] + 1 for x in list(filename_format.keys())[1:]}
        )
    split_list = file_name.split('_')
    rebuild_list = [split_list[local_dict[x]] for x in local_dict.keys()]
    rebuild_list += [split_list[-1].split('.')[0]]
    return '_'.join(rebuild_list) + '.dat'
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _enforce_format_naming(from_format, to_format):

    """Check the file formats passed"""

    if not from_format in ['TOB3', 'TOB1']:
        raise TypeError('Input format must be either TOB3 or TOB1')
    if not to_format in ['TOB1', 'TOA5']:
        raise TypeError('Input format must be either TOB1 or TOA5')
    if from_format == to_format:
        raise TypeError('Input format must be different to output format')
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _get_dirname(file_name):

    """Get the required child directory name(s) for the file"""

    split_list = file_name.split('_')
    split_dict = {x: split_list[filename_format[x]] for x in filename_format}
    year_month = '_'.join([split_dict['Year'], split_dict['Month']])
    sub_dirs_list = [split_dict['Format'], year_month]
    if not split_dict['Format'] == 'TOB3':
        sub_dirs_list += [split_dict['Day']]
    return _check_path(path=base_data_path.format(split_dict['Site']),
                       subdirs=sub_dirs_list, error_if_missing=False)
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _get_rounded_mins(mins, interval):

    """Roll forward minutes to end of relevant interval"""

    return (mins - np.mod(mins, interval) + interval).item()
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _roll_filename_datetime(file_name, data_interval):

    """Correct filename timestamp to represent end rather than beginning
       of period"""

    split_list = file_name.split('_')
    split_dict = {x: split_list[filename_format[x]] for x in filename_format}
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
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def _set_logger(site, name=None):

    """Create logger and send output to file"""

    logger_dir_path = _check_path(path=base_logger_path.format(site))
    logger_file_path = (logger_dir_path /
                        '{}_fast_data_processing.log'.format(site))
    if not name: this_logger = logging.getLogger(name=__name__)
    else: this_logger = logging.getLogger(name=name)
    this_logger.setLevel(logging.DEBUG)
    if not this_logger.hasHandlers():
        handler = logging.FileHandler(logger_file_path)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        this_logger.addHandler(handler)
    return this_logger
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
### Main ###
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
if __name__ == "__main__":

    site, interval = sys.argv[1], sys.argv[2]
    base_logger_path = (r'E:/Cloudstor/Shared/EcoSystemProcesses/Sites/'
                        '{}/Data/Flux/Logs')
    logger = _set_logger(site=site)
    logger.info('Raw file received - begin processing for site {}'.format(site))
    logger.info('Running TOA5 format conversion with CardConvert_parser')
    try:
        cc_parser = CardConvert_parser(sitename=site, in_fmt='TOB3',
                                       out_fmt='TOA5',
                                       data_intvl_mins=interval)
        files_to_parse = cc_parser.get_raw_files()
        logger.info('The following files will be parsed: {}'
                    .format(', '.join(files_to_parse)))
        cc_parser.run_CardConvert()
        n_expected = cc_parser.get_n_expected_converted_files()
        n_produced = len(cc_parser.get_converted_files())
        msg = ('TOA5 format conversion complete: expected {0} converted '
               'files, got {1}!'.format(n_expected, n_produced))
        if n_expected == n_produced: logger.info(msg)
        else: logger.warning(msg)
    except Exception as e:
        logger.error('CardConvert processing failed! Message: {}'
                     .format(e)); sys.exit()
        pdb.set_trace()
    logger.info('Renaming and moving files...')
    try:
        cc_parser.parse_converted_files()
        logger.info('Files moved successfully!')
    except Exception as e:
        logger.error('Converted file move failed! Message: {}'.format(e))
        sys.exit()
    try:
        logger.info('Moving TOB3 files to final storage')
        cc_parser.move_raw_files()
        logger.info('Completed move of raw data... enjoy your cornflakes!')
    except Exception as e:
        logger.error('Raw file move failed! Message: {}'.format(e)); sys.exit()