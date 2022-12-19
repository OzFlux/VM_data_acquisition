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
import subprocess as spc
import sys
import pdb

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
ALIAS_DICT = {'GWW': 'GreatWesternWoodlands'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class generic_handler():
    """Base class that sets attributes and file format checking methods"""

    #--------------------------------------------------------------------------
    def __init__(self, site):

        self.file_site_name = site
        self.stream = 'flux_fast' if not 'Under' in site else 'flux_fast_aux'
        self.site = site.replace('Under', '')
        site_details = DETAILS.get_single_site_details(site=self.site)
        intvl = str(int(
            1000 / int(site_details.freq_hz)
            )) + 'ms'
        self.time_step = site_details.time_step
        self.interval = intvl
        self.raw_data_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TMP'],
            site=self.site
            )
        self.checks_flag = False
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_file_name_format(self, file, expected_format):
        """
        Check the file format conforms to expected convention.

        Parameters
        ----------
        file : str
            The file name to check.
        expected_format : str
            Sets whether the format to check for is TOB3 (raw) or TOA5.

        Raises
        ------
        RuntimeError
            Raised if number of elements following split (based on '_') is wrong.
        TypeError
            Raised if time or other critical elements are not what is expected.

        Returns
        -------
        None.

        """

        # initialisations
        gen_err_str = (
            'Integrity check for file {file} failed with the following error: {}'
            )

        # Initialise formatters
        format_dict = FILENAME_FORMAT.copy()
        time_elem_list = ['Year', 'Month', 'Day']
        time_fmt = '%Y_%m_%d'
        if expected_format == 'TOA5':
            format_dict.update({'HrMin': 6})
            time_fmt += '_%H%M'
            time_elem_list.append('HrMin')

        # Split file format to dict for comparison of elements
        name, ext = file.split('.')
        elem_list = name.split('_')
        if not len(elem_list) == len(format_dict):
            msg = gen_err_str.format('Wrong number of elements in file name!')
            logging.error(msg) ; raise RuntimeError(msg)
        elem_dict = {x: elem_list[format_dict[x]] for x in format_dict.keys()}
        elem_dict.update({'Ext': ext})

        # Make comparison dictionary for expected format
        ex_dict = {
            'Format': expected_format, 'Site': self.file_site_name,
            'Freq': self.interval, 'Ext': 'dat'
            }

        # Check critical non-time elements
        for i, this_elem in enumerate(ex_dict.keys()):
            if not elem_dict[this_elem] == ex_dict[this_elem]:
                msg = gen_err_str.formay(
                    'Element "{0}" in file name is not of required format; '
                    'expected "{1}", got "{2}"'
                    .format(
                        this_elem, ex_dict[this_elem], elem_dict[this_elem]
                        )
                    )
                logging.error(msg) ; raise TypeError(msg)

        # Check critical time elements
        time_string = '_'.join([elem_dict[x] for x in time_elem_list])
        try:
            dt.datetime.strptime(time_string, time_fmt)
        except TypeError as e:
            msg = gen_err_str.format(str(e))
            logging.error(msg) ; raise
        logging.info(f'    - {file} passed integrity check!')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class raw_file_handler(generic_handler):

    """Inherits from base class generic_handler and adds methods for raw files"""
    #--------------------------------------------------------------------------
    def __init__(self, site):

        super().__init__(site)
        self.destination_base_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TOB3'],
            site=self.site
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_files(self):
        """
        Parses the available raw files (if any) to check whether they have been
        previously parsed or format is bad.

        Raises
        ------
        FileExistsError
            Raised if file has already been moved to final storage.

        Returns
        -------
        None.

        """

        logging.info('Checking raw file integrity...')
        existing_list = [
            file.name for file in self.destination_base_path.rglob('TOB3*.dat')
            ]
        for file in self.get_raw_file_list():
            self._check_file_name_format(file=file.name, expected_format='TOB3')
            if file.name in existing_list:
                msg = f'File {file.name} already exists in archive area! Aborting...'
                logging.error(msg) ; raise FileExistsError(msg)
        self.checks_flag = True
        logging.info('Done!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_raw_file_list(self):
        """
        Check for new raw files in the directory.

        Raises
        ------
        RuntimeError
            Raised if no new files.

        Returns
        -------
        list
            List of files.

        """

        l = list(self.raw_data_path.glob('TOB3*.dat'))
        if not l:
            msg = 'No new raw files to parse in target directory'
            logging.error(msg); raise RuntimeError(msg)
        return list(self.raw_data_path.glob('TOB3*.dat'))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_data_to_final_storage(self):
        """
        Move raw files to final storage

        Returns
        -------
        None.

        """

        if not self.checks_flag:
            return
        logging.info('Moving raw files')
        for file in self.get_raw_file_list():
            dest_subdir = self._dir_from_file(file.name)
            target_path = self.destination_base_path / dest_subdir
            if not target_path.exists():
                target_path.mkdir(parents=True)
            logging.info(f'    - {str(file)} -> {target_path / file.name}')
            file.rename(target_path / file.name)
        logging.info('Move of raw files complete')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _dir_from_file(self, file):
        """
        Generate requisite directory from file name.

        Parameters
        ----------
        file : str
            File name.

        Returns
        -------
        str
            Dir name.

        """

        elem_list = file.split('.')[0].split('_')
        elem_dict = {
            x: elem_list[FILENAME_FORMAT[x]] for x in FILENAME_FORMAT
            }
        return '_'.join([elem_dict['Year'], elem_dict['Month']])
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class processed_file_handler(generic_handler):
    """Inherits from base class generic_handler and adds methods for converted
    files"""

    #--------------------------------------------------------------------------
    def __init__(self, site):

        super().__init__(site)
        # self.checks_flag = False
        self.rename_flag = False
        self.destination_base_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TOA5'],
            site=self.site
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_files(self):
        """
        Parses the available processed files to check whether they have been
        previously parsed or format is bad.

        Raises
        ------
        FileExistsError
            Raised if file has already been moved to final storage.

        Returns
        -------
        None.

        """

        logging.info('Checking formatted file integrity...')
        existing_list = [
            file.name for file in self.destination_base_path.rglob('TOA5*.dat')
            ]
        for file in self.get_renamed_file_list():
            self._check_file_name_format(file=file.name, expected_format='TOA5')
            if file.name in existing_list:
                msg = 'This file already exists in archive area! Aborting...'
                logging.error(msg) ; raise FileExistsError(msg)
        self.checks_flag = True
        logging.info('Done')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_converted_file_list(self):
        """
        Check for new processed files in the directory.

        Raises
        ------
        RuntimeError
            Raised if files have already been renamed.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        if self.rename_flag:
            msg = 'Files already renamed'
            logging.error(msg) ; raise RuntimeError(msg)
        return self.raw_data_path.glob('TOA5_TOB3*.dat')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_renamed_file_list(self):
        """
        Check for new renamed files in the directory.

        Raises
        ------
        RuntimeError
            Raised if files haven't already been renamed.

        Returns
        -------
        generator
            Contains the relevant contents of the directory (all TOA5 files).

        """

        if not self.rename_flag:
            msg = 'Files not renamed yet!'
            logging.error(msg) ; raise RuntimeError(msg)
        return self.raw_data_path.glob('TOA5*.dat')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_data_to_final_storage(self):
        """
        Move the processed data to final storage.

        Raises
        ------
        RuntimeError
            Raised if check and rename flags are not high.

        Returns
        -------
        None.

        """

        if not self.checks_flag:
            msg = 'Files not checked yet!'
            logging.error(msg) ; raise RuntimeError(msg)
        logging.info('Moving converted files:')
        for file in self.get_renamed_file_list():
            dest_subdir_list = self._dir_from_file(file.name)
            target_path = self.destination_base_path
            for subdir in dest_subdir_list:
                target_path = target_path / subdir
            if not target_path.exists():
                target_path.mkdir(parents=True)
            logging.info(f'    - {str(file)} -> {target_path / file.name}')
            file.rename(target_path / file.name)
        logging.info('Move of processed files complete')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _dir_from_file(self, file):
        """
        Generates the requisite directory name based on the file name.

        Parameters
        ----------
        file : str
            File name for which to generate the directory name.

        Returns
        -------
        list
            DESCRIPTION.

        """

        format_dict = FILENAME_FORMAT.copy()
        format_dict.update({'HrMin': 6})
        elem_list = file.split('.')[0].split('_')
        elem_dict = {x: elem_list[format_dict[x]] for x in format_dict}
        if not elem_dict['HrMin'] == '0000':
            time_elems = [
                elem_dict['Year'], elem_dict['Month'], elem_dict['Day']
                ]
        else:
            new_date = (
                dt.datetime(int(elem_dict['Year']), int(elem_dict['Month']),
                            int(elem_dict['Day'])) -
                dt.timedelta(minutes=self.time_step)
                )
            time_elems = [
                str(new_date.year), str(new_date.month).zfill(2),
                str(new_date.day).zfill(2)
                ]
        return ['_'.join(time_elems[:-1]), time_elems[-1]]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def rename_files(self):
        """
        Rename the coverted files.

        Returns
        -------
        None.

        """

        logging.info('Renaming converted files:')
        for file in self.get_converted_file_list():
            new_filename = self._rebuild_filename(file.name)
            file.rename(self.raw_data_path / new_filename)
            logging.info(f'   {file.name} -> {new_filename}')
        logging.info('Done')
        self.rename_flag = True
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _rebuild_filename(self, file):
        """
        Get the new filename (CardConvert prepends the new format and appends
        year, month, day, hour and minute; we drop old format and redundant
        date parts)

        Parameters
        ----------
        file : str
            The existing filename to be demangled.

        Returns
        -------
        str
            Demangled filename.

        """

        filename, ext = file.split('.')
        mangled_list = filename.split('_')
        mangled_list.remove('TOB3')
        mangled_list.append(ext)
        format_dict = FILENAME_FORMAT.copy()
        format_dict.update({'HrMin': 9})
        elem_dict = {x: mangled_list[format_dict[x]] for x in format_dict}
        hr, mins = elem_dict['HrMin'][:2], elem_dict['HrMin'][2:]
        delta_mins = (
            (int(mins) - np.mod(int(mins), self.time_step) +
             self.time_step).item()
            )
        py_datetime = (
            dt.datetime(int(elem_dict['Year']), int(elem_dict['Month']),
                        int(elem_dict['Day']), int(hr), 0) +
            dt.timedelta(minutes=delta_mins)
            )
        info_str = (
            '_'.join([elem_dict['Format'], elem_dict['Site'], elem_dict['Freq']])
            )
        time_str = py_datetime.strftime('%Y_%m_%d_%H%M.dat')
        return '_'.join([info_str, time_str])
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_CardConvert(site):
    """
    Spawn subprocess -> CampbellSci Loggernet CardConvert utility to convert
    proprietary binary format to ASCII

    Parameters
    ----------
    site : str
        The site for which to run CardConvert.

    Raises
    ------
    spc.CalledProcessError
        Raise if spc returns non-zero exit code.

    Returns
    -------
    None.

    """

    logging.info('Running TOA5 format conversion with CardConvert_parser...')
    cc_path = str(PATHS.get_application_path(application='CardConvert'))
    path_to_file = (
        PATHS.get_local_path(resource='ccf_config') / f'TOA5_{site}.ccf'
        )
    spc_args = [cc_path, f'runfile={path_to_file}']
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
def write_ccf_file(site, time_step, overwrite=True):
    """


    Parameters
    ----------
    site : str
        The site for which to write the ccf file.
    time_step : int
        Averaging interval in minutes of the data.
    overwrite : Bool, optional
        Whether to overwrite existing file or use it. The default is True.

    Raises
    ------
    RuntimeError
        Raise if the bale interval fraction is > 1.

    Returns
    -------
    None.

    """

    logging.info('Writing CardConvert configuration file...')
    site_file_name = site
    site = site.replace('Under', '')
    stream = 'flux_fast' if not 'Under' in site_file_name else 'flux_fast_aux'
    path_to_file = (
        PATHS.get_local_path(resource='ccf_config') /
        f'TOA5_{site_file_name}.ccf'
        )
    path_to_data = PATHS.get_local_path(
        resource='data', stream=stream, subdirs=['TMP'], site=site
        )

    # Skip it if overwrite is not enabled
    if not overwrite:
        if path_to_file.exists():
            logging.info(
                'File exists, overwrite flag is False, skipping write...'
                )
            return

    # Construct the dictionary
    src_dir = str(path_to_data) + '\\'
    write_dict = {'SourceDir': src_dir, 'TargetDir': src_dir}
    write_dict.update(CCF_DICT)

    # Get and set the bale interval
    bale_intvl_frac = 1 / (1440 / time_step)
    if bale_intvl_frac > 1:
        msg = 'Error! Minutes must sum to less than 1 day!'
        logging.error(msg); raise RuntimeError(msg)
    bale_intvl = (
        str(int(CCF_DICT['BaleInterval']) - 1 + round(bale_intvl_frac, 10))
        )
    write_dict['BaleInterval'] = bale_intvl

    # Write it
    with open(path_to_file, mode='w') as f:
        f.write('[main]\n')
        for this_item in write_dict:
            f.write('{0}={1}\n'.format(this_item, write_dict[this_item]))
    logging.info('Done!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

def main(site, overwrite_ccf=True):

    # Instantiate raw file handler
    raw_handler = raw_file_handler(site=site)

    # pdb.set_trace()

    # Check raw files
    files_to_process = raw_handler.get_raw_file_list()
    raw_handler.check_files()

    # Make the ccf file
    write_ccf_file(
        site=site, time_step=raw_handler.time_step, overwrite=overwrite_ccf
        )

    # Run CardConvert
    run_CardConvert(site=site)

    # Count the number of files yielded and write all to log
    processed_handler = processed_file_handler(site=site)
    n_expected_files = (
        int(len(list(files_to_process)) * 1440 / raw_handler.time_step)
        )
    yielded_files = processed_handler.get_converted_file_list()
    n_yielded_files = int(len(list(yielded_files)))
    msg = f'Expected {n_expected_files}, got {n_yielded_files}!'
    if n_yielded_files == n_expected_files:
        logging.info(msg)
    else:
        logging.warning(msg)

    # Rename the CardConvert files
    processed_handler.rename_files()

    # Check the integrity of the renamed files
    processed_handler.check_files()

    # Move the raw data to final storage
    raw_handler.move_data_to_final_storage()

    # Move the renamed and checked files to final storage
    processed_handler.move_data_to_final_storage()
