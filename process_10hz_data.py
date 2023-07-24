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
import hashlib
import logging
import numpy as np
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
OPERATIONAL_SITES = DETAILS.get_operational_sites().index.tolist()
FILENAME_FORMAT = {
    'TOB3': ['Format', 'Site', 'Freq', 'Year', 'Month', 'Day'],
    'TOA5': ['Format', 'Site', 'Freq', 'Year', 'Month', 'Day', 'HrMin']
    }
TIME_FORMAT = {'TOB3': '%Y,%m,%d', 'TOA5': '%Y,%m,%d,%H%M'}
ALIAS_DICT = {'GWW': 'GreatWesternWoodlands'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class GenericHandler():
    """Base class that sets attributes and file format checking methods"""

    #--------------------------------------------------------------------------
    def __init__(self, site):

        self.site_name = site
        self.stream = 'flux_fast' if not 'Under' in site else 'flux_fast_aux'
        self.site = site.replace('Under', '')
        site_details = DETAILS.get_single_site_details(site=self.site)
        self.interval = str(int(
            1000 / int(site_details.freq_hz)
            )) + 'ms'
        self.time_step = site_details.time_step
        self.raw_data_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TMP'],
            site=self.site
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_name_format(self, file):

        # Parse file name elements into dictionary
        elems = file.stem.split('_')
        fmt = elems[0]
        elem_names = FILENAME_FORMAT[fmt]
        elems_dict = dict(zip(elem_names, elems))
        elems_dict.update({'Ext': file.suffix})

        # Raise error if problem, otherwise return none
        try:

            # Check critical non-time elements are valid
            assert elems_dict['Site'] == self.site
            assert elems_dict['Freq'] == self.interval
            assert elems_dict['Ext'] == '.dat'

            # Check critical time elements are valid
            dt.datetime.strptime(
                ','.join(elems_dict[elem] for elem in elem_names[3:]),
                TIME_FORMAT[elems_dict['Format']]
                )

        except (AssertionError, TypeError, ValueError) as e:

            raise RuntimeError('Invalid file format!') from e
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def remove_file(self, file):

        file.unlink()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class RawFileHandler(GenericHandler):

    """Inherits from base class generic_handler and adds methods for raw files"""
    #--------------------------------------------------------------------------
    def __init__(self, site):

        super().__init__(site)
        self.destination_base_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TOB3'],
            site=self.site
            )
        self.archived_files = list(
            self.destination_base_path.rglob('TOB3*.dat')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_parsed(self, file):

        destination = self.get_destination_path(file=file)
        if not destination.exists():
            return False
        if get_hash(file=file) == get_hash(file=destination):
            return True
        raise RuntimeError(
            f'File {file.name} in {self.raw_data_path} exists in archive, but '
            'hashes do not match!'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_destination_path(self, file):
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

        elems = file.stem.split('_')
        fmt = elems[0]
        elem_names = FILENAME_FORMAT[fmt]
        elems_dict = dict(zip(elem_names, elems))
        return (
            self.destination_base_path /
            '_'.join([elems_dict['Year'], elems_dict['Month']]) /
            file.name
            )
        return '_'.join([elems_dict['Year'], elems_dict['Month']])
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

        l = list(self.raw_data_path.glob(f'TOB3_{self.site}*.dat'))
        if not l:
            raise RuntimeError('No new raw files to parse in target directory')
        return l
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_file(self, file):

        destination = self.get_destination_path(file=file)
        if not destination.parent.exists():
            destination.parent.mkdir(parents=True)
        if not self.check_file_parsed(file=file):
            file.rename(destination)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class ConvertedFileHandler(GenericHandler):
    """Inherits from base class generic_handler and adds methods for converted
    files"""

    #--------------------------------------------------------------------------
    def __init__(self, site):

        super().__init__(site)
        self.destination_base_path = PATHS.get_local_path(
            resource='data', stream=self.stream, subdirs=['TOA5'],
            site=self.site
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_parsed(self, file):

        destination = self.get_destination_path(file=file)
        if not destination.exists():
            return False
        if get_hash(file=file) == get_hash(file=destination):
            return True
        raise RuntimeError(
            f'File {file.name} in {self.raw_data_path} exists in archive, but '
            'hashes do not match!'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_destination_path(self, file, use_dest_path=True):
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
        pathlib.Path
            Demangled absolute filename.

        """

        elems = file.stem.split('_')
        elems.remove('TOB3')
        if not len(elems) == 10:
            raise RuntimeError('Unexpected elements in filename!')
        elems = elems[:3] + elems[6:]
        elem_names = FILENAME_FORMAT['TOA5']
        elems_dict = dict(zip(elem_names, elems))

        # Explain this!
        new_dir_name = (
            f'{elems_dict["Year"]}_{elems_dict["Month"]}/{elems_dict["Day"]}'
            )

        elems_dict.update(self._roll_time(time_dict=elems_dict))
        new_file_name = '_'.join(elems_dict.values()) + file.suffix
        self.check_file_name_format(file=file.parent / new_file_name)
        if use_dest_path:
            return (
                self.destination_base_path / new_dir_name / new_file_name
                )
        return file.parent / ('_'.join(elems_dict.values()) + file.suffix)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_converted_file_list(self):
        """
        Check for new processed files in the directory.

        Raises
        ------
        RuntimeError
            Raised if no new files.

        Returns
        -------
        list
            List of files.

        """

        l = list(self.raw_data_path.glob('TOA5_TOB3*.dat'))
        if not l:
            raise RuntimeError(
                'No new processed files to parse in target directory'
                )
        return l
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_file(self, file):

        destination = self.get_destination_path(file=file)
        if not destination.parent.exists():
            destination.parent.mkdir(parents=True)
        if not self.check_file_parsed(file=file):
            file.rename(destination)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _roll_time(self, time_dict):

        time = dt.datetime.strptime(time_dict['HrMin'], '%H%M').time()
        delta_mins = (
            (int(time.minute) - np.mod(int(time.minute), self.time_step) +
             self.time_step)
            .item()
            )
        py_dt = (
            (dt.datetime(
                year=int(time_dict['Year']), month=int(time_dict['Month']),
                day=int(time_dict['Day']), hour=time.hour
                ) +
                dt.timedelta(minutes=delta_mins)
                )
            )
        return {
            'Year': py_dt.strftime('%Y'), 'Month': py_dt.strftime('%m'),
            'Day': py_dt.strftime('%d'), 'HrMin': py_dt.strftime('%H%M')
            }
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_hash(file):

    with open(file, 'rb') as f:
        the_bytes=f.read()
        return hashlib.sha256(the_bytes).hexdigest()
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

    cc_path = str(PATHS.get_application_path(application='CardConvert'))
    path_to_file = (
        PATHS.get_local_path(resource='ccf_config') / f'TOA5_{site}.ccf'
        )
    spc_args = [cc_path, f'runfile={path_to_file}']
    return spc.run(spc_args, capture_output=True)
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
        for key, value in write_dict.items():
            f.write(f'{key}={value}\n')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

def main(site, overwrite_ccf=True):

    # Instantiate raw file handler
    raw_handler = RawFileHandler(site=site)

    # Iterate over list of raw files and check if exist
    logging.info('Checking raw files...')
    raw_files_to_convert = raw_handler.get_raw_file_list()

    if not raw_files_to_convert:
        msg = 'No files to convert!'
        logging.error(msg); raise FileNotFoundError(msg)

    for file in raw_files_to_convert:

        logging.info(f'{file.name}')

        try:
            raw_handler.check_file_name_format(file=file)
            logging.info('    - format -> OK')
        except RuntimeError as e:
            logging.error(e); raise

        try:
            if raw_handler.check_file_parsed(file=file):
                raw_handler.remove_file(file=file)
                logging.error(
                    '    - already_parsed -> True! Checksums match - deleting!'
                    )
                continue
            else:
                logging.info('    - already parsed -> False')
        except RuntimeError as e:
            logging.error(e); raise

    # Check whether any raw files disappeared
    raw_files_to_convert= raw_handler.get_raw_file_list()

    if not raw_files_to_convert:
        msg = 'Available files were all duplicates! Exiting...'
        logging.error(msg); raise FileNotFoundError(msg)

    # Make the ccf file
    logging.info('Writing CardConvert configuration file...')
    write_ccf_file(
        site=site, time_step=raw_handler.time_step, overwrite=overwrite_ccf
        )
    logging.info('Done!')

    # Run CardConvert
    logging.info('Running TOA5 format conversion with CardConvert_parser...')
    rslt = run_CardConvert(site=site)
    try:
        rslt.check_returncode()
        logging.info('Format conversion done!')
    except spc.CalledProcessError():
        logging.error(
            'Failed to run CardConvert due to the following: '
            f'{rslt.stderr.decode()}'
            )
        raise

    # Move all of the raw files...
    logging.info('Moving raw files to final destination...')
    for file in raw_files_to_convert:
        raw_handler.move_file(file=file)
    logging.info('Raw file move complete!')

    # Count the number of files yielded and write all to log
    logging.info('Checking number of files yielded...')
    processed_handler = ConvertedFileHandler(site=site)
    n_expected_files = (
        int(len(list(raw_files_to_convert)) * 1440 / raw_handler.time_step)
        )
    yielded_files = processed_handler.get_converted_file_list()
    n_yielded_files = int(len(list(yielded_files)))
    msg = f'Expected {n_expected_files}, got {n_yielded_files}!'
    if n_yielded_files == n_expected_files:
        logging.info(msg)
    else:
        logging.warning(msg)

    # Iterate over list of converted files
    logging.info('Rebuilding and moving converted files...')
    for file in yielded_files:

        dest = processed_handler.get_destination_path(file=file)

        logging.info(f'{file.name} -> {dest}')

        try:
            raw_handler.check_file_name_format(file=dest)
            logging.info('    - format -> OK')
        except RuntimeError as e:
            logging.error(e); raise

        try:
            if processed_handler.check_file_parsed(file=file):
                processed_handler.remove_file(file=file)
                logging.error(
                    '    - already_parsed -> True! Checksums match - deleting!'
                    )
            else:
                logging.info('    - already parsed -> False')
        except RuntimeError as e:
            logging.error(e); raise

        processed_handler.move_file(file=file)

    logging.info('Rebuild and move of converted files complete!')
