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
FILENAME_FORMAT = {
    'TOB3': ['Format', 'Site', 'Freq', 'Year', 'Month', 'Day'],
    'TOA5': ['Format', 'Site', 'Freq', 'Year', 'Month', 'Day', 'HrMin']
    }
TIME_FORMAT = {'TOB3': '%Y,%m,%d', 'TOA5': '%Y,%m,%d,%H%M'}
ALIAS_DICT = {'GWW': 'GreatWesternWoodlands'}
STREAM_DICT = {'main': 'flux_fast', 'under': 'flux_fast_aux'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class GenericHandler():
    """
    Base class that sets attributes and file format checking methods
    """

    #--------------------------------------------------------------------------
    def __init__(self, site, system):
        """
        Initialise class instance

        Parameters
        ----------
        site : str
            The site.
        system : str
            The eddy covariance system - either 'main' or 'under'.

        Returns
        -------
        None.

        """

        self.site = site
        self.stream = STREAM_DICT[system]
        self.system_name = get_system_name(site=site, system=system)
        site_details = DETAILS.get_single_site_details(site=site)
        self.interval = str(int(1000 / int(site_details.freq_hz))) + 'ms'
        self.time_step = site_details.time_step

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_name_format(self, file):
        """
        Check that the passed file name conforms to expected convention.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path of the file.

        Raises
        ------
        RuntimeError
            Raised if file name format does not conform.

        Returns
        -------
        None.

        """

        # Parse file name elements into dictionary
        elems = file.stem.split('_')
        fmt = elems[0]
        elem_names = FILENAME_FORMAT[fmt]
        elems_dict = dict(zip(elem_names, elems))
        elems_dict.update({'Ext': file.suffix})

        # Raise error if problem, otherwise return none
        try:

            # Check critical non-time elements are valid
            assert elems_dict['Site'] == self.system_name
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
        """
        Delete file.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path to file.

        Returns
        -------
        None.

        """

        file.unlink()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class RawFileHandler(GenericHandler):
    """
    Inherits from base class generic_handler and adds handling methods
    for raw files
    """

    #--------------------------------------------------------------------------
    def __init__(self, site, system='main'):

        super().__init__(site, system=system)
        self.input_data_path = get_path(
            site=site, system=system, data='raw', io='input'
            )
        self.output_data_path = get_path(
            site=site, system=system, data='raw', io='output'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_parsed(self, file):
        """
        Check whether the passed file has been parsed and written to final
        storage.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path to file.

        Raises
        ------
        RuntimeError
            Raised if the file has been parsed and written to final storage BUT
            checksums are different.

        Returns
        -------
        bool
            True if file successfully parsed and written to final storage,
            false if not.

        """

        destination = self.get_destination_path(file=file)
        if not destination.exists():
            return False
        if get_hash(file=file) == get_hash(file=destination):
            return True
        raise RuntimeError(
            f'File {file.name} in {file.parent} exists in archive, but '
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
            self.output_data_path /
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

        l = list(self.input_data_path.glob(f'TOB3_{self.system_name}*.dat'))
        if not l:
            raise RuntimeError('No new raw files to parse in target directory')
        return l
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_file(self, file):
        """
        Move file to final storage.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path to file.

        Returns
        -------
        None.

        """


        destination = self.get_destination_path(file=file)
        if not destination.parent.exists():
            destination.parent.mkdir(parents=True)
        if not self.check_file_parsed(file=file):
            file.rename(destination)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class ConvertedFileHandler(GenericHandler):
    """
    Inherits from base class generic_handler and adds methods for converted
    files.
    """

    #--------------------------------------------------------------------------
    def __init__(self, site, system='main'):

        super().__init__(site, system=system)
        self.input_data_path = get_path(
            site=site, system=system, data='converted', io='input'
            )
        self.output_data_path = get_path(
            site=site, system=system, data='converted', io='output'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def check_file_parsed(self, file):
        """
        Check whether the passed file has been parsed and written to final
        storage.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path to file.

        Raises
        ------
        RuntimeError
            Raised if the file has been parsed and written to final storage BUT
            checksums are different.

        Returns
        -------
        bool
            True if file successfully parsed and written to final storage,
            false if not.

        """

        destination = self.get_destination_path(file=file)
        if not destination.exists():
            return False
        if get_hash(file=file) == get_hash(file=destination):
            return True
        raise RuntimeError(
            f'File {file.name} in {file.parent} exists in archive, but '
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
                self.output_data_path / new_dir_name / new_file_name
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

        l = list(self.input_data_path.glob('TOA5_TOB3*.dat'))
        if not l:
            raise RuntimeError(
                'No new processed files to parse in target directory'
                )
        return l
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def move_file(self, file):
        """
        Move file to final storage.

        Parameters
        ----------
        file : pathlib.Path
            Absolute path to file.

        Returns
        -------
        None.

        """

        destination = self.get_destination_path(file=file)
        if not destination.parent.exists():
            destination.parent.mkdir(parents=True)
        if not self.check_file_parsed(file=file):
            file.rename(destination)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _roll_time(self, time_dict):
        """
        Roll the embedded time components forward (Campbell CardConvert uses
        uses the start of the period as the timestamp when carvin up the TOB3
        files).

        Parameters
        ----------
        time_dict : dict
            Dictionary containing the time components.

        Returns
        -------
        dict
            Time components rolled forward by one site interval (mostly 30 mins).

        """

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
    """
    Do the checksum for the file.

    Parameters
    ----------
    file : pathlib.path
        Absolute path to file.

    Returns
    -------
    str
        Hash for the file.

    """

    with open(file, 'rb') as f:
        the_bytes=f.read()
        return hashlib.sha256(the_bytes).hexdigest()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_path(site, system, data, io):

    ref_dict = {
        'raw': {
            'input': PATHS.get_local_path(
                resource='data',
                stream=STREAM_DICT[system],
                subdirs=['TMP', 'TOB3'],
                site=site
                ),
            'output': PATHS.get_local_path(
                resource='data',
                stream=STREAM_DICT[system],
                subdirs=['TOB3'],
                site=site
                )
            },
         'converted': {
            'input': PATHS.get_local_path(
                resource='data',
                stream=STREAM_DICT[system],
                subdirs=['TMP', 'TOA5'],
                site=site
                ),
            'output': PATHS.get_local_path(
                resource='data',
                stream=STREAM_DICT[system],
                subdirs=['TOA5'],
                site=site
                )
             }
         }

    return ref_dict[data][io]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_system_name(site, system):
    """
    Get the snippet that is expected in the middle of the file name
    (e.g. Calperum for the main EC system or CalperumUnder for the understorey)

    Parameters
    ----------
    site : str
        The site.
    system : str
        The eddy covariance system - either 'main' or 'under'.

    Raises
    ------
    KeyError
        Raised if invalid system name.

    Returns
    -------
    str
        System name.

    """

    if not system in STREAM_DICT.keys():
        raise KeyError('Positional arg "system" must be either main or under!')
    if system == 'main':
        return site
    return site + 'Under'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_CardConvert(site, system):
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

    app_path = str(PATHS.get_application_path(application='CardConvert'))
    system_name = get_system_name(site=site, system=system)
    path_to_file = (
        PATHS.get_local_path(resource='ccf_config') / f'TOA5_{system_name}.ccf'
        )
    spc_args = [app_path, f'runfile={path_to_file}']
    return spc.run(spc_args, capture_output=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_ccf_file(site, time_step, system='main', overwrite=True):
    """


    Parameters
    ----------
    site : str
        The site for which to write the ccf file.
    time_step : int
        Averaging interval in minutes of the data.
    system : str
        Main eddy covariance system or understorey ('under').
        The default is 'main'.
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

    system_name = get_system_name(site=site, system=system)
    path_to_config = (
        PATHS.get_local_path(resource='ccf_config') /
        f'TOA5_{system_name}.ccf'
        )

    # Skip it if overwrite is not enabled
    if not overwrite:
        if path_to_config.exists():
            return

    # Construct the dictionary
    input_path = str(get_path(
        site=site, system=system, data='raw', io='input'
        )) + '\\'
    output_path = str(get_path(
            site=site, system=system, data='converted', io='input'
            )) + '\\'
    write_dict = {'SourceDir': input_path, 'TargetDir': output_path}
    write_dict.update(CCF_DICT)

    # Get and set the bale interval
    bale_intvl_frac = 1 / (1440 / time_step)
    if bale_intvl_frac > 1:
        raise RuntimeError('Error! Minutes must sum to less than 1 day!')
    bale_intvl = (
        str(int(CCF_DICT['BaleInterval']) - 1 + round(bale_intvl_frac, 10))
        )
    write_dict['BaleInterval'] = bale_intvl

    # Write it
    with open(path_to_config, mode='w') as f:
        f.write('[main]\n')
        for key, value in write_dict.items():
            f.write(f'{key}={value}\n')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

def main(site, system, overwrite_ccf=True):

    # Instantiate raw file handler
    raw_handler = RawFileHandler(site=site, system=system)

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
    rslt = run_CardConvert(site=site, system=system)
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
    processed_handler = ConvertedFileHandler(site=site, system=system)
    n_expected_files = (
        int(len(list(raw_files_to_convert)) * 1440 / processed_handler.time_step)
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
