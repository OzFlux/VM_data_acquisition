# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 06:59:50 2023

This module handles interrogation of available EddyPro files and continual
writing out of summary file data to a concatenated master file.
The EddyProConcatConfigurator class is passive; it doesn't write anything,
it just does the analysis and spits out the master file name, and the files
to be concatenated. The approach is to treat whatever file is newest as the
master i.e. the evaluation of concatenation legality of candidate files
is measured against its structure (according to the legality rules in the
file_concatenators module).


@author: jcutern-imchugh
"""

import argparse as ap
import operator as op
import pathlib

import numpy as np

import file_io as io
import file_handler as fh

EP_SUMMARY_SEARCH_STR = 'EP-Summary'
EP_MASTER_OUTPUT_NAME = 'EP_MASTER.txt'

class EddyProConcatConfigurator():
    """Class to extract next master file and concat list from existing"""

    def __init__(self, master_file=None, slave_path=None):
        """
        Set master and slave paths and search for eligible summary files.

        Parameters
        ----------
        master_file : str or pathlib.Path, optional
            The absolute path of the file that will be the master.
            The default is None.
            If None, a new master will be constructed from eligible files found
            in the slave directory path.
        slave_path : str or pathlib.Path, optional
            The absolute path of the directory to search for slave files.
            The default is None.
            If none, the parent directory of the master file is used as the
            slave path.

        Raises
        ------
        FileNotFoundError
            Raised if master_file and slave_path are None.

        Returns
        -------
        None.

        """

        # Check inputs
        if master_file is None and slave_path is None:
            raise RuntimeError(
                'You must provide either a master file or slave path'
                )

        # Set slave path and check it exists (note that the slave path
        # - if supplied - overrides the parent directory of the master file
        # as the search directory)
        if master_file:
            self.slave_path = pathlib.Path(master_file).parent
        if slave_path:
            self.slave_path = pathlib.Path(slave_path)
        if not self.slave_path.exists():
            raise FileNotFoundError(
                f'Search path {str(self.slave_path)} not found!'
                )

        # Get the slave files
        self.summary_files = list(
            self.slave_path.glob(f'*{EP_SUMMARY_SEARCH_STR}*.txt')
            )
        if len(self.summary_files) == 0:
            raise FileNotFoundError('No eligible summary files found!')

        # Set the master file path and check it exists
        if master_file:
            self.master = pathlib.Path(master_file)
        else:
            self.master = sorted(self.summary_files)[-1]
        if not self.master.exists():
            raise FileNotFoundError(
                f'Master file {str(self.master)} not found!'
                )

    def get_master_dates(self):
        """
        Get the start and end dates of the master file.

        Returns
        -------
        dict
            Dates of first and last file record
            (keys 'start_date' and 'end_date', respectively).

        """

        return io.get_start_end_dates(file=self.master, file_type='EddyPro')

    def get_summary_file_dates(self):
        """
        Get the start and end dates of the eligible summary files.

        Returns
        -------
        list
            Contains a dictionary for each element of self.summary_files.

        """

        return [
            io.get_start_end_dates(file=f, file_type='EddyPro')
            for f in self.summary_files
            ]

    def get_unparsed_files(self, which):
        """
        Find the files that either pre- or post-date the master file.

        Parameters
        ----------
        which : str
            'old' or 'new'.

        Returns
        -------
        list
            The files.

        """

        op_func = {'old': op.lt, 'new': op.gt}[which]
        date_key = {'old': 'start_date', 'new': 'end_date'}[which]

        dates = np.array(
            [item[date_key] for item in self.get_summary_file_dates()]
            )
        return (
            np.array(self.summary_files)
            [op_func(dates, self.get_master_dates()[date_key])]
            .tolist()
            )

    def get_file_configuration(self):
        """
        Return the configuration i.e. which file is the master, which are the
        slaves to be concatenated.

        Returns
        -------
        dict
            The master file and the list of slave files
            (keys 'master_file' and 'concat_list', respectively).

        """

        # Check for old and new unparsed files
        new_unparsed_files = self.get_unparsed_files(which='new')
        old_unparsed_files = self.get_unparsed_files(which='old')

        # Set the master to the default
        new_master = self.master

        # If there are new files, choose the newest as the master, then
        # relegate the current master to the concatenation list with:
            # i) the remaining new files, and; ii) any old files (i.e. that
            # predate the current master and have not been concatenated)
        try:
            new_master = new_unparsed_files[-1]
            new_unparsed_files = [self.master] + new_unparsed_files[:-1]
        except IndexError:
            pass
        files_to_parse = old_unparsed_files + new_unparsed_files
        return {'master_file': new_master, 'concat_files': files_to_parse}

def main(master_file=None, slave_path=None):
    """
    Handle writing of master file for ongoing concatenation of EddyPro
    daily summary files.

    Parameters
    ----------
    master_file : see description in EddyProConcatConfigurator.
    slave_path : see description in EddyProConcatConfigurator.

    Returns
    -------
    None.

    """

    epcc = EddyProConcatConfigurator(
        master_file=master_file,
        slave_path=slave_path
        )
    file_configs = epcc.get_file_configuration()
    if len(file_configs['concat_files']) == 0:
        raise FileNotFoundError('No files to parse')
    ep_handler = fh.DataHandler(
        file=file_configs['master_file'],
        concat_files=file_configs['concat_files'],
        )
    ep_handler.write_concatenation_report(
        abs_file_path=pathlib.Path(slave_path) / 'concatenation_report.txt'
        )
    ep_handler.write_conditioned_data(
        abs_file_path=epcc.master.parent / EP_MASTER_OUTPUT_NAME,
        )

# Args passed from term must be preceded with '--' (see below); either arg
if __name__=='__main__':

    parser = ap.ArgumentParser()
    parser.add_argument('--master_file')
    parser.add_argument('--slave_path')
    args = parser.parse_args()
    main(
        master_file=args.master_file,
        slave_path=args.slave_path
        )
