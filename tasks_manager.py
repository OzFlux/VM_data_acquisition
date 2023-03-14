# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:29:27 2022

This code coordinates and executes the tasks to be run across sites, including
a batch function that can be run for all sites for which the given task is
enabled. This list is specified in a spreadsheet (Tasks tab) that resides on
Cloudstor.

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import logging
from logging import handlers
import pandas as pd
import sys
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import campbell_funcs as cf
import file_merger as fm
import L1_parser as l1p
import paths_manager as pm
import process_10hz_data_new as ptd
import rclone_transfer as rt

#------------------------------------------------------------------------------
### INITIALISATION ###
#------------------------------------------------------------------------------

PATHS = pm.paths()
log_byte_limit = 10**6

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_logger(site, task):

    """Create logger and send output to file"""

    logger_write_path = (
        PATHS.get_local_path(resource='logs', site=site) / f'{site}_{task}.txt'
        )
    this_logger = logging.getLogger()
    this_logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(logger_write_path,
                                                   maxBytes=log_byte_limit)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [%(name)s] %(message)s'
        )
    handler.setFormatter(formatter)
    this_logger.addHandler(handler)
    return this_logger
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class tasks_manager():

    def __init__(self):

        self.tasks_df = self._make_df()

    def _make_df(self):
        """
        Construct the dataframe containing listed tasks to be executed for each
        site in the collection - read from the spreadsheet (Tasks tab).

        Returns
        -------
        pd.core.Frame.DataFrame
            Dataframe containing the tasks that need to be executed for each
            site.

        """

        return pd.read_excel(
            PATHS.get_local_path(resource='xl_variable_map'),
            sheet_name='Tasks', index_col='Site'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def generate_merged_file(self, site):
        """
        Pull data from downloaded tables and merge into single table with
        standard names for variables. Used for RTMC plotting.

        Parameters
        ----------
        site : str
            Site name.

        """

        merger = fm.file_merger(site=site)
        output_path = (
            PATHS.get_local_path(
                resource='data', stream='flux_slow', site=site
                )
            / '{}_merged_std.dat'.format(site)
            )
        merger.make_output_file(dest=output_path)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def generate_L1_excel(self, site):

        cf.make_site_L1(site=site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def generate_site_details_file(self, site):
        """
        Create the file containing the site information in dummy TOA5 format,
        and write to the appropriate directory.

        Parameters
        ----------
        site : str
            Site name.

        """

        cf.make_site_info_TOA5(site=site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task_string):
        """
        Get the list of sites for which a given task is enabled.
        """

        return (
            self.tasks_df.loc[self.tasks_df[task_string]==True]
            .index
            .to_list()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_task_list(self):
        """
        Get the list of tasks.
        """

        return self.tasks_df.columns.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def rclone_push_data(self, site, stream):
        """
        Push data from local to Cloudstor folders.

        Parameters
        ----------
        site : str
            Site name.
        stream : str
            The data stream to push (limited to flux_slow, flux_fast and
                                     RTMC).

        """

        rt.rclone_push_data(site=site, stream=stream)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def rclone_pull_data(self, site, stream):
        """
        Pull data from Cloudstor to local folders. Currently limited to slow
        data.

        Parameters
        ----------
        site : str
            Site name.
        stream : str
            The data stream to pull (limited to flux_slow, flux_fast and
                                     RTMC).

        """

        rt.rclone_pull_data(site=site, stream=stream)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def reformat_10Hz_data(self, site):

        ptd.main(site=site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def run_task(self, site, task_string):
        """
        Run a task from the task list.

        Parameters
        ----------
        site : str
            Site name.
        task_string : str
            The name of the task to run.

        Returns
        -------
        None.

        """

        configs_dict = {
            'generate_L1_excel': {
                'function': self.generate_L1_excel, 'stream': None
                },
            'generate_merged_file': {
                'function': self.generate_merged_file, 'stream': None
                },
            'generate_site_details_file': {
                'function': self.generate_site_details_file, 'stream': None
                },
            'Rclone_push_slow': {
                'function': self.rclone_push_data, 'stream': 'flux_slow'
                },
            'Rclone_pull_slow': {
                'function': self.rclone_pull_data, 'stream': 'flux_slow'
                },
            'Rclone_push_fast': {
                'function': self.rclone_push_data, 'stream': 'flux_fast'
                },
            'Rclone_push_RTMC': {
                'function': self.rclone_push_data, 'stream': 'RTMC'
                },
            'reformat_10Hz': {
                'function': self.reformat_10Hz_data, 'stream': None
                },
            }

        task_function = configs_dict[task_string]['function']
        task_stream = configs_dict[task_string]['stream']
        if task_stream:
            task_function(site=site, stream=task_stream)
        else:
            task_function(site=site)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

# Parse single task for single site and log it
def parse_task(task, site):

    tasks = tasks_manager()
    logger = _set_logger(site=site, task=task)
    logger.info(f'Running task "{task}" for site {site}')
    try:
        tasks.run_task(site=site, task_string=task)
    except Exception as e:
        logging.info('Task failed with the following error: {}'.format(e))
    logger.info('Task complete\n')
    logger.handlers.clear()

# Main function - first arg passed is task name, second arg (site) is optional;
# if not passed, run for all sites for which task is enabled (in spreadsheet);
# if passed, check that task is enabled for that site and run if so (otherwise do nothing).
if __name__=='__main__':

    tasks = tasks_manager()
    task = sys.argv[1]
    site_list = tasks.get_site_list_for_task(task_string=task)
    try:
        site = sys.argv[2]
        if site in site_list:
            parse_task(task=task, site=site)
    except IndexError:
        for site in site_list:
            parse_task(task=task, site=site)