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
import pathlib
import sys

#------------------------------------------------------------------------------
### SECONDARY IMPORTS ###
#------------------------------------------------------------------------------

import pandas as pd

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

sys.path.append(
    str(pathlib.Path(__file__).parent.parent.resolve() / 'profile')
    )
import data_parser as dp
import eddy_pro_concatenator as epc
import file_constructors as fc
import paths_manager as pm
import process_10hz_data as ptd
import profile_data_processor as pdp
import rclone_transfer as rt

#------------------------------------------------------------------------------
### INITIALISATION ###
#------------------------------------------------------------------------------

PathsManager = pm.paths()
LOG_BYTE_LIMIT = 10**6

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_logger(task, site=None):

    """Create logger and send output to file"""

    if site:
        logger_write_path = (
            PathsManager.get_local_path(
                resource='logs', site=site) / f'{site}_{task}.txt'
            )
    else:
        logger_write_path = (
            PathsManager.get_local_path(
                resource='generic_task_logs') / f'{task}.txt'
            )
    this_logger = logging.getLogger()
    this_logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(logger_write_path,
                                                   maxBytes=LOG_BYTE_LIMIT)
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
class TasksManager():

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
            PathsManager.get_local_path(resource='xl_variable_map'),
            sheet_name='Tasks', index_col='Site'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task):
        """
        Get the list of sites for which a given task is enabled.
        """

        return (
            self.tasks_df.loc[self.tasks_df[task]==True]
            .index
            .to_list()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_task_list_for_site(self, site):
        """
        Get the list of tasks enabled for a given site.
        """

        return (
            self.tasks_df.loc[site, self.tasks_df.loc[site]==True]
            .index
            .tolist()
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
    def run_task(self, task, site=None):

        site_only = {'site': site}

        tasks_dict = {

            'concat_EddyPro_data': {
                'func': concat_EddyPro_data,
                'args': site_only
                },

            'generate_L1_excel': {
                'func': generate_L1_excel,
                'args': site_only
                },

            'generate_merged_file': {
                'func': generate_merged_file,
                'args': site_only
                },

            'generate_site_details_file': {
                'func': fc.make_site_info_TOA5,
                'args': site_only
                },

            'process_profile_data': {
                'func': process_profile_data,
                'args': site_only
                },

            'Rclone_pull_slow_rdm': {
                'func': rt.pull_slow_flux,
                'args': site_only
                },

            'Rclone_push_fast_rdm': {
                'func': rt.push_fast_flux,
                'args': site_only
                },

            'Rclone_push_profile_rdm': {
                'func': rt.push_profile_processed,
                'args': site_only
                },

            'Rclone_push_slow_rdm': {
                'func': rt.push_slow_flux,
                'args': site_only
                },

            'Rclone_push_rtmc_rdm': {
                'func': rt.push_rtmc,
                'args': site_only
                },

            'Rclone_push_status': {
                'func': rt.push_status_files,
                'args': None
                },

            'reformat_10Hz_main': {
                'func': ptd.main,
                'args': {'site': site, 'system': 'main'}
                },

            'reformat_10Hz_under': {
                'func': ptd.main,
                'args': {'site': site, 'system': 'under'}
                },

            'update_site_status': {
                'func': update_site_status,
                'args': None
                }

            }

        sub_dict = tasks_dict[task]
        if sub_dict['args']:
            sub_dict['func'](**sub_dict['args'])
        else:
            sub_dict['func']()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def concat_EddyPro_data(site):

    slave_path = PathsManager.get_local_path(
        resource='data', site='CumberlandPlain', stream='flux_slow'
        )
    master_file = slave_path / 'EP_MASTER.txt'
    try:
        epc.main(master_file=master_file, slave_path=slave_path)
    except FileNotFoundError:
        epc.main(slave_path=slave_path)
    finally:
        pass

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_L1_excel(site):
    """
    Pull data from downloaded tables and merge into a correctly formatted L1
    spreadsheet.

    Parameters
    ----------
    site : str
        Site name.

    Returns
    -------
    None.

    """

    constructor = fc.L1Constructor(site=site)
    constructor.write_to_excel()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_merged_file(site):
    """
    Pull data from downloaded tables and merge into single table with
    standard names for variables. Used for RTMC plotting.

    Parameters
    ----------
    site : str
        Site name.

    Returns
    -------
    None.

    """

    merger = dp.SiteDataMerger(site=site)
    merger.write_all_as_TOA5()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def process_profile_data(site):

    processor = pdp.get_site_profile_class(site=site)
    output_path = PathsManager.get_local_path(
            resource='data', stream='profile_proc', site=site
            )
    processor.write_to_csv(file_name=output_path / 'storage_data.csv')
    processor.plot_time_series(
        output_to_file=output_path / 'storage_time_series_plot.png',
        open_window=False
        )
    processor.plot_diel_storage_mean(
        output_to_file=output_path / 'storage_diel_mean_plot.png',
        open_window=False
        )
    processor.plot_vertical_evolution_mean(
        output_to_file=output_path / 'vertical_time_evoln_mean.png',
        open_window=False
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def update_site_status():
    """
    Build site status spreadsheet.

    Returns
    -------
    None.

    """

    constructor = fc.SiteStatusConstructor()
    constructor.write_to_excel()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

# Parse single task for single site and log it
def parse_task(task, site=None):

    tasks = TasksManager()

    # Set sites to empty list, then:
        # 1. if site kwarg passed, then run task for that site;
        # 2. if no site kwarg passed, get list of sites for which to run task,
        #    and execute
        # 3. if task is not site specific, leave empty list in place
    sites = []
    if site:
        sites = [site]
    else:
        try:
            sites = tasks.get_site_list_for_task(task=task)
        except KeyError:
            pass

    # If there are sites in the list, parse the task for those sites,
    # or run generic task
    if sites:
        for site in sites:
            logger = _set_logger(task=task, site=site)
            logger.info(f'Running task "{task}" for site {site}')
            try:
                tasks.run_task(task=task, site=site)
            except Exception as e:
                logging.info(f'Task failed with the following error: {e}')
            logger.info('Task complete\n')
            logger.handlers.clear()
    else:
        logger = _set_logger(task=task)
        logger.info(f'Running task "{task}"')
        try:
            tasks.run_task(task=task, site=site)
        except Exception as e:
            logging.info(f'Task failed with the following error: {e}')
        logger.info('Task complete\n')
        logger.handlers.clear()

# Main function - first arg passed is task name, second arg (site) is optional;
# if not passed, run for all sites for which task is enabled (in spreadsheet);
# if passed, check that task is enabled for that site and run if so (otherwise do nothing).
if __name__=='__main__':

    task = sys.argv[1]
    try:
        parse_task(task=task, site=sys.argv[2])
    except IndexError:
        parse_task(task=task)
