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

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import file_constructors as fc
import paths_manager as pm
import process_10hz_data as ptd
import rclone_transfer as rt

#------------------------------------------------------------------------------
### INITIALISATION ###
#------------------------------------------------------------------------------

PathsManager = pm.paths()
log_byte_limit = 10**6

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_logger(site, task):

    """Create logger and send output to file"""

    logger_write_path = (
        PathsManager.get_local_path(
            resource='logs', site=site) / f'{site}_{task}.txt'
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
    def get_task_list(self):
        """
        Get the list of tasks.
        """

        return self.tasks_df.columns.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def run_task(self, site, task):

        site_only = {'site': site}

        tasks_dict = {

            'generate_L1_excel': {
                'func': generate_L1_excel,
                'args': site_only
                },

            'generate_merged_file': {
                'func': generate_merged_file,
                'args': site_only
                },

            'generate_site_details_file': {
                'func': generate_site_details_file,
                'args': site_only
                },

            'Rclone_push_slow': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'flux_slow', 'service':'cloudstor',
                    'which_way':'push'
                    }
                },

            'Rclone_pull_slow': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'flux_slow', 'service':'cloudstor',
                    'which_way':'pull'
                    }
                },

            'Rclone_push_rtmc': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'rtmc', 'service':'cloudstor',
                    'which_way':'push'
                    }
                },

            'Rclone_push_fast_rdm': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'flux_fast', 'service':'ten_Hz_archive',
                    'which_way':'push'
                    }
                },

            'Rclone_push_slow_rdm': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'flux_slow', 'service':'nextcloud',
                    'which_way':'push'
                    }
                },

            'Rclone_push_rtmc_rdm': {
                'func': rclone_move_data,
                'args': {
                    'site':site, 'stream':'rtmc', 'service':'nextcloud',
                    'which_way':'push'
                    }
                },

            'reformat_10Hz_main': {
                'func': reformat_10Hz_data,
                'args': {'site': site, 'system': 'main'}
                },

            'reformat_10Hz_under': {
                'func': reformat_10Hz_data,
                'args': {'site': site, 'system': 'under'}
                }

            }

        sub_dict = tasks_dict[task]
        sub_dict['func'](**sub_dict['args'])
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###

#------------------------------------------------------------------------------
def generate_L1_excel(**kwargs):

    constructor = fc.L1Constructor(site=kwargs['site'])
    constructor.write_to_excel()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_merged_file(**kwargs):
    """
    Pull data from downloaded tables and merge into single table with
    standard names for variables. Used for RTMC plotting.

    Parameters
    ----------
    site : str
        Site name.

    """

    merger = fc.TableMerger(site=kwargs['site'])
    merger.make_output_file()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_site_details_file(**kwargs):
    """
    Create the file containing the site information in dummy TOA5 format,
    and write to the appropriate directory.

    Parameters
    ----------
    site : str
        Site name.

    """

    fc.make_site_info_TOA5(site=kwargs['site'])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def rclone_move_data(**kwargs):

    exclude_dirs = ['TMP'] if kwargs['stream'] == 'flux_fast' else None
    rt.move_data(
        site=kwargs['site'],
        stream=kwargs['stream'],
        service=kwargs['service'],
        which_way=kwargs['which_way'],
        exclude_dirs=exclude_dirs
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reformat_10Hz_data(**kwargs):

    ptd.main(site=kwargs['site'], system=kwargs['system'])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

# Parse single task for single site and log it
def parse_task(task, site=None):

    tasks = TasksManager()
    if site:
        sites = [site]
    else:
        sites = tasks.get_site_list_for_task(task=task)
    for site in sites:
        logger = _set_logger(site=site, task=task)
        logger.info(f'Running task "{task}" for site {site}')
        try:
            tasks.run_task(site=site, task=task)
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
