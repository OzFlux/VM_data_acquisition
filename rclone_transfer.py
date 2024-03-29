# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 11:25:28 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import logging
import pathlib
import subprocess as spc

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import paths_manager as pm

#------------------------------------------------------------------------------
### INITIAL CONFIGURATION ###
#------------------------------------------------------------------------------

PATHS = pm.Paths()
APP_PATH = PATHS.get_application_path(application='rclone')
ARGS_LIST = [
    'copy', '--transfers', '36', '--progress', '--checksum', '--checkers',
    '48', '--timeout', '0'
    ]
ALLOWED_STREAMS = ['flux_slow', 'flux_fast', 'rtmc']
ALLOWED_REMOTES = PATHS.list_remote_storages()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generic_move(
        local_location, remote_location, which_way='to_remote',
        exclude_dirs=None, timeout=600
        ):

    # Check direction is valid
    if not which_way in ['to_remote', 'from_remote']:
        raise KeyError('Arg "which_way" must be "to_remote" or "from_remote"')

    # Check local and remote locations are valid
    logging.info('Checking local and remote directories...')
    if not pathlib.Path(local_location).exists:
        msg = f'    -> local file {str(local_location)} is not valid!'
        logging.error(msg); raise FileNotFoundError(msg)
    logging.info(f'    -> local directory {local_location} is valid')
    try:
        check_remote_available(str(remote_location))
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error(
            f'    -> remote location {remote_location} is not valid!'
            )
        raise
    logging.info(f'    -> remote location {remote_location} is valid')

    # Set from and to locations, based on direction
    if which_way == 'to_remote':
        from_location = local_location
        to_location = remote_location
    else:
        from_location = remote_location
        to_location = local_location

    # Do the transfer
    run_args = ARGS_LIST.copy()
    if exclude_dirs:
        run_args += _add_rclone_exclude(exclude_dirs=exclude_dirs)
    logging.info('Copying now...')
    run_list =  [APP_PATH] + run_args + [from_location, to_location]
    try:
        rslt = _run_subprocess(run_list=run_list, timeout=timeout)
        logging.info(rslt.stdout.decode())
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error('Copy failed!')
        raise
    logging.info('Copy succeeded')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def pull_slow_flux(site):

    logging.info(f'Begin retrieval of {site} slow data from UQRDM')
    _move_site_data_stream(
        site=site, stream='flux_slow', which_way='from_remote'
        )
    logging.info('Done')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_files():

    logging.info('Begin move of status files to UQRDM')
    generic_move(
        local_location=PATHS.get_local_resource_path(resource='network_status'),
        remote_location=PATHS.get_remote_data_path(data_stream='epcn_share'),
        timeout=180
        )
    logging.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_fast_flux(site):

    logging.info(f'Begin move of {site} fast data to UQRDM flux archive')
    _move_site_data_stream(
        site=site, stream='flux_fast', exclude_dirs=['TMP'], timeout=1200
        )
    logging.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_slow_flux(site):

    logging.info(f'Begin move of {site} slow flux data to UQRDM')
    _move_site_data_stream(site=site, stream='flux_slow')
    logging.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_profile_processed(site):

    logging.info(f'Begin move of {site} processed profile data to UQRDM')
    _move_site_data_stream(site=site, stream='profile_proc')
    logging.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_rtmc(site):

    logging.info(f'Begin move of {site} RTMC images to UQRDM')
    _move_site_data_stream(site=site, stream='rtmc')
    logging.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _move_site_data_stream(
        site, stream, exclude_dirs=None, which_way='to_remote', timeout=600
        ):

    local_path = _reformat_path_str(
        PATHS.get_local_data_path(site=site, data_stream=stream, as_str=True)
        )
    remote_path = _reformat_path_str(
        PATHS.get_remote_data_path(data_stream=stream, site=site,as_str=True)
        )
    generic_move(
        local_location=local_path, remote_location=remote_path,
        exclude_dirs=exclude_dirs, which_way=which_way, timeout=timeout
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_remote_available(remote_path):

    _run_subprocess(
        run_list=[APP_PATH, 'lsd', str(remote_path)],
        timeout=30
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _add_rclone_exclude(exclude_dirs):

    this_list = []
    for this_dir in exclude_dirs:
        this_list.append('--exclude')
        this_list.append(f'{this_dir}/**')
    return this_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _reformat_path_str(path_str):

    return path_str.replace('\\', '/')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _run_subprocess(run_list, timeout=5):

    return spc.run(
        run_list, capture_output=True, timeout=timeout, check=True
        )
#------------------------------------------------------------------------------