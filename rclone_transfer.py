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
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import paths_manager as pm


#------------------------------------------------------------------------------
### INITIAL CONFIGURATION ###
#------------------------------------------------------------------------------

PATHS = pm.paths()

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def rclone_pull_data(site, stream):
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
    
    logging.info(f'Begin pull of data stream "{stream}" for site "{site}"')
    from_remote = PATHS.get_remote_path(
        resource='cloudstor', stream=stream, site=site
        )
    logging.info('Pulling from remote directory: {}'.format(str(from_remote)))
    to_local = PATHS.get_local_path(
        resource='data', stream=stream, site=site, check_exists=True
        )
    logging.info('Pulling to local directory: {}'.format(str(to_local)))
    _rclone_push_pull_generic(
        source_dir=str(from_remote), target_dir=str(to_local),
        exclude_dirs=[]
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------           
def rclone_push_data(site, stream):
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
    
    logging.info(f'Begin push of data stream "{stream}" for site "{site}"')
    exclude_dict = {'flux_fast': 'TMP'}
    to_remote = PATHS.get_remote_path(
        resource='cloudstor', stream=stream, site=site
        )
    logging.info('Pushing to remote directory: {}'.format(str(to_remote)))
    from_local = PATHS.get_local_path(
        resource='data', stream=stream, site=site, check_exists=True
        )
    logging.info('Pushing from local directory: {}'.format(str(from_local)))
    exclude_dirs = (
        [] if not stream in exclude_dict else list(exclude_dict[stream])
        )
    _rclone_push_pull_generic(
        source_dir=str(from_local), target_dir=str(to_remote),
        exclude_dirs=exclude_dirs
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _rclone_push_pull_generic(source_dir, target_dir, exclude_dirs):

    # Set path to rclone
    app_str = PATHS.get_application_path(
        application='rclone', as_str=True, check_exists=False
        )
    
    # Set remote and local directories
    if 'cloudstor' in target_dir:
        remote = target_dir
        local = source_dir
    elif 'cloudstor' in source_dir:
        remote = source_dir
        local = target_dir
    else:
        logging.error(
            'One of either the source or target directory must be a valid '
            'cloudstor location'
            )
        return
    
    # Check remote exists
    logging.info('Checking remote directory is valid...')
    rslt = spc.run([app_str, 'lsd', remote], capture_output=True)
    try:
        rslt.check_returncode()
    except spc.CalledProcessError:
        logging.error(
            'Check failed! Return code: {0};\nDetails: {1}'
            .format(str(rslt.returncode), rslt.stderr.decode())
            )
        return
    logging.info('Found valid remote directory!')

    # Check local exists
    logging.info('Checking local directory is valid...')
    if not pathlib.Path(local).exists:
        logging.error('Check failed! Local directory not found!')
        return
    logging.info('Found valid local directory!')
    
    # Do copy
    logging.info('Copying now...')
    exec_list = [
        'copy', '--transfers', '36', '--progress', '--checksum',
        '--checkers', '48', '--timeout', '0'
        ]
    if exclude_dirs:
        for this_dir in exclude_dirs:
            exec_list.append('--exclude')
            exec_list.append(str(pathlib.Path('{}/**'.format(this_dir))))
    exec_list = [app_str] + exec_list + [source_dir, target_dir]
    rslt = spc.Popen(exec_list, stdout=spc.PIPE, stderr=spc.STDOUT)
    (out, err) = rslt.communicate()
    if out:
        logging.info(out.decode())
        logging.info('Copy complete')
    if err:
        logging.error(err.decode())
#------------------------------------------------------------------------------