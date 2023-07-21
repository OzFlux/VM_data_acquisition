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

PATHS = pm.paths()
APP_PATH = PATHS.get_application_path(application='rclone', as_str=True)
REMOTE_ALIAS_DICT = {'AliceSpringsMulga': 'AliceMulga'}
ARGS_LIST = [
    'copy', '--transfers', '36', '--progress', '--checksum', '--checkers',
    '48', '--timeout', '0'
    ]
ALLOWED_STREAMS = ['flux_slow', 'flux_fast', 'rtmc']
ALLOWED_SERVICES = ['nextcloud', 'cloudstor']

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class RcloneTransferConfig():

    #--------------------------------------------------------------------------
    def __init__(self, site, stream, service, exclude_dirs=None):
        """
        Configuration class for checking inputs and generating eval strings for
        rclone subprocess.

        Parameters
        ----------
        site : str
            Site.
        stream : str
            Data stream (see ALLOWED_STREAMS).
        service : str
            The cloud service (see ALLOWED_SERVICES).
        exclude_dirs : list, optional
            Directories to exclude from sync. The default is None.

        Raises
        ------
        KeyError
            Raised if either stream or service is not in allowed lists.

        Returns
        -------
        None.

        """

        if not service in ALLOWED_SERVICES:
            raise KeyError(
                '"service" kwarg must be one of {}'
                .format(', '.join(ALLOWED_SERVICES))
                )
        if not stream in ALLOWED_STREAMS:
            raise KeyError(
                '"stream" kwarg must be one of {}'
                .format(', '.join(ALLOWED_STREAMS))
                )
        self.site = site
        self.stream = stream
        self.service = service
        self.args_list = self._build_args(exclude_dirs=exclude_dirs)
        self.local_path = PATHS.get_local_path(
            site=site, resource='data', stream=stream,
            )
        try:
            remote_site_name = REMOTE_ALIAS_DICT[site]
        except KeyError:
            remote_site_name = site
        self.remote_path = PATHS.get_remote_path(
            resource=service, stream=stream, site=remote_site_name, as_str=True
            )
        self.move_dict = {
            'push': (
                [APP_PATH] + self.args_list +
                [self.local_path, self.remote_path]
                ),
            'pull': (
                [APP_PATH] + self.args_list +
                [self.remote_path, self.local_path]
                )
            }

    def _build_args(self, exclude_dirs):
        """
        Append the list of excluded directories to the end of the rclone
        arguments list.

        Parameters
        ----------
        exclude_dirs : list
            The directories to be added to the arguments.

        Raises
        ------
        TypeError
            Raised if exclude_dirs isn't a list.

        Returns
        -------
        this_list : list
            The expanded args list (if applicable).

        """

        this_list = ARGS_LIST.copy()
        if not exclude_dirs:
            return this_list
        if not isinstance(exclude_dirs, list):
            raise TypeError(
                'Arg "exclude_dirs" must be a list of directory strings!'
                )
        for this_dir in exclude_dirs:
            this_list.append('--exclude')
            this_list.append(str(pathlib.Path('{}/**'.format(this_dir))))
        return this_list
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_data(site, stream, service, which_way='push', exclude_dirs=None):
    """
    Do the move!

    Parameters
    ----------
    site : str
        Site.
    stream : str
        Data stream (see ALLOWED_STREAMS).
    service : str
        The cloud service (see ALLOWED_SERVICES).
    which_way : str, optional
        'push' or 'pull'. The default is 'push'.
    exclude_dirs : list, optional
        Directories to exclude from sync. The default is None.

    Raises
    ------
    KeyError
        Raised if which_way is not either 'push' or 'pull'.

    Returns
    -------
    None.

    """

    if not which_way in ['push', 'pull']:
        raise KeyError('Arg "which_way" must be "push" or "pull"')
    logging.info(
        f'Begin {which_way} of data stream "{stream}" for site "{site}":'
        )
    obj = RcloneTransferConfig(site=site, stream=stream, service=service)
    logging.info('Copying now...\n Checking local and remote directories...')
    if not obj.local_path.exists:
        msg = f'    -> local file {str(obj.local_path)} is not valid!'
        logging.error(msg); raise FileNotFoundError(msg)
    logging.info(f'    -> local directory {obj.local_path} is valid')
    try:
        check_remote_available(str(obj.remote_path))
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error(f'    -> remote location {str(obj.local_path)} is not valid!')
        raise
    logging.info(f'    -> remote location {str(obj.remote_path)} is valid')
    logging.info('Moving data...')
    try:
        rslt = _run_subprocess(
            run_list=obj.move_dict[which_way],
            timeout=120
            )
        logging.info(rslt.stdout.decode())
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error('Move failed!')
        raise
    logging.info('Move succeeded')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_remote_available(remote_path):

    _run_subprocess(
        run_list=[APP_PATH, 'lsd', str(remote_path)],
        timeout=30
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _run_subprocess(run_list, timeout=5):

    return spc.run(
        run_list, capture_output=True, timeout=timeout, check=True
        )
#------------------------------------------------------------------------------
