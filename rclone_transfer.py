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
        self.app_path = PATHS.get_application_path(application='rclone', as_str=True)
        self.site = site
        self.stream = stream
        self.service = service
        self.args_list = self._build_args(exclude_dirs=exclude_dirs)
        self.local_path = PATHS.get_local_path(
            site=site, resource='data', stream=stream,
            )
        self.remote_path = PATHS.get_remote_path(
            resource=service, stream=stream, site=site, as_str=True
            )
        self.move_dict = {
            'push': (
                [self.app_path] + self.args_list +
                [self.local_path, self.remote_path]
                ),
            'pull': (
                [self.app_path] + self.args_list +
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
        msg = f'Local file {str(obj.local_path)} does not exist!'
        logging.error(msg); raise FileNotFoundError(msg)
    logging.info('    -> local directory valid')
    try:
        _run_subprocess(
            run_list=[obj.app_path, 'lsd', str(obj.remote_path)],
            timeout=10
            )
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error('Move failed!')
        raise
    logging.info('    -> remote location valid')
    logging.info('Moving data...')
    try:
        rslt = _run_subprocess(
            run_list=obj.move_dict[which_way],
            timeout=60
            )
        logging.info(rslt.stdout)
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logging.error(e)
        logging.error('Move failed!')
        raise
    logging.info('Move succeeded')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _run_subprocess(run_list, timeout=5):

    return spc.run(
        run_list, capture_output=True, timeout=timeout, check=True
        )
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
    use_site = site if not site in REMOTE_ALIAS_DICT else REMOTE_ALIAS_DICT[site]
    to_remote = PATHS.get_remote_path(
        resource='cloudstor', stream=stream, site=use_site
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
        application='rclone', check_exists=True, as_str=True
        )

    # Set remote and local directories
    if 'cloudstor' in target_dir:
        remote = target_dir
        local = source_dir
    elif 'cloudstor' in source_dir:
        remote = source_dir
        local = target_dir
    else:
        msg = (
            'One of either the source or target directory must be a valid '
            'cloudstor location'
            )
        logging.error(msg); raise RuntimeError

    # Check remote exists
    logging.info('Checking remote directory is valid...')
    rslt = spc.run([app_str, 'lsd', remote], capture_output=True)
    try:
        rslt.check_returncode()
    except spc.CalledProcessError:

        msg = (
            'Check failed! Return code: {0};\nDetails: {1}'
            .format(str(rslt.returncode), rslt.stderr.decode())
            )
        logging.error(msg); raise
    logging.info('Found valid remote directory!')

    # Check local exists
    logging.info('Checking local directory is valid...')
    if not pathlib.Path(local).exists:
        msg = 'Check failed! Local directory not found!'
        logging.error(msg); raise FileNotFoundError(msg)
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