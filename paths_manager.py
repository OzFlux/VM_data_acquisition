# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:43:45 2022

@author: jcutern-imchugh

Contains classes and functions to find correct input / output paths for site
and system resources, in both local and remote locations. Note that the source
for the requisite metadata is a Windows initialisation file containing the
paths to various resources (applications, metadata spreadsheets etc) and data
streams. Main classes are:
    Paths. Returns pathlib.Path objects for various local and remote resourcers
    and data streams.
    GenericPaths. Convenience class that simply lists local resources and
    applications as pandas series, which can be accessed using dot notation.
    SitePaths. Convenience class that simply lists resources and applications
    as pandas series, which can be accessed using dot notation. In contrast to
    GenericPaths, it contains site-specific information.
"""

from configparser import ConfigParser
import pandas as pd
import pathlib

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

PATHS_FILE = 'paths.ini'
REMOTE_ALIAS_DICT = {
    'AliceSpringsMulga': 'AliceMulga', 'Longreach': 'MitchellGrassRangeland'
    }
PLACEHOLDER = '<site>'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class GenericPaths():

    def __init__(self):

        self._Paths = Paths()
        self.local_resources = self._get_generic_resources()
        self.applications = self._get_applications()

        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_generic_resources(self):

        return pd.Series({
            resource: self._Paths.get_local_resource_path(
                resource=resource
                )
            for resource in self._Paths.list_local_resources()
            })
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_applications(self):

        return pd.Series({
            app: self._Paths.get_application_path(application=app) for app in
            self._Paths._config['APPLICATIONS']}
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SitePaths(GenericPaths):

    def __init__(self, site):

        super().__init__()
        self.site = site
        self.local_resources = (
            self.local_resources.apply(
                lambda x: pathlib.Path(str(x).replace(PLACEHOLDER, site))
                )
            )
        self.local_data = self._get_local_data_paths()
        self.remote_data = self._get_remote_data_paths()

    #--------------------------------------------------------------------------
    def _get_local_data_paths(self):

        return pd.Series({
            data_stream: self._Paths.get_local_data_path(
                data_stream=data_stream, site=self.site
                )
            for data_stream in self._Paths.list_local_data_streams()
            })
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_remote_data_paths(self):

        return pd.Series({
            data_stream: self._Paths.get_remote_data_path(
                data_stream=data_stream, site=self.site
                )
            for data_stream in self._Paths.list_remote_data_streams()
            })
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Paths():
    """

    """

    def __init__(self):
        """
        Create the configreader object and placeholder str as attributes.

        Returns
        -------
        None.

        """

        self._config = ConfigParser()
        self._config.read(pathlib.Path(__file__).parent / PATHS_FILE)
        self._placeholder = PLACEHOLDER

    ###########################################################################
    ### BEGIN LOCAL DATA METHODS ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def list_local_resources(self):
        """
        List the local resources that are defined in the .ini file.

        Returns
        -------
        list
            The resources.

        """

        return list(self._config['LOCAL_PATH'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_local_resource_path(self, resource, site=None, **kwargs):
        """
        Get the path to the local resource.

        Parameters
        ----------
        resource : str
            the local resource for which to return the path.
            The default is None.
        site : str, optional
            Site name. The default is None.

        Returns
        -------
        pathlib.Path
            Path to local resource.

        """
        kwargs.pop('data_stream', None)
        return self.get_path(
            base_location='LOCAL_PATH', resource=resource, site=site, **kwargs
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_local_data_streams(self):
        """
        List the available local data streams defined in the .ini file.

        Returns
        -------
        list
            The data streams.

        """

        return list(self._config['LOCAL_DATA'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_local_data_path(self, **kwargs):
        """
        Returns the local data path for the given data stream.

        Parameters
        ----------
        **kwargs : dict
            See get_path method for documentation of kwargs.

        Returns
        -------
        pathlib.Path
            Path to local data.

        """

        return self.get_path(
            base_location='LOCAL_PATH', resource='data', **kwargs
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_image_path(self, img_type, **kwargs):
        """
        Get the path to site image.

        Parameters
        ----------
        img_type : str
            Either 'tower' or 'contour'.
        **kwargs : dict
            See get_path method for documentation of kwargs.

        Returns
        -------
        pathlib.Path
            Path to local image.

        """

        kwargs.pop('subdirs', None)
        img_dict = {'tower': '<site>_tower.jpg', 'contour': '<site>_contour.png'}
        return self.get_path(
                base_location='LOCAL_PATH', resource='site_images',
                file_name=img_dict[img_type], **kwargs
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_applications(self):
        """
        List the available local applications defined in the .ini file.

        Returns
        -------
        list
            The applications.

        """

        return list(self._config['APPLICATIONS'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_application_path(self, application, **kwargs):
        """
        Get the path to application.

        Parameters
        ----------
        application : str
            Application name.
        **kwargs : dict
            See get_path method for documentation of kwargs. Note that kwargs
            'data_stream', 'site' 'subdirs' and 'file_name' do not apply.

        Returns
        -------
        pathlib.Path
            Path to application.

        """

        [
            kwargs.pop(arg, None) for arg in
            ['data_stream', 'site' 'subdirs', 'file_name']
            ]
        return self.get_path(
            base_location='APPLICATIONS', resource=application
            )
    #--------------------------------------------------------------------------

    ###########################################################################
    ### END LOCAL DATA METHODS ###
    ###########################################################################

    ###########################################################################
    ### BEGIN REMOTE DATA METHODS ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def list_remote_storages(self):
        """
        List the available remote storages defined in the .ini file.

        Returns
        -------
        list
            The storages.

        """

        return list(self._config['REMOTE_STORAGE'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_remote_data_path(self, data_stream, **kwargs):
        """
        Returns the remote data path for the given data stream.

        Parameters
        ----------
        data_stream : str
            The data stream for which to return the path.
        **kwargs : dict
            See get_path method for documentation of kwargs.

        Returns
        -------
        pathlib.Path
            Path to remote data.

        """

        storage = self._config['REMOTE_DATA_LINKAGES'][data_stream]
        return self.get_path(
            base_location='REMOTE_STORAGE', resource=storage,
            data_stream=data_stream, **kwargs
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_remote_data_streams(self):
        """
        List the available remote data streams defined in the .ini file.

        Returns
        -------
        list
            The data streams.

        """

        return list(self._config['REMOTE_DATA'])
    #--------------------------------------------------------------------------

    ###########################################################################
    ### END REMOTE DATA METHODS ###
    ###########################################################################

    ###########################################################################
    ### BEGIN GENERIC PUBLIC METHODS ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def get_path(self, base_location, resource, data_stream=None, site=None,
                 subdirs=[], file_name=None, check_exists=False,
                 as_str=False
                 ):
        """
        Return the path to resources or data.

        Parameters
        ----------
        base_location : str
            The level of the ini file under which to find the resource.
        resource : str
            The resource to grab.
        data_stream : str, optional
            DESCRIPTION. The default is None.
        site : TYPE, optional
            DESCRIPTION. The default is None.
        subdirs : TYPE, optional
            DESCRIPTION. The default is [].
        file_name : TYPE, optional
            DESCRIPTION. The default is None.
        check_exists : TYPE, optional
            DESCRIPTION. The default is False.
        as_str : TYPE, optional
            DESCRIPTION. The default is False.

        Raises
        ------
        TypeError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        stream_dict = {
            'LOCAL_PATH': 'LOCAL_DATA',
            'REMOTE_STORAGE': 'REMOTE_DATA'
            }

        if not isinstance(subdirs, list):
            if not isinstance(subdirs, str):
                raise TypeError('kwarg "subdirs" must be of type list or str!')
            subdirs = [subdirs]
        path = pathlib.Path(self._config[base_location][resource])
        elements = [] + subdirs
        if file_name:
            elements.append(file_name)
        if data_stream:
            stream_header = stream_dict[base_location]
            path = path / self._config[stream_header][data_stream]
        if elements:
            path = self._add_subdirs(path=path, subdirs_list=elements)
        if site:
            if base_location == 'REMOTE_PATH':
                try:
                    site = REMOTE_ALIAS_DICT[site]
                except KeyError:
                    pass
            path = pathlib.Path(self._insert_site_str(target_obj=path, site=site))
        if check_exists:
            self._check_exists(path=path)
        if as_str:
            return str(path)
        return path
    #--------------------------------------------------------------------------

    ###########################################################################
    ### END GENERIC PUBLIC METHODS ###
    ###########################################################################

    ###########################################################################
    ### BEGIN PRIVATE METHODS ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def _add_subdirs(self, path, subdirs_list):
        """
        Concatenate path with listed subdirectories.

        Parameters
        ----------
        path : pathlib.Path
            The existing path to which to concatenate subdirectories.
        subdirs_list : list
            List of subdirectories.

        Returns
        -------
        path : pathlib.Path
            The concatenated path.

        """

        for subdir in subdirs_list:
            path = path / subdir
        return path
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_exists(self, path):
        """
        Check a given path exists.

        Parameters
        ----------
        path : pathlib.Path
            The path to check.

        Raises
        ------
        FileNotFoundError
            Raised if does not exist.

        Returns
        -------
        None.

        """

        if not path.exists():
            raise FileNotFoundError('No such path!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _insert_site_str(self, target_obj, site):
        """


        Parameters
        ----------
        target_obj : pathlib.Path
            DESCRIPTION.
        site : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        if isinstance(target_obj, pathlib.Path):
            return pathlib.Path(str(target_obj).replace(PLACEHOLDER, site))
        return target_obj.replace(PLACEHOLDER, site)
    #--------------------------------------------------------------------------

    ###########################################################################
    ### END PRIVATE METHODS ###
    ###########################################################################

#------------------------------------------------------------------------------