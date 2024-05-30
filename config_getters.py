# -*- coding: utf-8 -*-
"""
Created on Tue May 28 16:35:05 2024

@author: jcutern-imchugh
"""

import pathlib
import yaml

#------------------------------------------------------------------------------
config_paths_file = pathlib.Path('E:/Config_files/Paths/local_paths.yml')
with open(config_paths_file) as f:
    config_paths = yaml.safe_load(stream=f)['configs']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_hardware_configs(site):
    """
    Get the site hardware configuration file content.

    Args:
        site: name of site for which to return hardware configuration dictionary.

    Returns:
        Hardware configurations.

    """

    return _get_site_configs(site=site, which='hardware')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_variable_configs(site: str) -> dict:
    """
    Get the site variable configuration file content.

    Args:
        site: site for which to return variable configuration dictionary.

    Returns:
        Dictionary with variable attributes.

    """

    return _get_site_configs(site=site, which='variables')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_configs(site, which):
    """
    Get the site hardware configuration file content.

    Args:
        site: name of site for which to return hardware configuration dictionary.
        which: .

    Returns:
        Hardware configurations.

    """

    file = (
        pathlib.Path(config_paths['base_path']) /
        config_paths['stream'][which].replace('<site>', site)
        )
    with open(file) as f:
        return yaml.safe_load(stream=f)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_nc_dim_attrs():

    return _get_generic_configs(which='nc_dim_attrs')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_nc_global_attrs():

    return _get_generic_configs(which='nc_generic_attrs')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_generic_configs(which):

    file = (
        pathlib.Path(config_paths['base_path']) /
        config_paths['stream'][which]
        )
    with open(file) as f:
        return yaml.safe_load(stream=f)
#------------------------------------------------------------------------------
