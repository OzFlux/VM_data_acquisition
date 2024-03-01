# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 14:58:27 2024

@author: jcutern-imchugh
"""

import pathlib
import sys

import paths_manager as pm
import data_mapper as dm
sys.path.append(str(pathlib.Path(__file__).parents[1] / 'site_details'))
from sparql_site_details import site_details as sd


class MetaDataManager():

    def __init__(
            self, site, details=False, paths=False,
            mapped_data_mngr=False, file_mngr=False, mapper_source_field=None
            ):

        if details:
            self.Details = sd().get_single_site_details(site=site)
        if paths:
            self.Paths = pm.SitePaths(site=site)
        if mapped_data_mngr:
            self.Mapper = dm.MappedDataManager(
                site=site, default_source_field=mapper_source_field
                )
        if file_mngr:
            self.Files = dm.FileManager(site=site)
