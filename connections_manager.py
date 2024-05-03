# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh
"""

import json
import pandas as pd
import pathlib
import yaml

import file_io as io
from paths_manager import GenericPaths as gp
import paths_manager as pm
import logger_functions as lf



paths = pm.Paths()

###############################################################################
### BEGIN MULTI-SITE CONFIGURATION GENERATOR SECTION ###
###############################################################################

class ConfigsGenerator():
    """Class to read and interrogate data from excel file, and write
    operational configuration files - mostly used if config input data changes!
    """

    def __init__(self):

        self.modem_table = _read_excel_fields(sheet_name='Modems')
        self.modem_fields = self.modem_table.columns.tolist()
        self.sites = self.modem_table.index.tolist()
        self.logger_table = _read_excel_fields(sheet_name='Loggers')
        self.logger_fields = self.logger_table.columns.tolist()

    def get_site_logger_list(self, site: str) -> list:
        """
        Get list of loggers for a given site.

        Args:
            site: name of site.

        Returns:
            List of loggers.

        """

        return self.logger_table.loc[[site], 'logger'].tolist()

    def get_site_logger_details(
            self, site: str, logger: str=None, field: str=None
            ) -> pd.DataFrame | pd.Series | str:
        """
        Get details of loggers for a given site.

        Args:
            site: the site.
            logger: logger name for which to return details. Defaults to None.
            field: field to return. Defaults to None.

        Returns:
            Logger details. If optional kwargs are not specified, returns a
            string. If logger is specified, return a series.
            If field is specified, return a str.

        """

        sub_df = self.logger_table.loc[[site]].set_index(keys='logger')
        if not logger is None:
            sub_df = sub_df.loc[logger]
        if not field is None:
            sub_df = sub_df[[field]]
        if not field:
            return sub_df
        return sub_df[field]

    def get_site_modem_details(
            self, site: str, field: str=None
            ) -> pd.Series | str:
        """
        Get details of modem for a given site.

        Args:
            site: name of site.
            field: field to return. Defaults to None.

        Returns:
            Details of modem.

        """

        if field is None:
            return self.modem_table.loc[site]
        return self.modem_table.loc[site, field]

    def get_routable_sites(self) -> list:
        """
        Get list of sites that (should) have working ovpn connections.

        Returns:
            The sites.

        """

        return (
            self.modem_table[self.modem_table.routable!=0]
            .index
            .unique()
            .tolist()
            )

    def map_tables_to_files(
            self, site: str, logger:str, raise_if_no_file: bool=True,
            paths_as_str: bool=False
            ) -> dict:
        """
        Tie table names to local file locations.

        Args:
            site: name of site.
            logger: logger: logger name for which to provide mapping.
            raise_if_no_file: raise exception if the file does not exist. Defaults to True.
            paths_as_str: output paths as strings (instead of pathlib). Defaults to False.

        Raises:
            FileNotFoundError: DESCRIPTION.

        Returns:
            Dictionary mapping table (key) to absolute file path (value).

        """

        details = self.get_site_logger_details(site=site, logger=logger)
        dir_path = paths.get_local_data_path(
            site=site, data_stream='flux_slow'
            )
        rslt = {
            table: dir_path / f'{site}_{logger}_{table}.dat'
            for table in details['tables'].split(',')
            }
        if raise_if_no_file:
            for key, val in rslt.items():
                if not val.exists():
                    raise FileNotFoundError(
                        f'No file named {val} exists for table {key}!'
                        )
        if paths_as_str:
            return {key: str(value) for key, value in rslt.items()}
        return rslt

    #--------------------------------------------------------------------------
    def build_configs_dict(self, site: str) -> dict:
        """
        Build configuration dictionary.

        Args:
            site: name of site.

        Returns:
            The dictionary.

        """

        return {
            'modem': self.get_site_modem_details(site=site).to_dict(),
            'loggers': {
                logger:
                    self.get_site_logger_details(
                        site=site, logger=logger
                        ).to_dict() |
                    {'table_map': self.map_tables_to_files(
                        site=site, logger=logger, paths_as_str=True
                        )
                        }
                for logger in self.get_site_logger_list(site=site)
                }
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def dump_config_to_file(self, site: str, write_fmt='yaml'):
        """


        Args:
            site (str): DESCRIPTION.
            out_fmt (TYPE, optional): DESCRIPTION. Defaults to 'yml'.

        Raises:
            NotImplementedError: DESCRIPTION.

        Returns:
            None.

        """

        fmt_dict = {'yaml': 'yml', 'json': 'json'}
        out_file = (
            paths.get_local_resource_path(resource='L1_config_files') /
            f'{site}_configs.{fmt_dict[write_fmt]}'
            )
        rslt = self.build_configs_dict(site=site)
        with open(file=out_file, mode='w', encoding='utf-8') as f:
            if write_fmt == 'yaml':
                yaml.dump(data=rslt, stream=f, sort_keys=False)
            elif write_fmt == 'json':
                json.dump(rslt, f, indent=4)
            else:
                raise NotImplementedError('Unrecognised format!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def table_to_file_map(
            self, site: str, logger:str, raise_if_no_file: bool=True,
            paths_as_str: bool=False
            ) -> dict:
        """
        Tie table names to local file locations.

        Args:
            site: name of site.
            logger: logger: logger name for which to provide mapping.
            raise_if_no_file: raise exception if the file does not exist. Defaults to True.
            paths_as_str: output paths as strings (instead of pathlib). Defaults to False.

        Raises:
            FileNotFoundError: DESCRIPTION.

        Returns:
            Dictionary mapping table (key) to absolute file path (value).

        """



        details = self.get_site_logger_details(site=site, logger=logger)
        dir_path = paths.get_local_data_path(
            site=site, data_stream='flux_slow'
            )
        rslt = {
            table: dir_path / f'{site}_{logger}_{table}.dat'
            for table in details['tables'].split(',')
            }
        if raise_if_no_file:
            for key, val in rslt.items():
                if not val.exists():
                    raise FileNotFoundError(
                        f'No file named {val} exists for table {key}!'
                        )
        if paths_as_str:
            return {key: str(value) for key, value in rslt.items()}
        return rslt
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _read_excel_fields(sheet_name):

    return (
        io.read_excel(
            file=gp().local_resources.xl_connections_manager,
            sheet_name=sheet_name
            )
        .set_index(keys='Site')
        .sort_index()
        )
#------------------------------------------------------------------------------

###############################################################################
### END MULTI-SITE CONFIGURATION GENERATOR SECTION ###
###############################################################################



###############################################################################
### BEGIN SINGLE SITE CONFIGURATION READER SECTION ###
###############################################################################

#------------------------------------------------------------------------------
class SiteConfigsReader():
    """Class to read and interrogate data from site-specific config file"""

    #--------------------------------------------------------------------------
    def __init__(self, site: str, read_fmt: str='yaml'):
        """
        Do inits - read the json file.

        Args:
            site: name of site.

        Returns:
            None.

        """

        fmt_dict = {'yaml': 'yml', 'json': 'json'}
        expected_path = (
            paths.get_local_resource_path(resource='L1_config_files') /
            f'{site}_configs.{fmt_dict[read_fmt]}'
            )
        with open(file=expected_path) as f:
            if read_fmt == 'yaml':
                rslt = yaml.safe_load(f)
            elif read_fmt == 'json':
                rslt = json.load(f)
            else:
                raise NotImplementedError('Unrecognised format!')
        self.configs = rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_logger_list(self) -> list:
        """
        Get list of loggers.

        Args:
            site: name of site.

        Returns:
            List of loggers.

        """

        return list(self.configs['loggers'].keys())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_logger_details(
            self, logger: str=None, field: str=None
            ) -> pd.DataFrame | pd.Series | str:
        """
        Get details of loggers.

        Args:
            logger: logger name for which to return details. Defaults to None.
            field: field to return. Defaults to None.

        Returns:
            Logger details. If optional kwargs are not specified, returns a
            string. If logger is specified, return a series.
            If field is specified, return a str.

        """

        sub_df = (
            pd.DataFrame(self.configs['loggers'])
            .drop('table_map')
            .T
            )
        if not logger is None:
            sub_df = sub_df.loc[logger]
        if not field is None:
            sub_df = sub_df[[field]]
        if not field:
            return sub_df
        return sub_df[field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_modem_details(self, field: str=None) -> pd.Series | str:
        """
        Get details of modem.

        Args:
            field: field to return. Defaults to None.

        Returns:
            Details of modem.

        """

        modem_fields = pd.Series(self.configs['modem'])
        if field is None:
            return modem_fields
        return modem_fields[field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_table_to_file_map(
            self, logger:str, raise_if_no_file: bool=True,
            paths_as_str: bool=False
            ) -> dict:
        """
        Tie table names to local file locations.

        Args:
            logger: logger name for which to provide mapping.
            raise_if_no_file: raise exception if the file does not exist. Defaults to True.
            paths_as_str: output paths as strings (instead of pathlib). Defaults to False.

        Raises:
            FileNotFoundError: DESCRIPTION.

        Returns:
            Dictionary mapping table (key) to absolute file path (value).

        """

        rslt = self.configs['loggers'][logger]['table_map']
        if raise_if_no_file or not paths_as_str:
            fmt_rslt = {
                key: pathlib.Path(value) for key, value in rslt.items()
                }
        if raise_if_no_file:
            for key, value in fmt_rslt.items():
                if not value.exists():
                    raise FileNotFoundError(
                        f'No file named {value} exists for table {key}!'
                        )
        if paths_as_str:
            return pd.Series(rslt)
        return pd.Series(fmt_rslt)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

class LoggerDataManager():

    def __init__(self, site):

        conns = ConfigsManager()
        self.logger_details = conns.get_site_logger_details(site=site)
        self.lookup_table = self._build_expanded_lookup_table(
            )

    def _build_expanded_lookup_table(self):

        df_list = []
        for logger in self.logger_details.index:
            df = lf.build_lookup_table(
                IP_addr=self.logger_details.loc[logger, 'IP_addr']
                )
            df['logger'] = logger
            df_list.append(df)
        return (
            pd.concat(df_list)
            [['units', 'process', 'table', 'logger']]
            .fillna('')
            )

    def get_variable_attributes(self, variable, field=None):

        if not field:
            return self.lookup_table.loc[variable]
        return self.lookup_table.loc[variable, field]

    def get_table_variables(self, table, list_only=False):

        sub_df = (
            self.lookup_table.loc[self.lookup_table.table==table]
            .drop('table', axis=1)
            )
        if list_only:
            return sub_df.index.tolist()
        return sub_df
