# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 10:38:32 2024

@author: jcutern-imchugh
"""

import geojson




import data_mapper as dm
from data_parser import SiteDataParser
from paths_manager import GenericPaths
import sparql_site_details as sd


VARIABLE_LIST = ['Fco2', 'Fh', 'Fe', 'Fsd']


def make_status_geojson():

    reference_data = sd.make_df()
    return geojson.FeatureCollection(
        [
            geojson.Feature(
                id=site,
                geometry=geojson.Point(coordinates=[
                    reference_data.loc[site, 'latitude'],
                    reference_data.loc[site, 'longitude']
                    ]),
                properties=get_station_status(site)
                )
            for site in dm.get_mapped_site_list()
            ]
        )

def write_status_geojson():

    output_path = (
        GenericPaths().local_resources.network_status / 'network_status.json'
        )
    with open(file=output_path, mode='w', encoding='utf-8') as f:
        geojson.dump(
            make_status_geojson(),
            f,
            indent=4
            )

def get_station_status(site):

    parser = SiteDataParser(site=site, concat_files=False)
    return (
        parser.get_record_stats_by_variable(variable_list=VARIABLE_LIST)
        ['days_since_last_valid_record']
        .to_dict()
        )
