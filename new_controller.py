# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 14:13:29 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import sys
import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import generic_variable_mapper as gvm
import rtmc_xml_parser_new as rxp
import paths_manager as pm

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

# Set the site name
site = 'Calperum'

# Get the paths module
PATHS = pm.paths()

# Set the palettes for plots
RGB_DICT = {
    'Soil heat flux at depth z': [
        [189, 230, 241], [238, 176, 176], [189, 230, 241], [255, 229, 157]
        ],
    'Soil temperature': [
        [214, 28, 78], [247, 126, 33], [250, 194, 19], [254, 249, 167]
        ]
    }

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def line_plot_parser(long_name, screen_name, component_name):
    
    df = mapper.site_df.loc[long_name]
    plot_editor = parser.get_editor_by_component_name(
        screen=screen_name, component_name=component_name, 
        )
    old_labels = plot_editor.get_trace_labels()
    new_labels = df.translation_name.tolist()
    for i, new_label in enumerate(new_labels):
        calculation_text = '"DataFile.merged.{}"'.format(new_label)
        try:
            old_label = old_labels[i]
        except IndexError:
            pass
        plot_editor.get_set_trace_calculation_by_label(
            label=old_label, calculation_text=calculation_text, 
            label_text=new_labels[i]
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN CODE ###
#------------------------------------------------------------------------------

# Create the parser object
mapper = gvm.mapper(site=site)
parser = rxp.rtmc_parser(PATHS.RTMC_template(check_exists=True))

# Change the data path for the logger data
logger_data_source = str(PATHS.RTMC_data_file(site=site, check_exists=True))
data_source_editor = parser.get_file_source_editor(source_type='data')
data_source_editor.get_set_source_file(path=logger_data_source)

# Change the data path for the details file
details_data_source = str(PATHS.RTMC_details_file(site=site, check_exists=True))
details_source_editor = parser.get_file_source_editor(source_type='details')
details_source_editor.get_set_source_file(path=details_data_source)

# Reconfigure the soil heat flux plot
line_plot_parser(long_name='Soil heat flux at depth z', screen_name='Soil', 
                 component_name='Time Series Chart')

# Reconfigure the soil temperature plot





