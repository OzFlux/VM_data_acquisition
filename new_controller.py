# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 14:13:29 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

from matplotlib.pyplot import cm
import numpy as np
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
site = 'Gingin'

# Get the paths module
PATHS = pm.paths()

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------   
def colour_getter(long_name):
    
    RGB_DICT = {
        'Soil heat flux at depth z': cm.get_cmap(name='Set1'),
        'Soil temperature': cm.get_cmap(name='Set1_r'),
        'Soil water content': cm.get_cmap(name='Set1')
        }
    output_str = 'RGBA({},1)'
    colours = [RGB_DICT[long_name](i) for i in range(9)]
    for colour in colours:
        colour_string = (
            output_str.format(
                ','.join([str(int(x)) for x in np.array(colour[:-1]) * 255])
                )
            )
        yield colour_string
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def line_plot_parser(long_name, screen_name, component_name):
    
    try:
        df = mapper.site_df.loc[long_name]
    except KeyError:
        'No data for this variable!'
        return
    plot_editor = parser.get_editor_by_component_name(
        screen=screen_name, component_name=component_name, 
        )
    old_labels = plot_editor.get_trace_labels()
    new_labels = df.translation_name.tolist()
    if len(old_labels) > len(new_labels):
        drop_labels = old_labels[len(new_labels):]
        for this_label in drop_labels:
            plot_editor.drop_trace_element_by_label(label=this_label)
            old_labels.remove(this_label)
    palette = colour_getter(long_name=long_name)
    for i, new_label in enumerate(new_labels):
        colour = next(palette)
        calculation_str = '"DataFile.merged.{}"'.format(new_label)
        try:
            old_label = old_labels[i]
            plot_editor.set_trace_attributes_by_label(
                label=old_label, calculation=calculation_str, rgb=colour,
                new_label=new_label
                )
        except IndexError:
            plot_editor.create_trace_element_by_label(new_label=new_label)
            plot_editor.set_trace_attributes_by_label(
                label=new_label, calculation=calculation_str, rgb=colour)
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

# Change the data path for the details data
details_data_source = str(PATHS.RTMC_details_file(site=site, check_exists=True))
details_source_editor = parser.get_file_source_editor(source_type='details')
details_source_editor.get_set_source_file(path=details_data_source)

# Change the data path for the snapshot output
snapshot_destination = str(PATHS.RTMC_snapshot_directory(site=site, check_exists=True))
settings_editor = parser.get_basic_settings_editor()
settings_editor.get_set_snapshot_destination(text=snapshot_destination)

# Reconfigure the soil heat flux plot
try:
    line_plot_parser(long_name='Soil heat flux at depth z', screen_name='Soil', 
                     component_name='Time Series Chart')
except KeyError:
    pass

# Reconfigure the soil temperature plot
try:
    line_plot_parser(long_name='Soil temperature', screen_name='Soil', 
                     component_name='Time Series Chart1')
except KeyError:
    pass

# Reconfigure the soil moisture plot
try:
    line_plot_parser(long_name='Soil water content', screen_name='Soil', 
                     component_name='Time Series Chart2')
except KeyError:
    pass

# Reconfigure the soil heat flux storage plot
