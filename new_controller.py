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
import pandas as pd
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
site = 'Litchfield'

# Get the paths module
PATHS = pm.paths()

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------   
def colour_getter(long_name):
    """
    Generator to create a set of formatted strings to specify successive 
    colours for a line plot

    Parameters
    ----------
    long_name : str
        The long name of the variable as it appears in RGB_DICT.

    Yields
    ------
    colour_string : str
        RTMC format: "RGBA(r,g,b,a)" where r, g, b are 0-255 integers and a is
        alpha (transparency) parameter (just set to 1).

    """
    
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
def get_comm_status_string(logger_name):
    
    return (
        '"Server:__statistics__.{}_std.Collection State" > 2 '
        .format(logger_name)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_no_data_status_string(logger_name, table_name):
    
    return '"Server:{0}.{1}"'.format(logger_name, table_name)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def line_plot_parser(long_name, screen_name, component_name):
    
    try:
        df = mapper.site_df.loc[long_name]
        if isinstance(df, pd.core.series.Series):
            df = df.to_frame().T
    except KeyError:
        'No data for this variable!'
        return
    plot_editor = parser.get_editor_by_component_name(
        screen=screen_name, component_name=component_name, 
        )
    old_labels = [
        x for x in plot_editor.get_trace_labels() 
        if plot_editor.get_axis_by_label(x)=='left'
        ]
    new_labels = df.translation_name.tolist()      
    if len(old_labels) > len(new_labels):
        drop_labels = old_labels[len(new_labels):]
        for this_label in drop_labels:
            plot_editor.drop_trace_element_by_label(label=this_label)
            old_labels.remove(this_label)
    palette = colour_getter(long_name=long_name)
    for i, new_label in enumerate(new_labels):
        colour = next(palette)
        calculation_str = '"DataFile:merged.{}"'.format(new_label)
        try:
            elem_label = old_labels[i]
            plot_editor.set_trace_attributes_by_label(
                label=elem_label, calculation=calculation_str, rgb=colour,
                new_label=new_label, title=new_label)
        except IndexError:
            elem_label = new_labels[0]
            plot_editor.duplicate_trace_element_by_label(
                old_label=elem_label, new_label=new_label
                )
            plot_editor.set_trace_attributes_by_label(
                label=new_label, calculation=calculation_str, rgb=colour,
                title=new_label)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN CODE ###
#------------------------------------------------------------------------------

# Create the mapper and parser objects
mapper = gvm.mapper(site=site)
parser = rxp.rtmc_parser(PATHS.RTMC_template(check_exists=True))
site = sys.argv[1]

#------------------------------------------------------------------------------
# Background configs
#------------------------------------------------------------------------------

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

#------------------------------------------------------------------------------
# System screen configs
#------------------------------------------------------------------------------

# Change the the comm status alarm component calculation string
logger_name = mapper.get_logger_list(long_name='CO2 flux')
if not isinstance(logger_name, float):
    calculation_str = get_comm_status_string(logger_name=logger_name)
    comm_status_editor = parser.get_editor_by_component_name(
        screen='System', component_name='Comm Status Alarm'
        )
    comm_status_editor.get_set_element_calculation_text(
        text=calculation_str)
    
# Change the no data alarm component calculation string
logger_name = mapper.get_logger_list(long_name='CO2 flux')
table_name = mapper.get_table_list(long_name='CO2 flux')
if not isinstance(logger_name, float):
    calculation_str = get_no_data_status_string(
        logger_name=logger_name, table_name=table_name
        )
    no_data_editor = parser.get_editor_by_component_name(
        screen='System', component_name='No Data Alarm'
        )
    no_data_editor.get_set_element_calculation_text(
        text=calculation_str)

# Reconfigure the figure sources (contour and site photo)
contour_path = str(PATHS.RTMC_site_images(img_type='contour', site=site))
img_editor = parser.get_editor_by_component_name(
    screen='System', component_name='Image1'
    )
img_editor.get_set_element_ImgName(text=contour_path)
try:
    tower_path = str(PATHS.RTMC_site_images(
        img_type='tower', site=site, check_exists=True
        ))
    img_editor = parser.get_editor_by_component_name(
        screen='System', component_name='Image2'
        )
    img_editor.get_set_element_ImgName(text=tower_path)
except FileNotFoundError:
    print('No tower image found for {}'.format(site))
    pass

#------------------------------------------------------------------------------
# Turbulent_flux screen configs
#------------------------------------------------------------------------------

# Reconfigure the mean soil temperature and basic status bar
digital_temp_str = (
    mapper.rtmc_syntax_generator.get_aliased_output(
        long_name='Soil temperature'
        )
    )
soil_T_digital_editor = parser.get_editor_by_component_name(
    screen='Turbulent_flux', component_name='Digital2'
    )
soil_T_digital_editor.get_set_element_calculation_text(text=digital_temp_str)
StatusBar_temp_str = (
    mapper.rtmc_syntax_generator.get_aliased_output(
        long_name='Soil temperature', scaled_to_range=True, 
        start_cond='start_absolute'
        )
    )
soil_T_StatusBar_editor = parser.get_editor_by_component_name(
    screen='Turbulent_flux', component_name='Basic Status Bar'
    )
soil_T_StatusBar_editor.get_set_pointer_calculation_text(
    text=StatusBar_temp_str
    )

# Reconfigure the mean soil moisture and basic status bar
digital_moist_str = (
    mapper.rtmc_syntax_generator.get_aliased_output(
        long_name='Soil water content'
        )
    )
soil_moist_digital_editor = parser.get_editor_by_component_name(
    screen='Turbulent_flux', component_name='Digital4'
    )
soil_moist_digital_editor.get_set_element_calculation_text(
    text=digital_moist_str
    )
soil_moist_StatusBar_editor = parser.get_editor_by_component_name(
    screen='Turbulent_flux', component_name='Basic Status Bar4'
    )
StatusBar_moist_str = (
    mapper.rtmc_syntax_generator.get_aliased_output(
        long_name='Soil water content', scaled_to_range=True, 
        start_cond='start_absolute'
        )
    )
soil_moist_StatusBar_editor.get_set_pointer_calculation_text(
    text=StatusBar_moist_str
    )


#------------------------------------------------------------------------------
# Soil screen configs
#------------------------------------------------------------------------------

# Reconfigure the soil heat flux plot
line_plot_parser(long_name='Soil heat flux at depth z', screen_name='Soil', 
                 component_name='Time Series Chart')

# Reconfigure the soil temperature plot
line_plot_parser(long_name='Soil temperature', screen_name='Soil', 
                 component_name='Time Series Chart1')

# Reconfigure the soil moisture plot
# pdb.set_trace()
line_plot_parser(long_name='Soil water content', screen_name='Soil', 
                 component_name='Time Series Chart2')

# Reconfigure the soil heat flux and storage average plot
# First heat storage
soil_plot_editor = parser.get_editor_by_component_name(
    screen='Soil', component_name='Time Series Chart3'
    )
heat_storage_string = (
    mapper.rtmc_syntax_generator.get_soil_heat_storage()
    )
soil_plot_editor.set_trace_attributes_by_label(
    label='Gs_mean', calculation=heat_storage_string
    )

# Then heat flux plates
uncorr_heat_flux_string = (
    mapper.rtmc_syntax_generator.get_aliased_output(
        long_name='Soil heat flux at depth z')
    )
soil_plot_editor.set_trace_attributes_by_label(
    label='Gz_mean', calculation=uncorr_heat_flux_string
    )

# Then combination method
corr_heat_flux_string = (
    mapper.rtmc_syntax_generator.get_corrected_soil_heat_flux()
    )
soil_plot_editor.set_trace_attributes_by_label(
    label='G_mean', calculation=corr_heat_flux_string
    )

# Write to a new file
new_file_name = str(
    PATHS.RTMC_template().parent / '{}_std.rtmc2'.format(site)
    )
parser.write_to_file(file_name=new_file_name)