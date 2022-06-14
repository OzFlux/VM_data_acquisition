#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:01:38 2022

@author: imchugh

To fix:
    1) rotarygause function has no way to pass arguments; this is okay at the
    moment because the rotary gaue is only used for pressure, and everybody
    outputs pressure in kPa, but it isn't flexible'
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import pdb
import numpy as np
import pathlib
import sys

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import variable_mapper as vm
import rtmc_xml_parser as rxp
import site_utils as su

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------
PATH_TO_XL = '/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'
PATH_TO_XML = '/home/unimelb.edu.au/imchugh/Documents/Calperum.rtmc2'
PATH_TO_IMAGES = (
    'E:/Cloudstor/Network_documents/RTMC_files/Static_images/Site_images'
    )
IMAGES_DICT = {'Contour_Image': '{}_contour.png',
               'SitePhoto_Image': '{}_tower.jpg',
               'MSLP_Image': 'mslp.png', 'Sat_Image': 'sat_img.jpg'}
TIME_OFFSET_UNITS = {'min': '1', 'hr': '2'}
SERVER_UTC_OFFSET = 10
SCREENS_TO_PARSE = ['System', 'Meteorology', 'Turbulent_flux', 'Radiant_flux']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def function_getter(element):

    function_dict = {'BasicStatusBar': BasicStatusBar_function,
                     'Digital': Digital_function,
                     'MultiStateAlarm': MultiStateAlarm_function,
                     'MultiStateImage': MultiStateImage_function,
                     'CommStatusAlarm': CommStatusAlarm_function,
                     'NoDataAlarm': NoDataAlarm_function,
                     'Time': Time_function,
                     'TimeSeriesChart': TimeSeriesChart_function,
                     'WindRose': WindRose_function,
                     'RotaryGauge': RotaryGauge_function,
                     'Image': Image_function}

    component_name = element.attrib['name']
    component_type = component_name.split('_')[-1]
    print ('    - Parsing component {}'.format(component_name))
    try:
        function_dict[component_type](element)
    except KeyError:
        print (
            '        *** No algorithm for component type {} *** '
            .format(component_type)
               )
        return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def base_function(element):

    component_name = element.attrib['name']
    component_type = component_name.split('_')[-1]
    details = rtmc_mapper.get_component_attr(component_name=component_name)
    editor = parser.get_editor(editor_type=component_type, element=element)
    return details, editor
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def BasicStatusBar_function(element):

    details, editor = base_function(element=element)
    constructor = var_mapper.get_variable(long_name=details.long_name)
    if details.compound_operation == 'pointers':
        min_text = constructor.get_statistic(statistic='min_limited',
                                             interval='daily')
        editor.get_set_pointer_calculation_text(pointer='min', text=min_text)
        max_text = constructor.get_statistic(statistic='max_limited',
                                             interval='daily')
        editor.get_set_pointer_calculation_text(pointer='max', text=max_text)
    if details.operation == 'pct_min_max':
        main_text = constructor.get_pct_min_max()
    else:
        main_text = constructor.rtmc_output
    editor.get_set_element_calculation_text(text=main_text)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def CommStatusAlarm_function(element):

    return MultiStateAlarm_function(element)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def Digital_function(element):

    details, editor = base_function(element=element)
    long_name_list = details.long_name.split(',')
    if details.operation == 'variable_difference':
        constructor = var_mapper.get_two_variable_operation(
            long_name_first=long_name_list[0],
            long_name_second=long_name_list[1]
            )
        new_text = constructor.rtmc_output
    else:
        constructor = var_mapper.get_variable(long_name=long_name_list[0])
        if details.operation:
            new_text = constructor.get_statistic(
                statistic=details.operation, interval=details.interval
                )
        else:
            new_text = constructor.rtmc_output
    editor.get_set_element_calculation_text(text=new_text)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def Image_function(element):

    image_name = element.attrib['name']
    file_str = IMAGES_DICT[image_name].format(site)
    full_path = pathlib.Path(PATH_TO_IMAGES) / file_str
    text = str(full_path)
    editor = parser.get_editor(editor_type='Image', element=element)
    editor.get_set_element_ImgName(text=text)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def MultiStateAlarm_function(element):

    details, editor = base_function(element=element)
    constructor = (
        var_mapper.get_variable(long_name=details.long_name)
        )
    editor.get_set_element_calculation_text(constructor.rtmc_output)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def MultiStateImage_function(element):

    details, editor = base_function(element=element)
    constructor = var_mapper.get_variable(long_name=details.long_name)
    if details.operation:
        text = constructor.get_statistic(
            statistic=details.operation, interval=details.interval
            )
    else:
        text = constructor.rtmc_output
    editor.get_set_element_calculation_text(text=text)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def NoDataAlarm_function(element):

    return MultiStateAlarm_function(element)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def RotaryGauge_function(element):

    details, editor = base_function(element=element)
    constructor = (
        var_mapper.get_variable(long_name=details.long_name)
        )
    text = constructor.rtmc_output + '*10'
    editor.get_set_element_calculation_text(text=text)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def Time_function(element):

    details, editor = base_function(element=element)
    component_name = element.attrib['name']
    if not component_name == 'UTC_Time':
        return
    constructor = var_mapper.get_variable(long_name='UTC offset')
    time_offset = constructor.default_value
    utc_offset = str(int(-(SERVER_UTC_OFFSET * 60 - time_offset * 60)))
    time_units = TIME_OFFSET_UNITS['min']
    editor.get_set_element_offset_text(text=str(utc_offset))
    editor.get_set_element_offset_units_text(time_units)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def TimeSeriesChart_function(element):

    label_map = (
        rtmc_mapper.get_label_mapping(chart_name=element.attrib['name'])
        )
    details, editor = base_function(element=element)
    for trace_label in editor.get_trace_labels():
        try:
            long_name = label_map[trace_label]
        except KeyError as e:
            if trace_label == 'Const': continue
            pdb.set_trace()
            raise Exception('No long name entry for trace label {}'
                            .format(trace_label)) from e
        constructor = (
            var_mapper.get_variable(long_name=long_name)
            )
        new_text = constructor.rtmc_output
        editor.get_set_trace_calculation_by_label(
            label=trace_label, text=new_text
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def WindRose_function(element):

    details, editor = base_function(element=element)
    spd_constructor = var_mapper.get_variable(long_name='Wind speed')
    dir_constructor = var_mapper.get_variable(long_name='Wind direction')
    spd_list = spd_constructor.rtmc_output.split('.')
    spd_table = '.'.join(spd_list[:-1]) + '"'
    spd_var = '"' + spd_list[-1]
    dir_list = dir_constructor.rtmc_output.split('.')
    dir_table = '.'.join(dir_list[:-1]) + '"'
    dir_var = '"' + dir_list[-1]
    if not spd_table == dir_table:
        raise RuntimeError(
            'Wind speed and wind direction must be in same logger table'
            )
    editor.get_set_element_calculation_text(text=spd_table)
    editor.get_set_wind_spd_column(text=spd_var)
    editor.get_set_wind_dir_column(text=dir_var)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

# Create the RTMC XML parser and site variable mapper
site = 'Calperum' #sys.argv[1]
parser = rxp.rtmc_parser(path=PATH_TO_XML)
var_mapper = vm.variable_mapper(path=PATH_TO_XL, site=site)

# Iterate over the screen components
for screen in SCREENS_TO_PARSE[:3]:
    print ('Editing components for Screen "{}":'.format(screen))
    # Create the rtmc component mapper
    rtmc_mapper = vm.rtmc_component_mapper(path=PATH_TO_XL, screen=screen)
    # Iterate over components for each screen
    for custom_name in rtmc_mapper.get_component_names():
        try:
            element = parser.get_component_element_by_name(
                screen=screen, component_name=custom_name
                )
        except KeyError:
            default_name = rtmc_mapper.get_default_name(custom_name)
            element = parser.get_component_element_by_name(
                screen=screen, component_name=default_name
            )
            parser.rename_component_element(element=element, new_name=custom_name)
        function_getter(element=element)