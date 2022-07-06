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
PATH_TO_XML = '/home/unimelb.edu.au/imchugh/Documents/Calperum_new.rtmc2'
PATH_TO_IMAGES = (
    'E:/Cloudstor/Network_documents/RTMC_files/Static_images/Site_images'
    )
PATH_TO_SITE_DETAILS = (
    'E:/Cloudstor/Network_documents/RTMC_files/Static_data'
    )
IMAGES_DICT = {'Contour_Image': '{}_contour.png',
               'SitePhoto_Image': '{}_tower.jpg',
               'MSLP_Image': 'mslp.png', 'Sat_Image': 'sat_img.jpg'}
# TIME_OFFSET_UNITS = {'min': '1', 'hr': '2'}
# SERVER_UTC_OFFSET = 10
SCREENS_TO_PARSE = ['System', 'Meteorology', 'Turbulent_flux', 'Radiant_flux']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def function_getter(element):

    function_dict = {'10002': BasicStatusBar_function,
                     '10101': Digital_function,
                     '10207': MultiStateAlarm_function,
                     '10712': MultiStateImage_function,
                     '10205': CommStatusAlarm_function,
                     '10204': NoDataAlarm_function,
                     '10108': Time_function,
                     '10602': TimeSeriesChart_function,
                     '10606': WindRose_function,
                     '10503': RotaryGauge_function,
                     '10702': Image_function}
    type_id = element.attrib['type']
    function_dict[type_id](element=element)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def base_function(element):

    component_name = element.attrib['name']
    details = rtmc_mapper.get_component_attr(component_name=component_name)
    editor = parser.get_editor(element=element)
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
    if not rtmc_mapper.use_custom_names:
        image_name = rtmc_mapper.translate_component_name(image_name)
    file_str = IMAGES_DICT[image_name].format(site)
    full_path = pathlib.Path(PATH_TO_IMAGES) / file_str
    windows_path_str = str(full_path).replace('/', '\\')
    editor = parser.get_editor(element=element)
    editor.get_set_element_ImgName(text=windows_path_str)
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
def SetFileSource(site):

    new_file_path = str(
        pathlib.Path(PATH_TO_SITE_DETAILS) /
        '{}_details.dat'.format(site)
        )
    editor = parser.get_file_source_editor()
    editor.get_set_source_file(path=new_file_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def Time_function(element):

    details, editor = base_function(element=element)
    component_name = element.attrib['name']
    ec_table = (
        var_mapper.get_variable(long_name='Logger collect').rtmc_output
        )
    custom_name = (
        rtmc_mapper.translate_component_name(component_name=component_name)
        )
    editor.get_set_element_calculation_text(text=ec_table)
    if custom_name == 'UTC_Time':
        utc_offset = (
            var_mapper.get_variable(long_name='UTC offset').default_value
            )
        editor.get_set_element_offset_text(text=str(round(utc_offset * 60)))
        editor.get_set_element_offset_units_text('1')
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
site = 'Boyagin' #sys.argv[1]
custom_names = False
parser = rxp.rtmc_parser(path=PATH_TO_XML)
var_mapper = vm.variable_mapper(path=PATH_TO_XL, site=site)

# Edit project settings - site_details file source
SetFileSource(site=site)

# Iterate over the screen components
for screen in SCREENS_TO_PARSE[:3]:
    print ('Editing components for Screen "{}":'.format(screen))

    # Create the rtmc component mapper
    rtmc_mapper = vm.rtmc_component_mapper(
        path=PATH_TO_XL, screen=screen, use_custom_names=custom_names
        )

    # Iterate over components for each screen
    for name in rtmc_mapper.get_component_names():
        print ('    - Parsing component {}'.format(name))

        # Get the individual component
        element = parser.get_component_element_by_name(
            screen=screen, component_name=name
            )

        function_getter(element=element)
        # # Run the function
        # try:
        #     function_getter(element=element)
        # except KeyError:
        #     pdb.set_trace()
        #     print (
        #         '        *** No algorithm for component type {} *** '
        #         .format(element.attrib['type'])
        #             )