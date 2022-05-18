#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:01:38 2022

@author: imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import pdb

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import variable_mapper as vm
import rtmc_xml_parser as rxp

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------
PATH_TO_XL = '/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'
PATH_TO_XML = '/home/unimelb.edu.au/imchugh/Documents/Boyagin_copy.rtmc2'
TIME_OFFSET_UNITS = {'seconds': '0', 'minutes': '1', 'hours': '2', 'days': '3'}
TIME_OFFSET = '-30 minutes'
site = 'Calperum'
screen = 'System'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

# Create the RTMC XML parser and site variable mapper
parser = rxp.rtmc_parser(path=PATH_TO_XML)
mapper = vm.variable_mapper(path=PATH_TO_XL, site=site)

# Iterate through component types:

# #------------------------------------------------------------------------------
# # Time Series Charts

# timeseries_elements = (
#     parser.get_component_element_by_type(screen=screen,
#                                           component_type='TimeSeriesChart')
#     )

# for element in timeseries_elements:
#     alias_dict = mapper.get_TimeSeriesChart_aliases(
#         screen_name=screen,
#         chart_name=element.attrib['name']
#         )
#     editor = rxp.time_series_editor(elem=element)
#     for trace_label in editor.get_trace_labels():
#         try:
#             long_name = alias_dict[trace_label]
#         except KeyError:
#             print ('No long name entry for trace label {}'.format(trace_label))
#             continue
#         print ('Long name entry for trace label {0} is {1}'
#                 .format(trace_label, long_name))
#         constructor = (
#             mapper.get_rtmc_variable(long_name=long_name)
#             )
#         new_text = constructor.get_rtmc_output()
#         editor.set_trace_calculation_by_label(label=trace_label,
#                                               variable=new_text)
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# # Digital displays

# digital_elements = (
#     parser.get_component_element_by_type(screen=screen,
#                                           component_type='Digital')
#     )

# for element in digital_elements:

#     editor = rxp.digital_editor(elem=element)
#     component_name = element.attrib['name']
#     long_name = (
#         mapper.get_Digital_LongName(screen_name=screen,
#                                     digital_name=component_name)
#         )
#     long_name_list = long_name.split(',')
#     if len(long_name_list) == 1:
#         constructor = mapper.get_rtmc_variable(long_name=long_name)
#     else:
#         constructor = (
#             vm._rtmc_constructor(
#                 mapper.get_two_variable_operation(long_name_list[0],
#                                                   long_name_list[1])
#                 )
#             )
#     if component_name == 'VBatt_BasicStatusBar': pdb.set_trace()
#     new_text = constructor.get_rtmc_output()
#     editor.set_element_calculation_text(text=new_text)
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# # Basic status bars

# StatusBar_elements = (
#     parser.get_component_element_by_type(screen=screen,
#                                          component_type='BasicStatusBar')
#     )

# for element in StatusBar_elements:

#     component_name = element.attrib['name']
#     long_name = (
#         mapper.get_Digital_LongName(screen_name=screen,
#                                     digital_name=component_name)
#         )
#     editor = rxp.BasicStatusBar_editor(elem=element)
#     constructor = mapper.get_rtmc_variable(long_name=long_name)
#     min_text = constructor.get_interval_statistic(statistic='min_limited',
#                                                   interval='daily')
#     max_text = constructor.get_interval_statistic(statistic='max_limited',
#                                                   interval='daily')
#     main_text = constructor.get_rtmc_output()
#     editor.set_element_calculation_text(text=main_text)
#     editor.set_pointer_calculation_text(pointer='min', text=min_text)
#     editor.set_pointer_calculation_text(pointer='max', text=max_text)
# #------------------------------------------------------------------------------

Time_elements = (
    parser.get_component_element_by_type(screen=screen,
                                          component_type='Time')
    )

# for element in Time_elements:

