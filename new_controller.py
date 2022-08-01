# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 14:13:29 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

import sys

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

import generic_variable_mapper as gvm
import rtmc_xml_parser_new as rxp
import paths_manager as pm

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

# Set the site name and RTMC template file paths
site = 'Calperum'
RTMC_XML_PATH = 'E:\\Campbellsci\RTMC\Gingin_overhaul.rtmc2'

#------------------------------------------------------------------------------
### MAIN CODE ###
#------------------------------------------------------------------------------

translator = gvm.file_parser(site=site)
parser = rxp.rtmc_parser(path=RTMC_XML_PATH)


# Change the data path for the main data and site details files
file_source_editor = parser.get_file_source_editor(source_type='data')
# new_file_source_path = x
file_source_editor.get_set_source_file()