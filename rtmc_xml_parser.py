#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 11:23:06 2022

@author: imchugh
"""

import pdb
import pathlib
import xml.etree.ElementTree as ET

#------------------------------------------------------------------------------
### CONSTANTS
#------------------------------------------------------------------------------
COMPONENT_DICT = {'Image': '10702',
                  'Digital': '10101',
                  'TimeSeriesChart': '10602',
                  'Time': '10108',
                  'BasicStatusBar': '10002',
                  'MultiStateAlarm': '10207',
                  'CommStatusAlarm': '10205',
                  'MultiStateImage': '10712',
                  'NoDataAlarm': '10204'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class digital_editor():

    """Get and set main calculation elements"""

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_element_calculation_text(self):

        return self.elem.find('calculation').text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def set_element_calculation_text(self, text):

        calculation_element = self.elem.find('calculation')
        calculation_element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class BasicStatusBar_editor(digital_editor):

    """Get and set main and pointer calculation elements"""

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_pointer_calculation_text(self, pointer):

        d = {'max': 'max_pointer', 'min': 'min_pointer'}

        return self.elem.find('./{}/calculation'.format(d[pointer])).text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def set_pointer_calculation_text(self, pointer, text):

        d = {'max': 'max_pointer', 'min': 'min_pointer'}

        calculation_element = (
            self.elem.find('./{}/calculation'.format(d[pointer]))
            )
        calculation_element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class time_editor(digital_editor):

    """Get and set main calculation elements"""

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_element_offset_text(self, text=None):

        element = self.elem.find('time_offset_with_units')
        if not text:
            return element.text
        element.text = text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_element_offset_units_text(self, text=None):

        element = self.elem.find('time_offset_units')
        if not text:
            return element.text
        element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
class time_series_editor():

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_trace_elements(self):

        return self.elem.findall('Traces/traces')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_trace_labels(self):

        return [x.attrib['label'] for x in self.get_trace_elements()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_trace_element_by_label(self, label):

        elems = self.get_trace_elements()
        for x in elems:
            try:
                if x.attrib['label'] == label:
                    return x
            except KeyError:
                next
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_trace_calculation_by_label(self, label):

        elem = self.get_trace_element_by_label(label=label)
        return elem.find('calculation').text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def set_trace_calculation_by_label(self, label, variable):

        root_elem = self.get_trace_element_by_label(label=label)
        calculation_elem = root_elem.find('calculation')
        calculation_elem.text = variable
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class rtmc_parser():

    def __init__(self, path):

        self.path = path
        self.tree = ET.parse(path)
        self.root = self.tree.getroot()
        self.parent_map = {c: p for p in self.tree.iter() for c in p}
        self.state_change = False

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_screen_element(self, screen=None):

        if not screen:
            return self.root.findall('./Screens/screen')
        return (
            self.root.find('./Screens/screen[@screen_name="{}"]'
                           .format(screen))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_element_by_type(self, screen, component_type=None):

        if component_type:
            component_idx = COMPONENT_DICT[component_type]
        screen_element = self.get_screen_element(screen=screen)
        if not component_type:
            return screen_element.findall('./Components/component')
        return (
            screen_element.findall('./Components/component[@type="{}"]'
                                   .format(component_idx))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_element_by_name(self, screen, component_name=None):

        screen_element = self.get_screen_element(screen=screen)
        if not component_name:
            return screen_element.findall('./Components/component')
        return (
            screen_element.find('./Components/component[@name="{}"]'
                                 .format(component_name))
            )
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def Digital_editor(self, digital_element):

    #     return digital_editor(elem=digital_element)
    # #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def TimeSeriesChart_editor(self, ts_element):

    #     return time_series_editor(elem=ts_element)
    # #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(self, write_to_self=False):

        input_file_path = pathlib.Path(self.path)
        if write_to_self:
            self.tree.write(input_file_path)
            return
        file_ext = input_file_path.suffix
        input_file_name = input_file_path.name.replace(file_ext, '')
        output_file_path = (
            input_file_path.parent / (input_file_name + '_new' + file_ext)
            )
        self.tree.write(output_file_path)
    #--------------------------------------------------------------------------