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
                  'NoDataAlarm': '10204',
                  'WindRose': '10606',
                  'RotaryGauge': '10503'}
RTMC_IMAGE_PATH = (
    'E:\\Cloudstor\\Network_documents\\RTMC_files\\Static_images\\Site_images'
    )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Digital_editor():

    """Edit RTMC component elements"""

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_element_calculation_text(self, text=None):

        """Get and set calculation element"""

        calculation_element = self.elem.find('calculation')
        if not text:
            return calculation_element.text
        calculation_element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class BasicStatusBar_editor(Digital_editor):

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_pointer_calculation_text(self, pointer, text=None):

        """Get and set pointer calculation element"""
        d = {'max': 'max_pointer', 'min': 'min_pointer'}
        element = self.elem.find('./{}/calculation'.format(d[pointer]))
        if not text:
            return element.text
        element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Image_editor():

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    def get_set_element_ImgName(self, text=None):

        location_element = self.elem.find('image_name')
        if not text:
            return location_element.text
        location_element.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Time_editor():

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
class TimeSeriesChart_editor():

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
    def get_set_trace_calculation_by_label(self, label, text=None):

        elem = self.get_trace_element_by_label(label=label)
        calculation_elem = elem.find('calculation')
        if not text:
            return calculation_elem.text
        calculation_elem.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class WindRose_editor(Digital_editor):

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    def get_set_wind_dir_column(self, text=None):

        wind_dir_elem = self.elem.find('wind_speed_column_name')
        if not text:
            return wind_dir_elem
        wind_dir_elem.text = text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_wind_spd_column(self, text=None):

        wind_spd_elem = self.elem.find('wind_direction_column_name')
        if not text:
            return wind_spd_elem
        wind_spd_elem.text = text
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class rtmc_parser():

    """Traverse xml tree, find and edit components and write changes"""

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
        """
        Get the root element for a given screen

        Parameters
        ----------
        screen : str, optional
            Name of the screen for which to return the element.
            The default is None.

        Returns
        -------
        xml_element
            Returns list of xml screen elements if screen is None.

        """

        if not screen:
            return self.root.findall('./Screens/screen')
        return (
            self.root.find('./Screens/screen[@screen_name="{}"]'
                           .format(screen))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_element_by_type(self, screen, component_type=None,
                                      look_in_groups=True):

        if component_type:
            component_idx = COMPONENT_DICT[component_type]
        screen_element = self.get_screen_element(screen=screen)
        component_list = screen_element.findall('./Components/component')
        if not component_type:
            return component_list
        if not look_in_groups:
            return [
                x for x in component_list if x.attrib['type'] == component_idx
                ]
        group_list = [x for x in component_list if x.attrib['type']=='10806']
        component_list = [
            x for x in component_list if x.attrib['type']==component_idx
            ]
        for group in group_list:
            component_list += [
                x for x in group.findall('Components/component') if
                x.attrib['type'] in COMPONENT_DICT.values()
                ]
        return component_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_element_by_name(self, screen, component_name=None,
                                      raise_if_missing=True):

        screen_element = self.get_screen_element(screen=screen)
        try:
            if not component_name:
                return screen_element.findall('./Components/component')
        except ValueError:
            pdb.set_trace()
        component_element = (
            screen_element.find(
                './Components/component[@name="{}"]'.format(component_name)
                )
            )
        if not component_element:
            if raise_if_missing:
                raise KeyError(
                    'Could not find component {}'.format(component_name)
                    )
        return component_element
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_edited_screen_component_elements(self, screen):

        components = self.get_component_element_by_name(screen=screen)
        return [
            x.attrib['name'] for x in components if
            x.find('comp_name_manually_editted').text == 'true'
            ]
    #--------------------------------------------------------------------------

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
        output_file_path = str(output_file_path)
        self.tree.write(str(output_file_path))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_editor(self, editor_type, element):

        EDITOR_DICT = {'Image': Image_editor,
                       'Digital': Digital_editor,
                       'TimeSeriesChart': TimeSeriesChart_editor,
                       'Time': Time_editor,
                       'BasicStatusBar': BasicStatusBar_editor,
                       'MultiStateAlarm': Digital_editor,
                       'CommStatusAlarm': Digital_editor,
                       'MultiStateImage': Digital_editor,
                       'NoDataAlarm': Digital_editor,
                       'WindRose': WindRose_editor,
                       'RotaryGauge': Digital_editor}

        return EDITOR_DICT[editor_type](element)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def rename_component_element(self, element, new_name):

        element.attrib['name'] = new_name
    #--------------------------------------------------------------------------