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
class FileSource_editor():

    def __init__(self, elem):

        self.elem = elem

    #--------------------------------------------------------------------------
    def get_set_source_file(self, path=None):

        settings_elem = self.elem.find('settings')
        if not path:
            return settings_elem.attrib['file-name']
        settings_elem.attrib['file-name'] = path
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_source_name(self, name=None):

        if not name:
            return self.elem.attrib['name']
        self.elem.attrib['name'] = name
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Time_editor(Digital_editor):

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

        wind_dir_elem = self.elem.find('wind_direction_column_name')
        if not text:
            return wind_dir_elem
        wind_dir_elem.text = text
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_wind_spd_column(self, text=None):

        wind_spd_elem = self.elem.find('wind_speed_column_name')
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
        self._COMP_DICT = {
            '10702': {'type_name': 'Image', 'function': Image_editor},
            '10101': {'type_name': 'Digital', 'function': Digital_editor},
            '10602': {'type_name': 'Time Series Chart',
                      'function': TimeSeriesChart_editor},
            '10106': {'type_name': 'Time', 'function': Time_editor},
            '10108': {'type_name': 'Segmented Time', 'function': Time_editor},
            '10002': {'type_name': 'Basic Status Bar',
                      'function': BasicStatusBar_editor},
            '10207': {'type_name': 'Multi-State Alarm',
                      'function': Digital_editor},
            '10205': {'type_name': 'Comm Status Alarm',
                      'function': Digital_editor},
            '10712': {'type_name': 'Multi-State Image',
                      'function': Digital_editor},
            '10204': {'type_name': 'No Data Alarm',
                      'function': Digital_editor},
            '10606': {'type_name': 'Wind Rose', 'function': WindRose_editor},
            '10503': {'type_name': 'Rotary Gauge', 'function': Digital_editor}
            }

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def add_element(self, parent_elem, child_elem):

        parent_elem.append(child_elem)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def drop_element(self, parent_elem, child_elem):

        parent_elem.remove(child_elem)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_editor(self, element):

        funcs_dict = {
            x: self._COMP_DICT[x]['function'] for x in self._COMP_DICT
            }
        try:
            type_id = element.attrib['type']
        except KeyError as e:
            raise Exception(
                'This does not appear to be a component element - '
                'did not contain attribute "type"'
                ) from e
        try:
            return funcs_dict[type_id](element)
        except KeyError as e:
            raise Exception(
                'Component element of type {} is not defined!'
                .format(type_id)
                ) from e
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_editor_by_component_name(self, screen, component_name):

        element = self.get_component_element_by_name(
            screen=screen, component_name=component_name
            )
        return self.get_editor(element)
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
        if not component_name:
            return screen_element.findall('./Components/component')
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
    def get_file_source_editor(self):

        return FileSource_editor(
            elem=self.root.find('Sources/source/[@name="Site_details"]')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(self, file_name):

        file_name_fmt = pathlib.Path(file_name)
        if not file_name_fmt.parent.exists:
            raise FileNotFoundError(
                'No such directory as {}!' .format(str(file_name_fmt.parent))
                )
        if not file_name_fmt.suffix == '.rtmc2':
            raise TypeError('File extension must be ".rtmc2"')
        self.tree.write(str(file_name_fmt))
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def rename_component_element(self, element, new_name):

    #     element.attrib['name'] = new_name
    # #--------------------------------------------------------------------------