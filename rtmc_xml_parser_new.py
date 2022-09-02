#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 11:23:06 2022

@author: imchugh
"""

from copy import deepcopy
import pdb
import pathlib
import sys
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

# RTMC_IMAGE_PATH = (
#     'E:\\Cloudstor\\Network_documents\\RTMC_files\\Static_images\\Site_images'
#     )
# RTMC_XML_PATH = 'E:\\Campbellsci\RTMC\Gingin_overhaul.rtmc2'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class RTMC_syntax_generator():
    
    def __init__(self, raw_output):
        
        self.raw_output = raw_output
    
    #--------------------------------------------------------------------------
    def get_alias_map(self):

        var_name = self.raw_output.split('.')[-1]
        return 'Alias({0},"{1}");'.format(var_name, self.raw_output)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_alias_output(self):

        var_name = self.raw_output.split('.')[-1]
        return self._str_joiner(
            ['Alias({0},"{1}");'.format(var_name, self.raw_output), var_name]
            )
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _str_joiner(self, str_list, joiner='\r\n\r\n'):

        return joiner.join(str_list)
    #--------------------------------------------------------------------------
    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### COMPONENT EDITING CLASSES ###
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
    def get_set_pointer_calculation_text(self, pointer=None, text=None):

        """Get and set pointer calculation element"""
        d = {'max': 'max_pointer', 'min': 'min_pointer'}
        if not pointer:
            element = self.elem.find('Pointers/pointer/calculation')
        else:
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
class BasicSettings_editor():

    def __init__(self, elem):

        self.elem = elem
        
    def get_set_snapshot_destination(self, text=None):

        snapshot_element = self.elem.find('snapshot_directory')
        if not text:
            return snapshot_element.text
        snapshot_element.text = text
        
    def get_set_snapshot_screen_state(self, screen, state=None):

        enabled_element = self.elem.find(
            './Screens/screen[@screen_name="{}"]/snapshot_enabled'
            .format(screen)
            )
        if not state:
            return enabled_element.text
        enabled_element.text = state
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileSource_editor():

    def __init__(self, elem):

        self.elem = elem


    #--------------------------------------------------------------------------
    def get_sources(self):
        
        return 
    #--------------------------------------------------------------------------
    
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
    def get_axis_by_label(self, label):

        elem = self.get_trace_element_by_label(label=label)
        axis = elem.find('trace').attrib['vertical-axis']
        if axis == '1':
            return 'right'
        return 'left'
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

        return self.elem.find('Traces/traces[@label="{}"]'.format(label))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_set_trace_calculation_by_label(self, label, calculation_text=None,
                                           label_text=None):

        elem = self.get_trace_element_by_label(label=label)
        calculation_elem = elem.find('calculation')
        if not calculation_text:
            return calculation_elem.text
        calculation_elem.text = calculation_text
        if label_text:
            elem.attrib['label'] = label_text
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def set_trace_attributes_by_label(
            self, label, **kwargs):

        elem = self.get_trace_element_by_label(label=label)
        if 'new_label' in kwargs:
            elem.attrib['label'] = kwargs['new_label']
        if 'calculation' in kwargs:
            calculation_elem = elem.find('calculation')
            calculation_elem.text = kwargs['calculation']
        if 'rgb' in kwargs:
            colours_elem = elem.find('trace/pen')
            colours_elem.attrib['colour'] = kwargs['rgb']
        if 'title' in kwargs:
            title_elem = elem.find('trace')
            title_elem.attrib['title'] = kwargs['title']    
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def drop_trace_element_by_label(self, label):
        
        parent_elem = self.elem.find('Traces')
        child_elem = self.get_trace_element_by_label(label=label)
        parent_elem.remove(child_elem)
        n_child_elems = len(self.get_trace_labels())
        parent_elem.attrib['count'] = str(n_child_elems)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def duplicate_trace_element_by_label(self, old_label, new_label):
        
        parent_elem = self.elem.find('Traces')
        child_elem = deepcopy(self.get_trace_element_by_label(label=old_label))
        try:
            child_elem.attrib['label'] = new_label
        except AttributeError:
            pdb.set_trace()
        parent_elem.append(child_elem)
        n_child_elems = len(self.get_trace_labels())
        parent_elem.attrib['count'] = str(n_child_elems)
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

    # #--------------------------------------------------------------------------
    # def add_element(self, parent_elem, child_elem):

    #     parent_elem.append(child_elem)
    # #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def drop_element(self, parent_elem, child_elem):

    #     parent_elem.remove(child_elem)
    # #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_editor(self, element):

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
        return self.get_component_editor(element)
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
    def get_file_source_editor(self, source_type):

        type_dict = {'data': 'DataFile', 'details': 'DetailsFile'}
        if not source_type in type_dict.keys():
            raise KeyError(
                '"file_type" arg must be one of: {}'
                .format(', '.join(type_dict.keys()))
                )
        return FileSource_editor(
            elem=self.root.find(
                'Sources/source/[@name="{}"]'.format(type_dict[source_type])
                )
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_basic_settings_editor(self):
        
        return BasicSettings_editor(elem=self.root)
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
        if file_name_fmt == self.path:
            raise FileExistsError('No overwrite of template file allowed!')
        self.tree.write(str(file_name_fmt))
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def rename_component_element(self, element, new_name):

    #     element.attrib['name'] = new_name
    # #--------------------------------------------------------------------------
    
if __name__ == '__main__':
    
    pass
    