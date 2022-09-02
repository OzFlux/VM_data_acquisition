#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  4 11:23:06 2022

@author: imchugh
"""

from copy import deepcopy
import pdb
import pathlib
import xml.etree.ElementTree as ET

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES
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
        child_elem.attrib['label'] = new_label
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
            '10702': {'type_name': 'Image', 'class': Image_editor},
            '10101': {'type_name': 'Digital', 'class': Digital_editor},
            '10602': {'type_name': 'Time Series Chart',
                      'class': TimeSeriesChart_editor},
            '10106': {'type_name': 'Time', 'class': Time_editor},
            '10108': {'type_name': 'Segmented Time', 'class': Time_editor},
            '10002': {'type_name': 'Basic Status Bar',
                      'class': BasicStatusBar_editor},
            '10207': {'type_name': 'Multi-State Alarm',
                      'class': Digital_editor},
            '10205': {'type_name': 'Comm Status Alarm',
                      'class': Digital_editor},
            '10712': {'type_name': 'Multi-State Image',
                      'class': Digital_editor},
            '10204': {'type_name': 'No Data Alarm',
                      'class': Digital_editor},
            '10606': {'type_name': 'Wind Rose', 'class': WindRose_editor},
            '10503': {'type_name': 'Rotary Gauge', 'class': Digital_editor}
            }

    #--------------------------------------------------------------------------
    ### METHODS
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_editor(self, element):
        """
        Get the appropriate editor for the passed element type.

        Parameters
        ----------
        element : xml.etree.ElementTree.Element
            The element for which to find the editor.

        Raises
        ------
        Exception
            Raise if something other than an element, or an element of unknown
            type, is passed.

        Returns
        -------
        Class
            Editor class for the element type.

        """

        funcs_dict = {
            x: self._COMP_DICT[x]['class'] for x in self._COMP_DICT
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
        """
        Get the component from the xml tree and get the appropriate editor.

        Parameters
        ----------
        screen : str
            RTMC screen to parse for the given component.
        component_name : str
            The component name for the requested element.

        Returns
        -------
        Class
            Editor class for the element type.

        """

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
        xml.etree.ElementTree.Element
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
        """
        Get all elements of component_type for a given screen.

        Parameters
        ----------
        screen : str
            RTMC screen to parse for the given component type.
        component_type : str, optional
            The component type. The default is None.
        look_in_groups : bool, optional
            Whether to open and look in groups. This is untested, and groups 
            have been eliminated from the project. Possibly delete.
            The default is True.

        Returns
        -------
        list
            A list of elements of the requested type.

        """

        if component_type:
            component_dict = {
                self._COMP_DICT[x]['type_name']: x 
                for x in self._COMP_DICT.keys()
                }
            component_idx = component_dict[component_type]
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
                x.attrib['type'] in component_dict.values()
                ]
        return component_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_element_by_name(self, screen, component_name=None,
                                      raise_if_missing=True):
        """
        Get the xml element of component_name from a given screen.

        Parameters
        ----------
        screen : str
            RTMC screen to parse for the given component type.
        component_name : str
            The component name for the requested element. The default is None.
        raise_if_missing : bool, optional
            Whether to raise error if element is not found. The default is True.

        Raises
        ------
        KeyError
            Raise if raise_if_missing is True and element is not found.

        Returns
        -------
        xml.etree.ElementTree.Element
            The requested component element from the xml tree.

        """

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
        """
        Get all elements that have manually edited names on the screen. Redundant.

        Parameters
        ----------
        screen : str
            RTMC screen to parse for the edited component elements.

        Returns
        -------
        list
            All edited component elements for screen.

        """

        components = self.get_component_element_by_name(screen=screen)
        return [
            x.attrib['name'] for x in components if
            x.find('comp_name_manually_editted').text == 'true'
            ]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_source_editor(self, source_type):
        """
        Get editor for getting and setting file-based data sources.

        Parameters
        ----------
        source_type : str
            The file source type ('data' or 'details') for which to get the 
            editor.

        Raises
        ------
        KeyError
            Raise if anything other than 'data' or 'details' is passed to 
            source_type.

        Returns
        -------
        Class
            File source editor class.

        """

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
        """
        Get the settings editor :)

        Returns
        -------
        Class
            Settings editor. This class can be expanded arbitrarily as need 
            arises.

        """
        
        return BasicSettings_editor(elem=self.root)
    #-------------------------------------------------------------------------- 

    #--------------------------------------------------------------------------
    def write_to_file(self, file_name):
        """
        Write changes back to a new file (no overwrite of template file 
                                          allowed).

        Parameters
        ----------
        file_name : str
            The target path and file name to write to.

        Raises
        ------
        FileNotFoundError
            Raised if the path passed to 'file_name' is not valid.
        TypeError
            Raised if the file extension of the string passed to 'file_name' is 
            not rtmc2.
        FileExistsError
            Raised if the user passes the same path as the path to the 
            template.

        Returns
        -------
        None.

        """

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