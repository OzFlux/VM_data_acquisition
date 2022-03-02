#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 14:40:21 2021

@author: imchugh
"""

import pandas as pd
import pathlib
import xml.etree.ElementTree as ET
import pdb

path='/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'

class rtmc_parser():

    def __init__(self, path):

        self.path = path
        self.tree = ET.parse(path)
        self.root = self.tree.getroot()
        self.parent_map = {c: p for p in self.tree.iter() for c in p}
        self.state_change = False

    #--------------------------------------------------------------------------
    def _get_component_code(self, component):

        component_dict = {'Image': '10702',
                          'Digital': '10101',
                          'TimeSeriesChart': '10602',
                          'Time': '10108',
                          'BasicStatusBar': '10002',
                          'MultiStateAlarm': '10207',
                          'CommStatusAlarm': '10205',
                          'MultiStateImage': '10712',
                          'NoDataAlarm': '10204'}
        return component_dict[component]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_elements_by_str(self, comp, search_str, elem=None):

        lambdas_dict = {'tag': lambda x: str(x.tag),
                        'attrib': lambda x: str(x.attrib),
                        'text': lambda x: str(x.text)}
        try:
            func = lambdas_dict[comp]
        except KeyError as e:
            raise e('comp must be one of {}'
                    .format(', '.join(list(lambdas_dict.keys()))))
        l = []
        if elem:
            for child_elem in elem.iter():
                if search_str in func(child_elem):
                    l.append(child_elem)
        else:
            for child_elem in self.tree.iter():
                if search_str in func(child_elem):
                    l.append(child_elem)
        return l
    #--------------------------------------------------------------------------

    # def get_component_info(self, screen, component, edited_only=False):

    #     split_component = component.split('_')[-1]
    #     pdb.set_trace()
    #     try:
    #         components = self.get_elements_of_type(screen=screen,
    #                                                the_type=split_component)
    #     except KeyError:
    #         pass
    #     output_dict = {}
    #     for this_comp in components:
    #         label = this_comp.attrib['name']
    #         output_dict[label] = this_comp
    #     if
    #     return output_dict

    #--------------------------------------------------------------------------
    def get_element_info(self, elem):

        info_dict = {'element': elem}
        try:
            info_dict['name'] = elem.attrib['name']
        except KeyError:
            info_dict['name'] = None
        info_dict['parent_path'] = (
            self.get_element_path(elem=elem, incl_attr=True)
            )
        info_dict['children'] = [x.tag for x in elem]
        return info_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_element_path(self, elem, incl_attr=False):

        parent_map = self.parent_map
        l = []
        this_elem = elem
        while True:
            try:
                this_elem = parent_map[this_elem]
                tag = str(this_elem.tag)
                if incl_attr:
                    if this_elem.attrib:
                        attrib = (
                            ' '.join(['{0}="{1}"'
                                       .format(str(entry),
                                               str(this_elem.attrib[entry]))
                                       for entry in this_elem.attrib])
                            )
                        tag = tag + ' ' + attrib
                l.append(tag)
            except KeyError:
                break
        l.reverse()
        return l
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_screen_elements(self, screen=None):

        if not screen:
            return self.root.findall('./Screens/screen')
        return (
            self.root.find('./Screens/screen[@screen_name="{}"]'
                           .format(screen))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_component_elements(self, screen, component=None):

        """
        Returns screen components for passed screen name.

        Parameters
        ----------
        screen : str
            Name of the screen for which to return components.
        component : str, optional
            Name of the component to return. The default is None.

        Returns
        -------
        list
            All component elements for given screen.
        """

        screen_ele = self.get_screen_elements(screen=screen)
        if not component:
            return screen_ele.findall('./Components/component')
        return (
            screen_ele.find('./Components/component[@name="{}"]'
                            .format(component))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    # def get_calculation(self, screen, component):

    #     ele = self.get_component_elements(screen=screen, component=component)
    #     calc_ele = ele.find('./calculation')
    #     # pdb.set_trace()
    #     if calc_ele is None:
    #         raise KeyError('No calculation for screen {0} and component {1}'
    #                        .format(screen, component))
    #     return calc_ele.text

    def get_plot_traces(self, screen, component, trace=None):

        comp_ele = (
            self.get_component_elements(screen=screen, component=component)
            )
        if not trace:
            return comp_ele.findall('./Traces/traces')
        return (
            comp_ele.find('./Traces/traces[@label="{}"]'.format(trace))
            )

    # def get_digital_calcs(self):

    #     l = []
    #     component_type = self._get_component_code(component='Digital')
    #     for tup in self.rtmc_df.index:
    #         screen, component = tup[0], tup[1]
    #         elem = self.get_component_elements(screen=screen,
    #                                            component=component)
    #         if elem.attrib['type'] == component_type:
    #             l.append(elem)
    #     return l
    #     # return elem.find('./calculation').text

    def get_elements_of_type(self, screen, the_type, incl_info=False):

        code = self._get_component_code(the_type)
        all_components = self.get_component_elements(screen=screen)
        elem_list = [x for x in all_components if x.attrib['type'] == code]
        if not incl_info:
            return elem_list
        return [self.get_element_info(x) for x in elem_list]

    def set_element_text(self, elem, text):

        elem.text = text
        self.state_change = True
        return

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



#------------------------------------------------------------------------------
def _make_component_df(path):

    df = pd.read_excel(path, sheet_name='RTMC_components')
    df.index = (
        pd.MultiIndex.from_tuples(zip(df['Screen name'],
                                      df['RTMC component name']),
                                  names=['screen','component'])
        )
    df.drop(labels=['Screen name', 'RTMC component name'], axis=1, inplace=True)
    df.columns = ['long_name', 'operation', 'axis']
    return df
#------------------------------------------------------------------------------