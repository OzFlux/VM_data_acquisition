#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 14:23:32 2022

@author: imchugh
"""

import pdb
import variable_mapper as vm
import rtmc_parser as rp

class combo_class():

    def __init__(self, site):

        xl_path = '/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx'
        xml_path = '/home/unimelb.edu.au/imchugh/Desktop/Boyagin_dark.rtmc2'

        self.variable_mapper = vm.variable_mapper(path=xl_path, site=site)
        self.rtmc_parser = rp.rtmc_parser(path=xml_path)

    def get_component(self, screen, component):

        info = self.variable_mapper.get_rtmc_component_variables(
            screen=screen, component=component)
        info['element'] = self.rtmc_parser.get_component_elements(
            screen=screen, component=component)
        return info

    def set_component(self, screen, component):

        elem_info = self.get_component(screen=screen, component=component)
        sub_elem = elem_info['element'].find('./calculation')
        new_str = elem_info['info'].rtmc_name
        self.rtmc_parser.set_element_text(elem=sub_elem, text=new_str)
        return

    def parse_screen(self, screen):

        for component in self.variable_mapper.rtmc_df.loc[screen].index:

            try:
                if component.split('_')[1] == 'Digital':
                    self.set_component(screen=screen, component=component)
                else:
                    pass
            except IndexError:
                pdb.set_trace()

    def write_to_file(self):

        self.rtmc_parser.write_to_file()



        # elems = self.rtmc_parser.get_elements_of_type(screen=screen,
        #                                               the_type=the_type)
        # output_dict = {}
        # for elem in elems:
        #     label = elem.attrib['name']
        #     output_dict[label] = (
        #         self.variable_mapper.get_rtmc_component_fields(
        #             screen=screen, component=label, field='long_name')
        #         )
        # return output_dict