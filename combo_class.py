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
        try:
            info['element'] = self.rtmc_parser.get_component_elements(
                screen=screen, component=component)
        except TypeError:
            pdb.set_trace()
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

a = vm.variable_mapper(
    path='/home/unimelb.edu.au/imchugh/Desktop/site_variable_map.xlsx',
    site='Calperum'
    )
b = rp.rtmc_parser(
    path='/home/unimelb.edu.au/imchugh/Desktop/Boyagin_dark.rtmc2'
    )
component_list = a.rtmc_df.loc['Meteorology'].index.tolist()
sub_list = [x for x in component_list if 'Digital' in x]


screen='System'
component='AH_TimeSeriesChart'
the_element = b.get_component_elements(screen=screen, component=component)
long_names = (
    a.rtmc_df.sort_index().loc[(screen, component), 'long_name']
    .item()
    .split(',')
    )
l = [a.get_rtmc_variable(long_name=long_name) for long_name in long_names]
editor = rp.time_series_editor(elem=the_element)

# d = {}
# for var in sub_list:
#     d[var] = {
#         'var_map': a.get_rtmc_component_variables(screen='System',
#                                                   component=var),
#         'elem': b.get_component_elements(screen='System', component=var)
#         }
#     elem = d[var]['elem'].find('./calculation')
#     print('For variable {}:'.format(var))
#     print('    - Old text: {}'.format(elem.text))
#     try:
#         text = d[var]['var_map'][0].get_rtmc_output(alias=False)
#     except TypeError:
#         text = d[var]['var_map'][0].get_rtmc_output()
#     print('    - New text: {}'.format(text))
#     b.set_element_text(elem, text)
# b.write_to_file(write_to_self=True)



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