#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  1 15:45:32 2021

@author: imchugh
"""

import ftplib
import pathlib
import sys

#------------------------------------------------------------------------------
### Constants
#------------------------------------------------------------------------------
DIRS_DICT = {'satellite': 'anon/gen/gms',
             'mslp': 'anon/gen/fwo'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Classes
#------------------------------------------------------------------------------

class bom_ftp_getter():

    """Get ftp chart file lists from bom ftp server and get files"""

    def __init__(self, img_type):

        try:
            self.img_type = DIRS_DICT[img_type]
        except KeyError as e:
            raise Exception('img_type must be one of: {}'
                            .format(', '.join(list(DIRS_DICT.keys())))) from e
        bom_ftp = 'ftp.bom.gov.au'
        ftp = ftplib.FTP(bom_ftp)
        ftp.login()
        self.ftp = ftp

    def get_file_list(self, file_type=None, search_str=None):

        files = self.ftp.nlst(self.img_type)
        if file_type:
            files = [x for x in files if file_type in x]
        if search_str:
            files = [x for x in files if search_str in x]
        return files

    def get_file(self, src_file, output_dir, output_name=None):

        if not output_name:
            out_file = pathlib.Path(output_dir) / pathlib.Path(src_file).name
        else:
            out_file = pathlib.Path(output_dir) / output_name
        with open(out_file, 'wb') as f:
            self.ftp.retrbinary('RETR {}'.format(src_file), f.write)

if __name__ == "__main__":

    out_path = sys.argv[1]
    getter = bom_ftp_getter(img_type='satellite')
    file_list = getter.get_file_list(file_type='jpg', search_str='IDE00135')
    getter.get_file(src_file=file_list[-1], output_dir=out_path,
                    output_name='sat_img.jpg')
    getter = bom_ftp_getter(img_type='mslp')
    file_list = getter.get_file_list(file_type='png', search_str='IDY00030')
    getter.get_file(src_file=file_list[-1], output_dir=out_path,
                    output_name='mslp.png')