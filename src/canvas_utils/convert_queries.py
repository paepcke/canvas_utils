#!/usr/bin/env python
'''
Created on Aug 14, 2019

@author: paepcke
'''
import os
import shutil
import sys


class QueryConverter(object):
    '''
    Replaces placeholders for Canvas export database
    names with the actual names. The placeholders are
    
        <canvas_prd> for the name of the MySQL database (a.k.a. schema)
                     where the full Canvas export resides.
        <canvas_aux> for the name of the MySQL database (a.k.a. schema)
                     name where auxiliary Canvas files will be placed.
        <data_dir>   for the path the directory where course information
                     csv files will be provided to canvas_prep.py.

    '''
    aux_placeholder      = '<canvas_aux>'
    prd_placeholder      = '<canvas_db>'
    data_dir_placeholder = '<data_dir>'
    
    #-------------------------
    # Constructor 
    #--------------
    
    def __init__(self, 
                 prd_db, 
                 aux_db, 
                 course_info_dir, 
                 files_to_replace=None,
                 dest_dir=None
                 ):
        '''
        Specify how the query files are to be 
        replaced: 'canvasdata_prd' will be replaced
        with prd_db, 'canvasdata_aux' will be replaced
        with aux_db, and 'Data/explore_courses.csv' will
        be replaced with course_info_dir.
        
        @param prd_db: replacement text for 'canvasdata_prd'
        @type prd_db: str
        @param aux_db: replacement text for 'canvasdata_aux'
        @type aux_db: str
        @param course_info_dir: replacement text for 'data_dir'
        @type course_info_dir: str
        @param files_to_replace: list of files without paths in 
            Query directory to change. If None: all .sql files
        @type files_to_replace: [str]
        '''

        self.prd_db = prd_db
        self.aux_db = aux_db
        self.course_info_dir = course_info_dir
        
        curr_dir  = os.path.dirname(__file__)
        query_dir = os.path.join(curr_dir, 'Queries')
        files = os.listdir(query_dir)
        for file in files:
            # Save the original
            curr_file = os.path.join(query_dir, file)
            saved_file = os.path.join(query_dir, file) + '_Orig'
            shutil.move(curr_file, saved_file)
            # Replace the placeholders in saved_file,
            # and place the resulting file as the original
            # query file:
            self.replace_to_local(saved_file, curr_file)
    
    #-------------------------
    # replace_to_local 
    #--------------
            
    def replace_to_local(self, src, dst):
        '''
        Reads one Query file, and replaces any
        placeholders with their replacement as passed in
        by the user. The result is written to the destination.
        
        @param src: path to query file in which placeholders are
            to be replaced
        @type src: str
        @param dst: path to new file where result is to be written
        @type dst: str
        '''
        
        with open(src, 'r') as fd_in:
            with open(dst, 'w') as fd_out:
                line = fd_in.readline().strip()
                line.replace(QueryConverter.aux_placeholder, self.aux_db)
                line.replace(QueryConverter.prd_placeholder, self.prd_db)
                line.replace(QueryConverter.data_dir_placeholder, self.course_info_dir)
                
                fd_out.writeline()
                
    # -------------------------- Main --------------
    
if __name__ == '__main__':
    
    curr_file  = os.path.basename(__file__)
    
    usage = f"Usage: {curr_file} <db name with Canvas export> <db name for result tables> <course info directory>" 
    
    if len(sys.argv) != 4:
        print(usage)
        sys.exit()

    db_prd = sys.argv[1]
    db_aux = sys.argv[2]
    data_dir = sys.argv[3]
        
    QueryConverter(db_prd, db_aux, data_dir)
    