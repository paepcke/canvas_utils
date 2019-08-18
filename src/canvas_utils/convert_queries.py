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
    Replaces the hardcoded names of the Canvas database that contains
    full exports, and the database name for the auxiliary tables
    in the query files of the Queries subdirectory. See README.md for
    why the names are hardcoded in those query files to be Stanford
    specific. This utility is meant for localizing this package to
    universities where these two databases are known by other names. 
    
    On the command line the utility accepts replacements for the 
    canvasdata_prd (full exports db), the canvasdata_aux (destination 
    of auxiliary tables), and location of the course information directory.
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
        @param files_to_replace: list of full path files to 
            create a replacement for. If None: 
            Default: All .sql files in the Queries subdirectory 
            below this file's directory.
        @type files_to_replace: [str]
        @param dest_dir: directory where to place the new 
            query files. Default: directory where this script
            resides. 
        @type dest_dir: str
        '''

        self.prd_db = prd_db
        self.aux_db = aux_db
        self.course_info_dir = course_info_dir
        
        curr_dir  = os.path.dirname(__file__)
        
        if dest_dir is None:
            dest_dir = curr_dir
            
        if files_to_replace is None:
            # Replace all .sql files in the Queries subdir:
            
            # Queries subdir below this script's dir:
            query_dir = os.path.join(curr_dir, 'Queries')
            files_to_replace = os.listdir(query_dir)
            
            # Make the file names full paths:
            files_to_replace = [os.path.join(curr_dir, file_name) for file_name in files_to_replace]
            
            # Filter for pulling out only .sql files:
            sql_file_filter = filter(lambda fname: fname.endswith('.sql'), files_to_replace)
            # Pull the .sql files out of the filter:
            files_to_replace = [file_name for file_name in sql_file_filter]

        elif not type(files_to_replace) == list:
            # If files to replace is given, must be a list:
            files_to_replace = [files_to_replace]
        
        for file in files_to_replace:
            # Save the original in the dest dir with extension '_Orig':
            saved_file = os.path.join(dest_dir, file) + '_Orig'
            shutil.copy(file, saved_file)
            
            # Replace the placeholders in saved_file,
            # and place the resulting file as the original
            # query file:
            
            dest_file = os.path.join(dest_dir, os.path.basename(file))
            self.replace_to_local(saved_file, dest_file)
    
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
        
        with open(dst, 'w') as fd_out:
            with open(src, 'r') as fd_in:
                for line in fd_in:
                    line = line.strip('\n')
                    line = line.replace(QueryConverter.aux_placeholder, self.aux_db)
                    line = line.replace(QueryConverter.prd_placeholder, self.prd_db)
                    line = line.replace(QueryConverter.data_dir_placeholder, self.course_info_dir)
                    
                    fd_out.write(line + '\n')
                
    # -------------------------- Main --------------
    
if __name__ == '__main__':
    
    curr_file  = os.path.basename(__file__)
    
    usage = f"Usage: {curr_file} <db name with Canvas exports> <db name for result tables> <course info directory>" 
    
    if len(sys.argv) != 4:
        print(usage)
        sys.exit()

    db_prd = sys.argv[1]
    db_aux = sys.argv[2]
    data_dir = sys.argv[3]
        
    QueryConverter(db_prd, db_aux, data_dir)
    