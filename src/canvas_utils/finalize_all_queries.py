#!/usr/bin/env python
'''
Created on May 2, 2019

@author: paepcke
'''
import argparse
import os
import sys

from setuptools import glob

from finalize_table import AuxTableFinalizer 


class MassQueryFinalizer(object):
    '''
    The Queries directory holds all sql queries
    needed to create the Canvas auxiliary tables.
    But the queries are templates, which allow for
    the canvas_prep.py code to modify source and 
    destination databases.
    
    This utility creates a new directory, if needed,
    and fills it with copies of the Queries/*.sql files
    with all placeholders replaced by defaults. Those .sql
    files can then be used to create the tables. 
    '''
    #------------------------------------
    # constructor 
    #-------------------    

    def __init__(self, dest_dir=None):
        '''
        Constructor
        '''
        script_dir = os.path.join(os.path.dirname(__file__))
        
        # Ensure that dest dir is an absolute path:
        # If no dest_dir given, use default 'FinalizeQueries'
        # below this script's directory:
        if dest_dir is None:
            self.dest_dir = os.path.join(script_dir, 'FinalizedQueries')
        else:
            self.dest_dir = dest_dir
            
        # If the destination path exists, but is a file,
        # complain:
        if os.path.exists(self.dest_dir) and not os.path.isdir(self.dest_dir):
            raise ValueError(f"Destination exists, but is a file: '{self.dest_dir}'")
        elif not os.path.exists(self.dest_dir):
            # Create the dest dir:
            os.mkdir(self.dest_dir, 0o755)
            
        # Directories where the template files are:
        query_dir = os.path.join(script_dir, 'Queries')
        
        # List of .sql files to fill in:
        self.query_template_files = glob.glob(os.path.join(query_dir, '*.sql'))
        
        self.finalizer = AuxTableFinalizer()
                                                
    #------------------------------------
    # finalize_sql_files 
    #-------------------    
    
    def finalize_sql_files(self):
        for sql_file_path in self.query_template_files:
            # Do the work; the call returns the finished query
            # as a string:
            resolved_sql = self.finalizer.finalize_tbl_creation(sql_file_path)
            # Get just the 'tablename.sql' part of the file name:
            sql_file_name = os.path.basename(sql_file_path)
            
            # Write the filled-in query to a same-named file in 
            # the target directory:
            with open(os.path.join(self.dest_dir, sql_file_name), 'w') as fd:
                fd.write(resolved_sql)
                fd.write('\n')
                
        print("Finalized all queries.")

# --------------------- Main ------------------

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Fill query templates with default values."
                                     )
    default_dir = os.path.join(os.path.dirname(__file__), 'FinalizedQueries')
    parser.add_argument('-d', '--dir',
                        help=f'Directory where to place result .sql files; default: {default_dir}.',
                        default=default_dir)
                        
    args = parser.parse_args();
    finalizer = MassQueryFinalizer(dest_dir=args.dir)
    finalizer.finalize_sql_files()
    
