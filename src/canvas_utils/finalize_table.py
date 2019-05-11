#!/usr/bin/env python
'''
Created on Jan 2, 2019

@author: Andreas Paepcke
'''
import argparse
import os
import sys

class AuxTableFinalizer(object):
    '''
    SQL canvas_aux table creation files in the Queries subdir
    have placeholders interspersed to allow for changes in 
    names for the production and aux database names.
    
    This class' finalize_tbl_creation() replaces these 
    placeholders with given real values, and outputs to
    result to stdout.
    
    Usage: create an instance, then call finalize_tbl_creation()
    method on each .sql file. 
    
    '''
    
    
    PROD_DB  = 'canvasdata_prd'
    AUX_DB   = 'canvasdata_aux'
    DATA_DIR = os.path.join(os.path.dirname(__file__), 'Data') 
    
    #-------------------------
    # Constructor 
    #--------------
    
    def __init__(self, prod_db=None, aux_db=None, data_dir=None):
        '''
        Initialize all instance var with given values, or
        their defaults.
        
        @param prod_db: name of main canvas data database. E.g. canvasdata_prd 
        @type prod_db: str
        @param aux_db: name of auxiliary canvas data database. E.g. canvasdata_aux 
        @type aux_db: str
        @param data_dir: directory where to put ExploreCourses xml and csv file.
        @type data_dir: str
        '''
        
        if prod_db is None:
            prod_db = AuxTableFinalizer.PROD_DB
            
        if aux_db is None:
            aux_db = AuxTableFinalizer.AUX_DB
            
        if data_dir is None:
            data_dir = AuxTableFinalizer.DATA_DIR

        curr_dir = os.path.dirname(__file__)
        if not os.path.isabs(data_dir):
            data_dir = os.path.join(curr_dir, data_dir)
        else:
            data_dir = data_dir

        self.prod_db  = prod_db
        self.aux_db   = aux_db
        self.data_dir = data_dir
        self.curr_dir = curr_dir

        # Ready for calls to finalize_tbl_creation
        
    #-------------------------
    # finalize_tbl_creation 
    #--------------

    def finalize_tbl_creation(self, sql_file_or_fd):
        '''
        Do the work: read the file into a string, 
        replace all placeholders, and return the
        result
        
        @param sql_file_or_fd: the sql file to finalize
        @type sql_file_or_fd: {str | file-like}
        @return the sql text with all placeholders replaced.
        @rtype: str
        '''
        
        if type(sql_file_or_fd) == str:
            file_path = sql_file_or_fd
            with open(sql_file_or_fd, 'r') as fd:
                sql_txt = fd.read().strip()
        else:
            file_path = sql_file_or_fd.name
            sql_txt = sql_file_or_fd.read().strip()
            
        print(f"Finalizing query for {os.path.basename(file_path)}...")
        sql_txt_final = sql_txt.replace('<canvas_db>', self.prod_db)
        sql_txt_final = sql_txt_final.replace('<canvas_aux>', self.aux_db)
        sql_txt_final = sql_txt_final.replace('<data_dir>', self.data_dir)
    
        return sql_txt_final        

# ------------------------------- Main -----------------------
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Substitute db name for <canvas_db> in Queries/<sqlFile>.sql."
                                     )

    parser.add_argument('sqlfile',
                        help='fully qualified path to SQL file, or just the filename. \n' +
                             'In latter case, location in Queries subdir assumed'
                        )
    parser.add_argument('prod_db',
                        help='Name of database to substitute for <canvas_db> in SQL files.'
                        )
    parser.add_argument('aux_db',
                        help='Name of database to substitute for <canvas_aux> in SQL files.'
                        )
    parser.add_argument('data_dir',
                        help="Full path to Data dir, or just the last part (e.g. 'Data'."
                        )

    args = parser.parse_args();

        
    finalizer = AuxTableFinalizer(args.prod_db, args.aux_db, args.data_dir)
    sys.stdout.write(finalizer.finalize_tbl_creation(args.sqlfile))