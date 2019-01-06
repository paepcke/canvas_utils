#!/usr/bin/env python
'''
Created on Jan 5, 2019

@author: Andreas Paepcke
'''
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")
from canvas_utils.finalize_table import AuxTableFinalizer

class TblExtractor(object):
    '''
    Creates DROP and CREATE TABLE sql statements to 
    remove, and then make all canvasdata_aux tables.
    
    Method:
    Finds SQL creation files in the Queries subdirectories.
    Outputs all the DROP TABLE and CREATE TABLE statements
    at the top of each file. Relies on marker
     
             # <end_creation>
             
    to be the end of the table creation statement.
    
    Outputs to a file or to stdout.
    '''
    
    TARGET_DB = 'canvasdata_aux'

    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, outfile_or_fd=sys.stdout):
        '''
        Constructor. Prepares the out stream, then calls workhorse.
        
        @param outfile_or_fd: where to write the output SQL statements
        @type outfile_or_fd: {str | file-like}. Default: stdout
        '''
        
        # An object that can replace placeholders in the 
        # .sql files with their values (such as
        # <canvas_prd> ==> 'canvasdata_prd':
        self.table_finalizer = AuxTableFinalizer()
        
        if type(outfile_or_fd) == str:
            # Out arg is a file path; create an out stream:
            with open(outfile_or_fd, 'w') as out_fd:
                self.make_tbl_creation_code(TblExtractor.TARGET_DB, out_fd)
        else:
            # Out arg is a file-like:
            self.make_tbl_creation_code(TblExtractor.TARGET_DB, outfile_or_fd)
            
    #-------------------------
    # make_tbl_creation_code 
    #--------------
    
    def make_tbl_creation_code(self, db, outfd=sys.stdout):
        '''
        Finds the .sql tables, and calls the snippet
        extractor with each file. Sequentially writes
        the DROP/CREATE statements to the output.
        
        Prepends instructions to create the target db
        if needed, and to USE that db.
        
        @param db: the database to which the table creation 
            instructions should refer.
        @type db: str
        @param outfd: output stream where to write results
        @type outfd: file-like
        '''
        
        curr_dir = os.path.dirname(__file__)
        sql_files_dir = os.path.join(curr_dir, 'Queries')
        sql_files = [os.path.join(sql_files_dir, file) for file in os.listdir(sql_files_dir) if file.endswith('.sql')]
        
        # Output database creation statement if requested:
        outfd.write("CREATE DATABASE IF NOT EXISTS {};\n".format(db))
        outfd.write("USE {};\n".format(db))
        
        for sql_file in sql_files:
            sql_statements = self.parse_from_file(sql_file)
            outfd.write(sql_statements)
    
    #-------------------------
    # parse_from_file 
    #--------------
    
    def parse_from_file(self, file_path):
        '''
        Given an absolute path to an sql file with
        leading DROP/CREATE-TABLE statements, returns
        just those two statements. 
        
        Relies on marker "# <end_creation>" to indicate
        end of the create statement. 
        
        @param file_path: absolute path to .sql file
        @type file_path: str
        @return: the leading DROP/CREATE-TABLE statements.
        @rtype: str
        @raise ValueError: 
        '''
        
        with open (file_path, 'r') as fd:
            # Read the whole sql file, and replace the placeholders
            # (such as <canvas_prd>, <canvas_aux>, <data_dir>:
            all_sql = self.table_finalizer.finalize_tbl_creation(fd)

            # Find the end of the CREATE TABLE statement:            
            create_end_ptr = all_sql.find('# <end_creation>')
            
            if create_end_ptr == -1:
                raise ValueError("File {} does not have an end-create marker.".format(file_path))
        return all_sql[:create_end_ptr]
            
# ------------------------- Main ------------------            
        
if __name__ == '__main__':
    
    TblExtractor()