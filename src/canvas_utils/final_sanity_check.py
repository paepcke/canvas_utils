'''
Created on Sep 12, 2019

@author: paepcke
'''
import os
from pathlib import Path

from canvas_utils_exceptions import TableExportError

class SanityChecker(object):
    '''
    Checks superficially whether aux table export 
    runs succeeded. Ensures that all tables have a
    .csv file in the table copy dir, and that none
    of those have zero length.
    '''
    
    #-------------------------
    # constructor 
    #--------------

    def __init__(self, unittest=False):
        '''
        Constructor
        
        @param unittest: set to True if caller is a unittest
        @type unittest: bool
        '''
        self.curr_dir = os.path.dirname(__file__)
        HOME = os.getenv('HOME')
        self.table_export_dir_path = f"{HOME}/CanvasTableCopies"
        if unittest:
            # Let unittests redefine table_export_dir and do their thing:
            return
        self.init_table_vars()
        
        try:
            self.check_num_files()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            
        try:
            self.check_for_zero_len_exports()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            
        
    #-------------------------
    # check_num_files
    #--------------
        
    def check_num_files(self):
        '''
        Return True if there is one TableName.csv file
        in the table copy directory as there are table
        definitions in the Queries subdir. I missing tables,
        they are printed in an error msg, and False is returned.
        
        @return: True if all goes well. Else throws error.
        @rtype: bool
        @raise TableExportError: if not all tables are present. The 
            list of missing tables will be in the exceptions tables_list
            property
        '''
        # Must have at least the aux tables as .csv files 
        # in the dir to which we export tables. There might be
        # others, such as backups. So the intersect of just the
        # aux tables and the tables represented in the copy dest
        # must equal the table list:    
        if not self.all_tables_set.intersection(self.copied_tables_set) == self.all_tables_set:
            missing_tables = self.all_tables_set.difference(self.copied_tables_set)
            raise TableExportError("Did not export all tables:", table_list=missing_tables)
        return True

    #-------------------------
    # check_exported_file_dates 
    #--------------

    def check_exported_file_dates(self, time_threshold=24):
        '''
        Ensures that all the exported .csv tables are reasonably
        fresh. Only produces a warning: if any table is older
        by more than 24 hours than the most reacent table. 
        
        @param time_threshold: number of hours within which .csv
            file exports need to have been created to avoid a 
            warning message
        @type time_threshold: int
        @return: list of table names whose .csv file is older
            than time_threshold hours than the most recent table
            .csv file.
        '''
        # Time threshold in seconds:
        secs_threshold = 3600 * time_threshold
        
        # Create dict: Table name==>.csv-file-date
        tbl_age_dict = {}
        for tbl_nm in self.copied_tables_set:
            tbl_path = os.path.join(self.table_export_dir_path, tbl_nm + '.csv')
            # Last modified time in seconds since epoch:
            last_mod_time = os.path.getctime(tbl_path)
            tbl_age_dict[tbl_nm] = last_mod_time
            
        # Newest file:
        most_recent = max(tbl_age_dict.values())
        # Identify files older than time_threshold
        old_files = []
        for (tbl_nm, mod_time) in tbl_age_dict.items():
            if most_recent - mod_time > secs_threshold:
                old_files.append(tbl_nm)

        return old_files

    #-------------------------
    # check_for_zero_len_exports 
    #--------------

    def check_for_zero_len_exports(self):
        '''
        Ensure none of the export file has
        zero length
        
        @return: True if all is well.
        @rtype: bool
        @raise TableExportError: if missing tables are found. 
            First, collects all missing tables. Then raises
            the error, including the list of missing tables 
            in the exception's table_list property.
        '''
        missing_tables = []
        for table_name in self.all_tables:
            file_path  = os.path.join(self.table_export_dir_path, table_name + '.csv')
            file_stats = os.stat(file_path)
            file_len   = file_stats.st_size
            if file_len == 0:
                missing_tables.append(table_name)
                
        if len(missing_tables) > 0:
            raise TableExportError("Zero length table exports", missing_tables)

        return True

    #-------------------------
    # init_table_vars 
    #--------------
    
    def init_table_vars(self):
        '''
        Initializes instance vars:
            self.all_tables       : names of all aux tables      
            self.all_tables_set.  : set version of all aux tables
            self.copied_tables,   : all aux tables that were exported
            self.copied_table_set : set version of exported aux tables
        '''

        all_files_in_Queries = os.listdir(os.path.join(self.curr_dir, 'Queries'))
        # Want only the .sql files:
        query_files = filter(lambda file_name: file_name.endswith('.sql'), all_files_in_Queries)
        self.all_tables  = [Path(file_name).stem for file_name in query_files]
        
        all_copied_table_files = os.listdir(self.table_export_dir_path)
        # Don't want the schema files:
        csv_files = filter(lambda file_name: file_name.endswith('.csv'), all_copied_table_files)
        # Get the table names:
        self.copied_tables = [Path(file_name).stem for file_name in csv_files]

        self.all_tables_set = set(self.all_tables)
        self.copied_tables_set   = set(self.copied_tables)
        
