#!/usr/bin/env python
'''
Created on Sep 12, 2019

@author: paepcke
'''
from email.message import EmailMessage
import json
import os
from pathlib import Path
import smtplib
import socket

from canvas_utils_exceptions import TableExportError
from config_info import ConfigInfo
from utilities import Utilities


class SanityChecker(object):
    '''
    Checks superficially whether aux table export 
    runs succeeded. 
       o Ensures that all tables have a .csv file in 
         the table copy dir, and that 
       o none of those have less data than last time
       
    Maintains a json file Data/typical_table_csv_file_sizes.json.
    If a new table is added to the Queries subdir, this JSON is
    updated.
    '''
    
    # The machine where table refreshes usually run.
    # An SMTP server is available there:
    DEVMACHINE_HOSTNAME = 'dmrapptooldev71.stanford.edu'
    
    #-------------------------
    # constructor 
    #--------------

    def __init__(self, unittest=False):
        '''
        Constructor
        
        @param unittest: set to True if caller is a unittest
        @type unittest: bool
        '''
        self.utils = Utilities()
        self.config_info = ConfigInfo()
        self.admin_email_recipient = self.config_info.admin_email_recipient
        
        
        self.curr_dir = os.path.dirname(__file__)
        
        # Path to json of 'reasonable' table file lengths
        # to expect when tables are exported:
        self.resonable_file_sizes_path = os.path.join(self.curr_dir, 'Data', 'typical_table_csv_file_sizes.json')
        
        HOME = os.getenv('HOME')
        self.table_export_dir_path = f"{HOME}/CanvasTableCopies"
        if unittest:
            # Let unittests redefine table_export_dir and do their thing:
            return
        self.init_table_vars()
        
        detected_errors = []
        try:
            self.check_num_files()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            detected_errors.append(e)
            
        try:
            self.check_exported_file_lengths()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            detected_errors.append(e)
            
        if len(detected_errors) > 0:
            # If we are running on the dev machine,
            # we can send email, b/c it has an SMTP service:
            self.maybe_send_mail(detected_errors)
    
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
    # check_exported_file_lengths 
    #--------------

    def check_exported_file_lengths(self):
        '''
        Ensure none of the export files is
        less than what is stated as expected in file
        Data/typical_table_csv_file_sizes.json. We
        assume that the content of this file is available
        in self.putative_file_sizes_dict.
        
        If we find a new table, one that is not represented
        in the typical_table_csv_file_sizes.json file, we
        add the table's current file size as the desirable one,
        unless it's zero.
        
        @return: True if all is well.
        @rtype: bool
        @raise TableExportError: if missing tables are found. 
            First, collects all missing tables. Then raises
            the error, including the list of missing tables 
            in the exception's table_list property.
        '''
        shrunken_tables  = []
        for table_name in self.all_tables:
            file_path  = os.path.join(self.table_export_dir_path, table_name + '.csv')
            try:
                file_stats = os.stat(file_path)
            except IOError:
                file_len = 0
            else:
                file_len   = file_stats.st_size
            try:
                expected_minimal_file_len = self.putative_file_sizes_dict[table_name]
            except KeyError:
                # New table that is not yet represented in file
                # typical_table_csv_file_sizes.json:
                self.putative_file_sizes_dict[table_name] = file_len
                self.update_reasonable_file_sizes(self.putative_file_sizes_dict)
                expected_minimal_file_len = file_len
                 
            if (file_len < expected_minimal_file_len) or (file_len == 0):
                shrunken_tables.append(table_name)
                
            # If new file is larger than excpected, update the 
            # expected file size in the json file:
            if file_len > expected_minimal_file_len:
                self.putative_file_sizes_dict[table_name] = file_len
                self.update_reasonable_file_sizes(self.putative_file_sizes_dict)
                
        if len(shrunken_tables) > 0:
            raise TableExportError("Table(s) exports abnormally small", shrunken_tables)

        return True

    #-------------------------
    # update_reasonable_file_sizes 
    #--------------
    
    def update_reasonable_file_sizes(self, file_size_dict):
        '''
        Given a dict of table-->exprected-csv-file-size, save
        the dict as JSON in self.reasonable_file_sizes_path.
        
        @param file_size_dict: mapping of table names to expected csv file size
        @type file_size_dict: {src : int}
        '''
        with open(self.resonable_file_sizes_path, 'w') as fd:
            json.dump(file_size_dict, fd)
        
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
        
        # Initialize the 'reasonable' file lengths for each
        # table from the pickled dict:
        
        with open(self.resonable_file_sizes_path, 'r') as fd:
            self.putative_file_sizes_dict = json.load(fd)

    #-------------------------
    # maybe_send_mail 
    #--------------
    
    def maybe_send_mail(self, error_list):
        '''
        If an SMTP server is available on the host
        we are running on, then construct a composite
        of all error messages, and email it to someone.
        
        @param error_list: list of error objects to report
        @type error_list: [TableExportError]
        @return: True if an email was sent, else False
        @rtype: bool
        '''
        if socket.gethostname() != SanityChecker.DEVMACHINE_HOSTNAME:
            return False

        content = ""        
        for err in error_list:
            content += f"{err.message} ({err.table_list})"

        msg = EmailMessage()
        
        msg.set_content(content)
        
        me = "AuxTableRefreshProcess@stanford.edu"
        you = self.admin_email_recipient
        msg['Subject'] = "Error report from aux table refresh :-("
        msg['From'] = me
        msg['To'] = you
        # Send the message via our own SMTP server.
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
        
        return True
        


# -------------------------- Main ------------------
if __name__ == '__main__':
    
    SanityChecker()