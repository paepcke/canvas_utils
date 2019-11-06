#!/usr/bin/env python
'''
Created on Sep 12, 2019

@author: paepcke
'''
from datetime import datetime
from email.message import EmailMessage
from enum import Enum
import json
import os
from pathlib import Path
import re
import smtplib
import socket
import sys

from canvas_utils_exceptions import TableExportError, DatabaseError
from config_info import ConfigInfo
from utilities import Utilities


class EmailReason(Enum):
        HAPPY = 0
        SAD   = 1

class SanityChecker(object):
    '''
    Checks superficially whether aux table export 
    runs succeeded. 
       o Ensures that all tables have a .tsv file in 
         the table copy dir, and that 
       o none of those have less data than last time
       
    Maintains a json file Data/typical_table_tsv_file_sizes.json.
    If a new table is added to the Queries subdir, this JSON is
    updated.
    '''
    
    # The machine where table refreshes usually run.
    # An SMTP server is available there:
    DEVMACHINE_HOSTNAME = 'dmrapptooldev71.stanford.edu'
    CRONLOG_DIR         = Path(Path.home(), 'cronlogs')
    
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
        self.reasonable_file_sizes_path = os.path.join(self.curr_dir, 'Data', 'typical_table_tsv_file_sizes.json')
        
        self.table_export_dir_path = self.config_info.oracle_tbl_dest_dir
        if unittest:
            # Let unittests redefine table_export_dir and do their thing:
            return
        self.init_table_vars()
        
        detected_errors = []
        try:
            self.check_num_files()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            # This error will be picked up in the cronlog analysis:
            # detected_errors.append(e)
            
        try:
            self.check_exported_file_lengths()
        except TableExportError as e:
            print(f"*****ERROR: {e.message} ({e.table_list})")
            # This error will be picked up in the cronlog analysis:
            # detected_errors.append(e)
                    
        # Check latest cronlog for errors:
        cronlog_error_lines = self.check_cronlog_errors()
        if cronlog_error_lines is not None:
            msg = f"Error(s) in cronlog: {''.join(cronlog_error_lines)} ({str(self.get_latest_cronlog())})"
            detected_errors.append(DatabaseError(msg))
          
        if len(detected_errors) > 0:
            # If we are running on the dev machine,
            # we can send email, b/c it has an SMTP service:
            self.maybe_send_mail(detected_errors, reason=EmailReason.SAD)
            sys.exit(1)
    
        else:
            # Send an OK msg:
            time_now = datetime.now().isoformat()
            self.maybe_send_mail(f"Ran fine; check time {time_now}", reason=EmailReason.HAPPY)
            sys.exit(0)
            
    #-------------------------
    # check_num_files
    #--------------
        
    def check_num_files(self):
        '''
        Return True if there is one TableName.tsv file
        in the table copy directory as there are table
        definitions in the Queries subdir. I missing tables,
        they are printed in an error msg, and False is returned.
        
        @return: True if all goes well. Else throws error.
        @rtype: bool
        @raise TableExportError: if not all tables are present. The 
            list of missing tables will be in the exceptions tables_list
            property
        '''
        # Must have at least the aux tables as .tsv files 
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
        Ensures that all the exported .tsv tables are reasonably
        fresh. Only produces a warning: if any table is older
        by more than 24 hours than the most reacent table. 
        
        @param time_threshold: number of hours within which .tsv
            file exports need to have been created to avoid a 
            warning message
        @type time_threshold: int
        @return: list of table names whose .tsv file is older
            than time_threshold hours than the most recent table
            .tsv file.
        '''
        # Time threshold in seconds:
        secs_threshold = 3600 * time_threshold
        
        # Create dict: Table name==>.tsv-file-date
        tbl_age_dict = {}
        for tbl_nm in self.copied_tables_set:
            tbl_path = os.path.join(self.table_export_dir_path, tbl_nm + '.tsv')
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
        Data/typical_table_tsv_file_sizes.json. We
        assume that the content of this file is available
        in self.putative_file_sizes_dict.
        
        If we find a new table, one that is not represented
        in the typical_table_tsv_file_sizes.json file, we
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
            file_path  = os.path.join(self.table_export_dir_path, table_name + '.tsv')
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
                # typical_table_tsv_file_sizes.json:
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
    # check_cronlog_errors
    #--------------

    def check_cronlog_errors(self):
        '''
        Finds latest cronlog, and searches is for string 'ERROR'
        Collects those lines, and returns them as a list if any
        are found, else returns None
        
        @return: list of lines containing str 'ERROR' from most recent cronlog,
            or None
        @rtype: {None | [str]}
        '''
        
        latest_log_path_obj = self.get_latest_cronlog()
        error_lines = []
        
        with open(latest_log_path_obj, 'r') as fd:
            for line in fd:
                if re.findall(r'ERROR', line):
                    error_lines.append(line)

        return None if len(error_lines) == 0 else error_lines

    #-------------------------
    # update_reasonable_file_sizes 
    #--------------
    
    def update_reasonable_file_sizes(self, file_size_dict):
        '''
        Given a dict of table-->exprected-csv-file-size, save
        the dict as JSON in self.reasonable_file_sizes_path.
        
        @param file_size_dict: mapping of table names to expected tsv file size
        @type file_size_dict: {src : int}
        '''
        with open(self.reasonable_file_sizes_path, 'w') as fd:
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
        tsv_files = filter(lambda file_name: file_name.endswith('.tsv'), all_copied_table_files)
        # Get the table names:
        self.copied_tables = [Path(file_name).stem for file_name in tsv_files]

        self.all_tables_set = set(self.all_tables)
        self.copied_tables_set   = set(self.copied_tables)
        
        # Initialize the 'reasonable' file lengths for each
        # table from the json file:
        
        with open(self.reasonable_file_sizes_path, 'r') as fd:
            self.putative_file_sizes_dict = json.load(fd)

    #-------------------------
    # maybe_send_mail 
    #--------------
    
    def maybe_send_mail(self, error_list, reason=EmailReason.SAD):
        '''
        If an SMTP server is available on the host
        we are running on, then construct a composite
        of all error messages, and email it to someone.
        
        Depending on the 'reason' parm, subject line
        will indicate success or failure.
        
        @param error_list: list of error objects to report, or
            a single string with a message about success outcome.
        @type error_list: { str | [TableExportError]}
        @param reason: whether or not the email is for a happy or 
            sad occasion
        @type reason: EmailReason
        @return: True if an email was sent, else False
        @rtype: bool
        '''
        if socket.gethostname() != SanityChecker.DEVMACHINE_HOSTNAME:
            return False

        content = ""
        if isinstance(error_list, str):
            content = error_list
        else:      
            for err in error_list:
                if isinstance(err, TableExportError):
                    content += f"{err.message} ({err.table_list})\n"
                else:
                    content += f"{err.message}\n"

        msg = EmailMessage()
        
        msg.set_content(content)
        
        me = "AuxTableRefreshProcess@stanford.edu"
        you = self.admin_email_recipient
        msg['Subject'] = "Report from aux table refresh " + ':-(' if reason == EmailReason.SAD else ':-)'
        msg['From'] = me
        msg['To'] = you
        # Send the message via our own SMTP server.
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
        
        return True


    #-------------------------
    # get_latest_cronlog 
    #--------------

    def get_latest_cronlog(self):
        '''
        Return a Path obj for the most recent cronlog
        file. The cronlog directory is taken from SanityChecker.CRONLOG_DIR
        
        @return: path to most recent cronlog
        @rtype: Path
        '''
        log_path_objs = [file_path_obj for file_path_obj in SanityChecker.CRONLOG_DIR.iterdir() if file_path_obj.suffix == '.log']
        # Find the most recent:
        log_path_objs_latest_last = sorted(log_path_objs, key=lambda log_path_obj:log_path_obj.stat().st_ctime)
        latest_log_path_obj = log_path_objs_latest_last[-1]
        return latest_log_path_obj

        


# -------------------------- Main ------------------
if __name__ == '__main__':
    
    if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
        print("Performs tests to see whether the most recent\n" + 
              "aux table pull succeed; sends email w/ result,\n" +
              "and prints to console.")
        sys.exit()
    
    SanityChecker()
