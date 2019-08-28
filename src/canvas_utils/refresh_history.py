#!/usr/bin/env python
'''
Created on Aug 24, 2019

@author: paepcke
'''
from _collections import OrderedDict
import argparse
from datetime import timezone
import os
import sys

from pymysql_utils.pymysql_utils import Cursors

from config_info import ConfigInfo
from utilities import Utilities


class LoadHistoryLister(object):
    '''
    Reads table LoadLog. Lists date of latest
    refresh for each table. Lists missing tables,
    and list of all tables.
    '''

    load_table_name = 'LoadLog'
    
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, latest_only=False, unittests=False):
        '''
        Constructor
        '''
        config_info = ConfigInfo()
        self.utils  = Utilities()
        
        # For convenience:
        self.load_table_name = LoadHistoryLister.load_table_name
        if unittests:
            self.aux_db = 'Unittest'
        else:
            self.aux_db = config_info.canvas_db_aux
        
        # Get results as dictionaries:
        if unittests:
            self.db_obj = self.utils.log_into_mysql(config_info.test_default_user,
                                                    self.utils.get_db_pwd(config_info.test_default_host,
                                                                          unittests=unittests),
                                                    db=self.aux_db,
                                                    host=config_info.test_default_host,
                                                    cursor_class = Cursors.DICT
                                                    )
            # Let unittests call methods on their own:
            return
        else:
            self.db_obj = self.utils.log_into_mysql(config_info.default_user,
                                                    self.utils.get_db_pwd(config_info.default_host,
                                                                          unittests=unittests),
                                                    db=config_info.canvas_db_aux,
                                                    host=config_info.default_host,
                                                    cursor_class = Cursors.DICT
                                                    )
            
        try:
            success = self.print_latest_refresh(latest_only)
            if success:
                self.print_missing_tables()
            # self.backup_availability()
        finally:
            self.db_obj.close()
        
    #-------------------------
    # print_latest_refresh 
    #--------------
    
    def print_latest_refresh(self, 
                             latest_only=False, 
                             out_fd=sys.stdout, 
                             load_log_content=None):
        '''
        Pretty print a list of aux tables that exist in 
        the database.
        
        @param latest_only: if True, only the most recent refresh
            event for each table will be shown.
        @type latest_only: bool
        @param out_fd: if provided, a file-like object to which
            output is written. Default: stdout. Used by unittests,
            but could also be used to write the report to a file.
        @type out_fd: file-like
        @param load_log_content: a list of dicts reflecting the content
            of the LoadLog table. Only used by unittests!
        @type load_log_content: [{}]
        @return: True for success, False for failure
        @rtype: bool
        '''
        
        try:
            # Only read content of LoadLog table if
            # unittests did not pass in their own in
            # the call:
            # Result will be:
            #  [{tbl_name : <str>, num_rows : <int>, time_refreshed : datetime},
            #   {tbl_name : <str>, num_rows : <int>, time_refreshed : datetime},
            #        ..
            #  ]
            if load_log_content is None:
                load_log_content = self.db_obj.query(f"SELECT * FROM {self.aux_db}.{self.load_table_name}")
        except ValueError as e:
            out_fd.write(f"Cannot list tables: {repr(e)}\n")
            return False
            
        # Pull all row-dicts out from the query result:
        tbl_dicts   = [tbl_dict for tbl_dict in load_log_content]
        
        # Sort the dicts by table name:
        sorted_tbl_dicts = sorted(tbl_dicts, key=lambda one_dict: one_dict['tbl_name'])
        
        out_fd.write(f"\nAux tables in {self.aux_db}:\n\n")
        
        tbl_nm_header    = 'Table Name'
        load_time_header = 'Last Refreshed'
        num_rows_header  = 'Num Rows'
        # Print the header:
        out_fd.write(f'{tbl_nm_header:>30} {load_time_header:^25} {num_rows_header:^5}\n')

        # If requested, only show the latest update
        # for each table:
        
        if latest_only:
            sorted_tbl_dicts = self.keep_latest_dict(sorted_tbl_dicts)
        
        # For each result dict, pull out the table name,
        # time refreshed, and number of rows. Assign them
        # to variables:
        
        for tbl_entry_dict in sorted_tbl_dicts:
            tbl_nm       = tbl_entry_dict['tbl_name']
            num_rows     = tbl_entry_dict['num_rows']
            
            # Get a UTC datetime obj (b/c we initialize
            # each MySQL session to be UTC):
            utc_load_datetime = tbl_entry_dict['time_refreshed']
            
            # Tell this 'unaware' datetime obj that it'
            tz_aware_load_datetime = utc_load_datetime.replace(tzinfo=timezone.utc)
            localized_datetime = tz_aware_load_datetime.astimezone(tz=None)
            load_time_str = localized_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

            # The ':>30' is "right-justfy; allow 30 chars.
            # The '^20'  is "center-justfy; allow 20 chars.
            out_fd.write(f"{tbl_nm:>30}   {load_time_str:^25} {num_rows:^5}\n")
          
        return True
      
    #-------------------------
    # keep_latest_dict
    #--------------
    
    def keep_latest_dict(self, load_event_dicts):
        '''
        Given a list of dicts with table-name, load-date,
        and row num keys, return a new list with only the
        dicts that describe the most recent table refresh.
        
        @param load_event_dicts: array of dict describing table
            refresh events.
        @type load_event_dicts: [{}]
        '''
        # Dict {tbl_name : load_event_dict} to hold
        # the most recent dict for the respective table.
        # Use an ordered dict to not mess up order of
        # passed-in dicts:
         
        latest_dicts = OrderedDict()
        for load_event_dict in load_event_dicts:
            tbl_nm = load_event_dict['tbl_name']
            try:
                if load_event_dict['time_refreshed'] > latest_dicts[tbl_nm]['time_refreshed']:
                    latest_dicts[tbl_nm] = load_event_dict
            except KeyError:
                # First time we see an entry for this table:
                latest_dicts[tbl_nm] = load_event_dict
        
        res = [newest_refresh_dict for newest_refresh_dict in latest_dicts.values()]
        return res        
      
    #-------------------------
    # print_missing_tables 
    #--------------

    def print_missing_tables(self, num_cols=4):
        '''
        Print the tables that are missing in the 
        aux tables database. Print in column form,
        alpha sorted.
        
        @param num_cols: number of table names in one row
        @type num_cols: int
        @return: True for success, False for failure
        @rtype: bool
        '''

        all_tables     = set(self.utils.create_table_name_array())
        tables_present = self.utils.get_tbl_names_in_schema(self.db_obj, self.aux_db)
        tables_present = set([table_dict['TABLE_NAME'] for table_dict in tables_present])

        missing_tables = all_tables - tables_present
        if len(missing_tables) == 0:
            print("No missing tables.")
            return True

        self.utils.print_columns(missing_tables, 'Missing Tables:', num_cols=num_cols, alpha=True)        
         
        return True    
# ----------------------- Utilities ---------------

# ----------------------- Main -------------
        
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="List aux table load histories, and missing tables."
                                     )

    parser.add_argument('-l', '--latest',
                        help='list only the most recent load event for each table',
                        action='store_true',
                        default=False);
    args = parser.parse_args();

    try:    
        LoadHistoryLister(args.latest)
    except KeyboardInterrupt:
        print("\nLoad history listing stopped by user.")    
         
    #LoadHistoryLister(unittests=True) 
        
