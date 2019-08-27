#!/usr/bin/env python
'''
Created on Aug 24, 2019

@author: paepcke
'''
from datetime import timezone
import math
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

    def __init__(self, unittests=False):
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
                                                                          unittests),
                                                    db=self.aux_db,
                                                    host=config_info.test_default_host,
                                                    cursor_class = Cursors.DICT
                                                    )
        else:
            self.db_obj = self.utils.log_into_mysql(config_info.default_user,
                                                    self.utils.get_db_pwd(config_info.default_host,
                                                                          unittests),
                                                    db=config_info.canvas_db_aux,
                                                    host=config_info.default_host,
                                                    cursor_class = Cursors.DICT
                                                    )
            
        try:
            success = self.print_latest_refresh()
            if success:
                self.print_missing_tables()
            # self.backup_availability()
        finally:
            self.db_obj.close()
        
    #-------------------------
    # print_latest_refresh 
    #--------------
    
    def print_latest_refresh(self):
        '''
        Pretty print a list of aux tables that exist in 
        the database.
        
        @return: True for success, False for failure
        @rtype: bool
        '''
        
        try:
            tbl_content = self.db_obj.query(f"SELECT * FROM {self.aux_db}.{self.load_table_name}")
        except ValueError as e:
            print(f"Cannot list tables: {repr(e)}")
            return False
            
        # Pull all row-dicts out from the query result:
        tbl_dicts   = [tbl_dict for tbl_dict in tbl_content]
        
        # Sort the dicts by table name:
        sorted_tbl_dicts = sorted(tbl_dicts, key=lambda one_dict: one_dict['tbl_name'])
        
        print(f"\nAux tables in {self.aux_db}:\n")
        
        tbl_nm_header    = 'Table Name'
        load_time_header = 'Last Refreshed'
        num_rows_header  = 'Num Rows'
        # Print the header:
        print(f'{tbl_nm_header:>30} {load_time_header:^25} {num_rows_header:^5}')
        
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
            print(f"{tbl_nm:>30}   {load_time_str:^25} {num_rows:^5}")
          
        return True
      
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
        
        sorted_missing_tables = sorted(missing_tables)
        
        # Want to list in col_num columns, with alpha
        # order going down columns. Ex. For tables 
        # named 1,2,...,0, want to print:
        #
        #   1  5   9  13
        #   2  6  10  14 
        #   3  7  11  15
        #   4  8  12  16
        
        # Number of needed rows; the ceiling is for
        # the last row, which may not be all filled:
        
        num_rows = math.ceil(len(sorted_missing_tables) / num_cols)

        # Get chunks of sorted table names 
        # with chunk size being the number
        # of rows we'll have. For 22 elements:
        #
        # cols_matrix = [[ 1, 2, 3, 4, 5, 6,],
        #                [ 7, 8, 9,10,11,12,],
        #                [13,14,15,16,17,18],
        #                [19,20,21,22]
        #               ]

        rows_it      = self.list_chopper(sorted_missing_tables, num_rows)
        cols_matrix  = [cols for cols in rows_it]

        # Ensure the last row is either full, or
        # filled with a space string in each empty
        # column:
        cols_matrix[-1] = self.fill_list_with_spaces(cols_matrix[-1], 
                                                     num_rows)
        # Make transpose to get lists of table names
        # to print on one line:
        
        print_matrix = []
        print("Missing tables: \n")
        for i in range(num_rows):
            print_row = []
            for j in range(num_cols):
                print_row.append(cols_matrix[j][i])
            print_matrix.append(print_row)

        # Build print strings in nicely arranged column:
        for print_row in print_matrix:
            tabular_print_str = ''
            for table_name in print_row:
                tabular_print_str += f"{table_name:<23}"
            # Print one line:
            print(tabular_print_str)
         
        return True    
# ----------------------- Utilities ---------------

    #-------------------------
    # fill_list_with_spaces 
    #--------------

    def fill_list_with_spaces(self, the_list, desired_width):
        if len(the_list) >= desired_width:
            return the_list
        # Have a short list.
        new_list = the_list.copy()
        # Make sure we are working with strings:
        new_list = [str(el) for el in new_list]
        for _i in range(desired_width - len(the_list)):
            new_list.append(' ')
        return new_list

    #-------------------------
    # list_chopper 
    #--------------
    
    def list_chopper(self, list_to_chop, chop_size):
        '''
        Iterator that returns one list of chop_size
        elements of list_to_chop at a time. 
        
        @param list_to_chop: list whose elements are to be delivered
            in chop_size chunks
        @type list_to_chop: [<any>]
        '''
        for i in range(0, len(list_to_chop), chop_size):
            yield list_to_chop[i:i + chop_size]        
        


# ----------------------- Main -------------
        
if __name__ == '__main__':
    
    usage = "Lists the available aux tables, and the ones that are missing. No options."
    if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
        print(usage)
        sys.exit(1)
    LoadHistoryLister() 
    #LoadHistoryLister(unittests=True) 
        