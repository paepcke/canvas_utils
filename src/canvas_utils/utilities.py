'''
Created on Aug 24, 2019

@author: paepcke
'''
import datetime
import getpass
import logging
import math
import os
import re
import shutil
import socket
import sys

from pymysql_utils.pymysql_utils import MySQLDB

from canvas_utils_exceptions import DatabaseError
from config_info import ConfigInfo
from query_sorter import QuerySorter


class Utilities(object):
    '''
    classdocs
    '''
    # datetime format used for appending to table names
    # for backups:
    datetime_format              = '%Y_%m_%d_%H_%M_%S_%f'
    datetime_format_no_subsecond = '%Y_%m_%d_%H_%M_%S'

    # Recognize: '2019_11_02_11_02_03'
    #        or: '2019_11_02_11_02_03_1234':
    datetime_regx = '[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}[_]{0,1}[0-9]*$'
    
    datetime_pat = None # Will be set in __init__() to re.compile(CanvasPrep.datetime_regx)


    def __init__(self):
        '''
        Constructor
        '''
        self.tables = None
        
        # Get all default values:
        self.config_info = ConfigInfo()

        # Regex-parsing date-time strings used to name 
        # backup tables.
        Utilities.datetime_pat = re.compile(Utilities.datetime_regx)

        # Get list of tables in an order in which
        # they can be loaded:
        self.tables = self.create_table_name_array()

    #-------------------------
    # tables setting properties 
    #--------------
    
    @property
    def tables(self):
        return self._table_arr

    @tables.setter
    def tables(self, table_arr):
        self._table_arr = table_arr
        
    #-------------------------
    # get_existing_tables_in_dir 
    #--------------
    
    def get_existing_tables_in_dir(self, db_obj, return_all=False, target_db=None):
        '''
        Returns list of auxiliary canvas table names that are
        already in the target db for auxiliary tables. So: even
        if tables other than the ones listed in CanvasPrep.tables
        exist in the target db, only members of CanvasPrep.tables
        are considered. Others are ignored.
        
        @param db_obj: db instance to use when looking for tables
        @type db_obj: pymysql_utils.MySQLDB
        @param return_all: if true, return_all tables in CanvasPrep.canvas_db_aux
                Else only the ones listed in CanvasPrep.tables.
        @type return_all: bool
        @param target_db: if provided, tables in the given schema
              are returned. Else table CanvasPrep.canvas_db_aux is
              assumed
        @type target_db: str
        @return: list of tables existing in target db
        @rtype: [str]
        '''

        if target_db is None:
            target_db = self.config_info.canvas_db_aux
        
        tbl_names_query = f'''
                          SELECT table_name 
                            FROM information_schema.tables 
                           WHERE table_schema = '{target_db}';
                         '''
        # Pull all table names from the result iterator:
        table_names = [tbl_nm for tbl_nm in db_obj.query(tbl_names_query)]
        
        if return_all:
            return table_names
        
        # Some versions of MySQL mess with case, so 
        # normalize our table list for the purpose of 
        # comparing with tables found:
        
        all_relevant_tbls = [tbl_nm.lower() for tbl_nm in self.tables]
        
        existing_tbls = []
        for tbl_name in table_names:
            if tbl_name.lower() in all_relevant_tbls:
                existing_tbls.append(tbl_name)
                
        return existing_tbls

    #------------------------------------
    # backup_table_name_components
    #-------------------
    
    def backup_table_name_components(self, backup_tbl_name):
        '''
        Given a backup table name, return the root table
        name, the datetime string, and a datetime object from
        that string.

            input:  foo_2019_05_09_11_51_03
            return: ('foo', '2019_05_09_11_51_03', datetime.datetime(2019, 5, 9, 11, 51, 3)
        
        @param table_backup_name: backup table name created by get_backup_table_name()
        @type table_backup_name: str
        @return: table name, datetime string, and datetime object
        @rtype: (str, str, datetime.datetime)
        @raise ValueError: if table_backup_name is ill-formed.
        '''
        try:
            # Length of a backup table datetime string is
            # 19: 'foo_bar_2019_09_03_23_04_10' without the
            # underscore that separates the table name from
            # the datetime. First ensure that the end of the
            # table name is a proper datetime string:
            try:
                # Construct regexp to find <aux_table>_<datestr>.
                # Will find: 'Terms_2019_08_10_09_01_08_387662',
                # but not:   'Foo_2019_08_10_09_01_08_387662'.
                # The regex will look like "Terms|Courses|..._<regex for datetime strings>"
                
                aux_backup_tbl_regex = f"({'|'.join(self.tables)})_{Utilities.datetime_regx}"
                
                # The findall returns an empty array if no match is found. Else
                # it will return an array with the table name. E.g. ['Terms']:
                
                tbl_name = re.findall(aux_backup_tbl_regex, backup_tbl_name)[0]
            except Exception as _e:
                raise ValueError(f"Table name {backup_tbl_name} is not a valid backup name.")
            
                
            # Get a datetime obj from just the backup name's date string part:
            dt_str_part_match_obj = re.search(Utilities.datetime_regx, backup_tbl_name)
            if dt_str_part_match_obj is None:
                # Datetime matches the aux_backup_tbl_regex, but is still not a proper datetime:
                raise ValueError(f"Table name {backup_tbl_name} is not a valid backup name.")
                
            # The date str part of the backup name is between the match's start and end: 
            dt_str = backup_tbl_name[dt_str_part_match_obj.start():dt_str_part_match_obj.end()]
            try:
                dt_obj = datetime.datetime.strptime(dt_str, Utilities.datetime_format)
            except ValueError:
                # Maybe the date string had no sub-second component? Try
                # again to get a datatime obj using a no-subsecond datetime format.
                # if this fails, it will throw a value error:
                try:
                    dt_obj = datetime.datetime.strptime(dt_str, Utilities.datetime_format_no_subsecond)
                except ValueError:
                    raise ValueError(f"Unrecognized datatime part of name '{backup_tbl_name}'")

            return(tbl_name, dt_str, dt_obj) 
        except IndexError:
            raise ValueError(f"Non-conformant backup table name '{backup_tbl_name}'")
        
    #------------------------------------
    # is_aux_table 
    #-------------------    

    def is_aux_table(self, table_name):
        '''
        Returns true if the given table name is one of the
        aux tables in CanvasPrep.tables.
        
        @param table_name: name of table to check
        @type table_name: str
        @requires: whether given table is one of the aux tables.
        @rtype: bool
        '''
        return table_name in self.tables

    #------------------------------------
    # is_backup_name 
    #-------------------    

    def is_backup_name(self, table_name):
        '''
        If the given table name, is a backup name of an aux table,
        return a triple: the aux table's name, the date-time of
        the backup as a string, and as a datetime object. 
        
        If table_name is not the backup name of an aux table, 
        return False.
        
        @param table_name: name of (potential) aux table backup table
        @type table_name: str
        @return: triplet of the aux table name, the date str, and
            the corresponding datetime object
        @rtype: (str, str, datetime)
        '''

        try:
            (root_name, date_str, datetime_obj) = self.backup_table_name_components(table_name)
        except ValueError:
            # The name is not a legal backup table name:
            return False
        return (root_name, date_str, datetime_obj)
    
    #------------------------------------
    # get_root_name 
    #-------------------    

    def get_root_name(self, table_name):
        '''
        If given table name is a backup name, return
        the original table's name: the root name. If 
        the table name is not a backup name, table_name
        itself is returned.
        
        @param table_name: name to examine
        @type table_name: str
        @return: table root name, or table_name itself
        @rtype: str
        '''
        
        if not self.is_backup_name(table_name):
            return table_name
        
        # Got a backup name:
        (tbl_root, _tbl_dt_str, _tbl_dt_obj) = self.backup_table_name_components(table_name)
        return tbl_root

    #-------------------------
    # get_db_pwd
    #--------------

    def get_db_pwd(self, host, ask_user=False, unittests=False):
        '''
        Find appropriate password for logging into MySQL. Normally
        a file is expected in CanvasPrep.canvas_pwd_file, and
        the pwd is taken from there.
        
        In future we may use the host parameter to
        select different password files in $HOME/.ssh,
        depending on the destination host. Currently
        we always assume the file in setup.cfg under
        canvas_pwd_file.  
        
        Password convention is different from 
        normal operation: for tests on localhost
        we assume that there is a user 'unittest' 
        without a pwd, and access only to database
        Unittest.
        
        @param host: name of server where MySQL service resides
        @type host: str
        @param ask_user: if True, request password on command line
        @type ask_user: bool
        @param unittests: whether or not caller is running
        @type unittests:
            unittests.
        @type: bool
        '''
        
        pwd_cli_req = "Password for Canvas database: "
        if host == 'localhost' and unittests:
            return ''
        
        # Was -p/--password option used on command line?
        if ask_user:
            pwd = getpass.getpass(pwd_cli_req)
            return pwd
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', self.config_info.canvas_pwd_file)
            if os.path.exists(default_pwd_file):
                with open(default_pwd_file, 'r') as fd:
                    pwd = fd.readline().strip()
                    return pwd
            
        # Even though not ask_user, could not get pwd from
        # file in .ssh. So ask on console anyway:
        pwd = getpass.getpass(pwd_cli_req)
        return pwd

    #-------------------------
    # log_into_mysql 
    #--------------
            
    def log_into_mysql(self, user, db_pwd, db=None, host='localhost', **kwargs):
        
        try:
            # Try logging in, specifying the database in which all the tables
            # will be created: 
            db = MySQLDB(user=user, passwd=db_pwd, db=db, host=host, **kwargs)
        except ValueError as e:
            # Does the db not exist yet?
            if str(e).find("OperationalError(1049,") > -1:
                # Log in, specifying an always present db to 'use':
                db =  MySQLDB(user=user, passwd=db_pwd, db='information_schema', host=host)
                # Create the db:
                db.execute('CREATE DATABASE %s;' % self.config_info.canvas_db_aux)
            else:
                raise DatabaseError(f"Cannot open Canvas database:\n{repr(e)}")
        except Exception as e:
            raise DatabaseError(f"Cannot open Canvas database:\n{repr(e)}")
        
        # Work in UTC, b/c default on Mac MySQL 8 is local time,
        # on Centos MySQL 5.7 is UTC; it's a mess:
        
        (err, _warn) = db.execute('SET @@session.time_zone = "+00:00"')
        if err is not None:
            self.log_warn(f"Cannot set session time zone to UTC: {repr(err)}")
        
        return db

    #-------------------------
    # create_table_name_array 
    #--------------
    
    @classmethod
    def create_table_name_array(cls):
        '''
        Collect all file names in the Queries subdirectory.
        Each file name is a table name with '.sql' appended.
        Chop off that extension to get a table name.
        
        Return a list of table names in an order that will 
        guaranteed that earlier tables don't depend on 
        later ones.
        
        @return: list of tables that are created by .sql files in the Query directory
        @rtype: [str]
        '''
        query_sorter = QuerySorter()
        tables = query_sorter.sorted_table_names
        return tables

    #------------------------------------
    # get_tbl_names_in_schema 
    #-------------------    
    
    def get_tbl_names_in_schema(self, db, db_schema_name):
        '''
        Given a db schema ('database name' in MySQL parlance),
        return a list of all tables in that db.
        
        @param db: pymysql_utils database object
        @type db: MySQLDB
        @param db_schema_name: name of MySQL db in which to find tables
        @type db_schema_name: str
        @return: list of table names in the given db and schema
        @rtype: [str]
        '''
        tables_res = db.query(f'''
                              SELECT TABLE_NAME 
                                FROM information_schema.tables 
                               WHERE table_schema = '{db_schema_name}';
                              ''')
        table_names = [res for res in tables_res]
        return table_names        

    #-------------------------
    # ensure_load_log_table_existence 
    #--------------

    def ensure_load_log_table_existence(self, load_log_tbl_nm, db_obj):
        '''
        Ensure that the LoadLog table exists. 
        
        @param load_log_tbl_nm: name of the table holding the load log.
        @type load_log_tbl_nm: str
        @param db_obj: database to use for checking and creating
        @type db_obj: MySQLDB
        '''

        # Does the table exist?

        if self.table_exists(load_log_tbl_nm, db_obj):
            return

        # Log table doesn't exist yet.
        # Create it:
        (err, _warn) = db_obj.execute(f'''CREATE TABLE {load_log_tbl_nm} (
                                         		tbl_name varchar(255),
                                    		    time_refreshed DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                        		num_rows int
                                          )
                                         ''')
        if err is not None:
            raise DatabaseError(f"Cannot create load log table {load_log_tbl_nm}: {repr(err)}")

    #------------------------------------
    # table_exists 
    #-------------------    
        
    def table_exists(self, table_name, db_obj):
        '''
        Returns true if table_name exists in database,
        else false.
        
        @param table_name: name of table to check (no leading data schema name)
        @type table_name: str
        @param db_obj: database object to use
        @type db_obj: MySQLDB
        @return: True/False
        @rtype: bool
        '''
        
        res = db_obj.query(f'''
                             SELECT table_name 
                               FROM information_schema.tables
                              WHERE table_name = '{table_name}'
                                AND table_schema = '{db_obj.dbName()}';
                             '''
                             )
        return res.result_count() > 0
        
    #------------------------------------
    # sort_backup_table_names 
    #-------------------    
    
    def sort_backup_table_names(self, backup_table_names):
        '''
        Given a list of backup table names, return 
        a new list, sorted by date, most recent first
        
        @param backup_table_names: list of table names
        @type backup_table_names: [str]
        @returns: sorted list
        @rtype: [str]
        '''
        
        # Sort the backups by age, most recent first:
        # Method backup_table_name_components() returns a triplet:
        # (root_table_name, datetime_str, datetime_obj). Sort
        # using datetime obj by using sorted()'s 'key' function: 
        backup_table_names_sorted = sorted(backup_table_names,
                                           reverse=True,
                                           key=lambda tbl_name: self.backup_table_name_components(tbl_name)[2])
        return backup_table_names_sorted

    #-------------------------
    # get_mysql_path 
    #--------------

    def get_mysql_path(self):
        '''
        Only relevant if running in Eclipse for debugging. 
        In Eclipse the shell PATH is not available. So 
        subprocess() won't find mysql, even with shell==True.
        
        So: if we are not in Eclipse, we find the mysql path
        with a simple 'which'. Else we guess.
        
        @return: Location of mysql program executable
        @rtype: str
        @raise RuntimeError: if executable is not found.
        '''

        # Eclipse puts extra info into the env:
        eclipse_indicator = os.getenv('XPC_SERVICE_NAME')
        
        # If the indicator is absent, or it doesn't include
        # the eclipse info, then we are not in Eclipse; the usual
        # case, of course:
        
        if eclipse_indicator is None or \
           eclipse_indicator == '0' or \
           eclipse_indicator.find('eclipse') == -1:
            # Not running in Eclipse; use reliable method to find mysql:
            mysql_loc = shutil.which('mysql')
            if mysql_loc is None:
                raise RuntimeError("MySQL client not found on this machine (%s)" % socket.gethostname())
        else:
            # We are in Eclipse:
            possible_paths = ['/usr/local/bin/mysql',
                              '/usr/local/mysql/bin/mysql',
                              '/usr/bin/mysql',
                              '/bin/mysql']
            for path in possible_paths:
                if os.path.exists(path):
                    mysql_loc = path
                    break
            if mysql_loc is None:
                raise RuntimeError("MySQL client not found on this machine (%s)" % socket.gethostname())
        return mysql_loc
    
    #-------------------------
    # print_columns
    #--------------

    def print_columns(self, 
                      tbl_list, 
                      header_str=None, 
                      num_cols=4, 
                      alpha=True,
                      out_fd=sys.stdout):
        '''
        Print the given tables alphabetically in
        columns.
        
        @param tbl_list: list of table names (can be other strings instead)
        @type tbl_list: [str]
        @param header_str: optional string for header of printout
        @type header_str: str
        @param num_cols: number of table names in one row
        @type num_cols: int
        @param alpha: whether or not to sort first
        @type alpha: bool,
        @param out_fd: file-like object to which output is written.
        @type out_fd: file-like
        '''

        if alpha:       
            tbls_print_ordered = sorted(tbl_list)
        else:
            tbls_print_ordered = tbl_list
        
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
        
        num_rows = math.ceil(len(tbls_print_ordered) / num_cols)

        # Get chunks of sorted table names 
        # with chunk size being the number
        # of rows we'll have. For 22 elements:
        #
        # cols_matrix = [[ 1, 2, 3, 4, 5, 6,],
        #                [ 7, 8, 9,10,11,12,],
        #                [13,14,15,16,17,18],
        #                [19,20,21,22]
        #               ]

        rows_it      = self.list_chopper(tbls_print_ordered, num_rows)
        cols_matrix  = [cols for cols in rows_it]

        # Ensure the last row is either full, or
        # filled with a space string in each empty
        # column:
        cols_matrix[-1] = self.fill_list_with_spaces(cols_matrix[-1], 
                                                     num_rows)
        # Make transpose to get lists of table names
        # to print on one line:
        
        print_matrix = []
        if header_str is not None:
            out_fd.write(f"{header_str}\n\n")
        for i in range(num_rows):
            print_row = []
            for j in range(num_cols-1):
                print_row.append(cols_matrix[j][i])
            print_matrix.append(print_row)

        # Build print strings in nicely arranged column:
        for print_row in print_matrix:
            tabular_print_str = ''
            for table_name in print_row:
                tabular_print_str += f"{table_name:<23}"
            # Print one line:
            out_fd.write(tabular_print_str + '\n')

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

        
    # ------------------------ Logging Related Utilities -------------        

    #-------------------------
    # setup_logging 
    #--------------
    
    def setup_logging(self, loggingLevel=logging.INFO, logFile=None):
        '''
        Set up the standard Python logger.

        @param loggingLevel: initial logging level
        @type loggingLevel: {logging.INFO|WARN|ERROR|DEBUG}
        @param logFile: optional file path where to send log entries
        @type logFile: str
        '''

        self.logger = logging.getLogger(os.path.basename(__file__))

        # Create file handler if requested:
        if logFile is not None:
            self.handler = logging.FileHandler(logFile)
            print('Logging of control flow will go to %s' % logFile)
        else:
            # Create console handler:
            self.handler = logging.StreamHandler()
        self.handler.setLevel(loggingLevel)

        # Create formatter
        #formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")
        formatter = logging.Formatter("%(asctime)s;%(levelname)s: %(message)s")
        self.handler.setFormatter(formatter)

        # Add the handler to the logger
        if len(self.logger.handlers) == 0:
            self.logger.addHandler(self.handler)
        self.logger.setLevel(loggingLevel)
        
    #------------------------------------
    # shutdown_logging 
    #-------------------    
        
    def shutdown_logging(self):
        self.logger.removeHandler(self.handler)
        logging.shutdown()

    #-------------------------
    # log_debug/warn/info/err 
    #--------------

    def log_debug(self, msg):
        self.logger.debug(msg)

    def log_warn(self, msg):
        self.logger.warning(msg)

    def log_info(self, msg):
        self.logger.info(msg)

    def log_err(self, msg):
        self.logger.error('*****' + msg)
        