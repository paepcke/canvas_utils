'''
Created on Aug 24, 2019

@author: paepcke
'''
import datetime
import getpass
import logging
import os
import re

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

    def get_db_pwd(self, host):
        '''
        Find appropriate password for logging into MySQL. Normally
        a file is expected in CanvasPrep.canvas_pwd_file, and
        the pwd is taken from there.
        
        Password convention is different from 
        normal operation: If passed-in pwd is None
        and host is localhost, we assume that there
        is a user 'unittest' without a pwd.
        
        @param host: name of server where MySQL service resides
        @type host: str
        '''
        
        if host == 'localhost':
            return ''
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', self.config_info.canvas_pwd_file)
            if os.path.exists(default_pwd_file):
                with open(default_pwd_file, 'r') as fd:
                    pwd = fd.readline().strip()
                    return pwd
            
        # Ask on console:
        pwd = getpass.getpass("Password for Canvas database: ")
        return pwd

    #-------------------------
    # log_into_mysql 
    #--------------
            
    def log_into_mysql(self, user, pwd, db=None, host='localhost'):
        
        try:
            # Try logging in, specifying the database in which all the tables
            # will be created: 
            db = MySQLDB(user=user, passwd=pwd, db=db, host=host)
        except ValueError as e:
            # Does the db not exist yet?
            if str(e).find("OperationalError(1049,") > -1:
                # Log in without specifying a db to 'use':
                db =  MySQLDB(user=user, passwd=pwd, db=db, host=host)
                # Create the db:
                db.execute('CREATE DATABASE %s;' % self.config_info.canvas_db_aux)
            else:
                raise DatabaseError("Cannot open Canvas database: %s" % repr(e))
        except Exception as e:
            raise DatabaseError("Cannot open Canvas database: %s" % repr(e))
        
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
        formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")
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
        self.logger.error(msg)
        