#!/usr/bin/env python
'''
Created on Jan 1, 2019

@author: Andreas Paepcke
'''

import argparse
import configparser
import datetime
import getpass
import glob
import logging
from os import getenv
import os
from pathlib import Path
import pickle
import pwd
import re
import sys

from pymysql_utils.pymysql_utils import MySQLDB

from pull_explore_courses import ECPuller
from query_sorter import QuerySorter

class CanvasPrep(object):
    '''
    Draws on continuously changing data from Canvas.
    Prepares auxiliary tables to facilitate analysis
    and visualizations.
    '''

    # Production server
    # default_host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    
    # Kathy server
    default_host = 'canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com'
    

    default_user = 'canvasdata_prd'
    #default_user = getpass.getuser()
    
    # Name of MySQL canvas data schema (db):
    canvas_db_nm = 'canvasdata_prd'
    
    # Canvas pwd file name:
    canvas_pwd_file = os.path.join(getenv("HOME"), '.ssh', 'canvas_pwd') 
    
    # Name of MySQL db (schema) where new,
    # auxiliary tables will be placed:
    canvas_db_aux = 'canvasdata_aux'

    # Pickle file name of dict with existing
    # tables. Used when only missing or incomplete
    # tables are to be created:
    healthy_tables_dict_file = 'healthy_tables_dict_file.pickle'
    
    # File to which Explore Courses .xml files are written:
    ec_xml_file = 'Data/explore_courses.xml'
     
    
    tables = []
    
    # Paths to the SQL files that do the work.
    # Filled in constructor
    tbl_creation_paths = []
    
    # datetime format used for appending to table names
    # for backups:
    datetime_format              = '%Y_%m_%d_%H_%M_%S_%f'
    datetime_format_no_subsecond = '%Y_%m_%d_%H_%M_%S'
    
    # Recognize: '2019_11_02_11_02_03'
    #        or: '2019_11_02_11_02_03_1234':
    datetime_regx = '[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}[_]{0,1}[0-9]*$'
    
    datetime_pat = None # Will be set in __init__() to re.compile(CanvasPrep.datetime_regx)
    
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 pwd=None, 
                 target_db=None, 
                 host=None,
                 tables=[],
                 new_only=False,
                 skip_backups=False,
                 dryrun=False, 
                 logging_level=logging.INFO,
                 unittests=False):
        '''
        @param user: login user for database
        @type user: str
        @param pwd: password for database
        @type pwd:str
        @param target_db: schema into which to place new tables in target database
        @type target_db: str
        @param host: MySQL host name
        @type host: str
        @param tables: optional list of tables to (re)-create
        @type tables: [str]
        @param new_only: if true, only table that don't already exist are created
        @type new_only: bool
        @param skip_backups: if true, don't backup already existing tables before overwriting them
        @type skip_backups: bool
        @param dryrun: if True, only print what would be done.
        @type dryrun: bool 
        @param logging_level: how much of the run to document
        @type logging_level: logging.INFO/DEBUG/ERROR/...
        @param unittests: set to True to have this instance do 
            nothing but initialization of constants. Used to allow
            unittests to call methods in isolation.
        @type unittests: boolean
        '''
        
        self.unittests = unittests

        # Regex-parsing date-time strings used to name 
        # backup tables.
        CanvasPrep.datetime_pat = re.compile(CanvasPrep.datetime_regx)

        # Under certain conditions __file__ is a relative path.
        # Ensure availability of an absolute path:
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        proj_root_dir = os.path.join(self.curr_dir, '../..')
        
        # Read any configs from the config file, if it exists:
        config_parser = configparser.ConfigParser()
        config_parser.read(os.path.join(proj_root_dir, 'setup.cnf'))

        try:
            CanvasPrep.default_host = config_parser['DATABASE']['default_host']
        except KeyError:
            pass

        try:
            CanvasPrep.canvas_db_aux = config_parser['DATABASE']['canvas_auxiliary_db_name']
        except KeyError:
            pass

        try:
            CanvasPrep.default_user = config_parser['DATABASE']['default_user']
        except KeyError:
            pass
        
        self.new_only = new_only
        self.skip_backups = skip_backups
        self.dryrun = dryrun
        if user is None:
            user = CanvasPrep.default_user

        if host is None:
            host = CanvasPrep.default_host
            
        self.host = host
        
        if pwd is None:
            pwd = self.get_db_pwd()
        
        self.pwd = pwd 
        
        if target_db is None:
            target_db = CanvasPrep.canvas_db_aux
        else:
            target_db = target_db
            
        # If user wants only particular tables to be created
        # then the tables arg will be a list of table names:
        if tables is not None and len(tables) > 0:
            CanvasPrep.tables = tables
        else:
            # No particular tables named on command line,
            # So get all .sql file names in Query directory.
            # Names are the table names.
            CanvasPrep.create_table_name_array()

        self.setup_logging()
        self.logger.setLevel(logging_level)
        
        # Create list of full paths to table
        # creation sql files:
        CanvasPrep.tbl_creation_paths = [self.file_nm_from_tble(tbl_nm) for tbl_nm in CanvasPrep.tables]
        
        # For convenience: figure out where all the
        # update queries, and the file with useful
        # mysql functions and procedures are stored:
        
        self.queries_dir = self.get_queries_dir()
        
        self.log_info('Connecting to db %s@%s.%s...' %\
                      (user, host, CanvasPrep.canvas_db_aux))
                      
        self.db = self.log_into_mysql(user, 
                                      self.pwd, 
                                      db=target_db,
                                      host=host)
        
        self.log_info('Done connecting to db.')
        
        if unittests:
            return
        
        # Used to have tables whose .sql was not self-contained;
        # it needed non-sql activity before or after. We took
        # care of these, but keep this mechanism for special
        # treatment in create_tables() in place for possible
        # future use:
        self.special_tables = {}
        
        # Set parameters such as acceptable datetime formats:
        self.prep_db()
        
    #------------------------------------
    # run 
    #-------------------    
        
    def run(self):
        
        # Determine how many tables need to be done.
        # If self.new_only is true, then only non-existing
        # tables are to be done, else all of them:
        
        existing_tables = self.get_existing_tables()
        
        # Back up tables if:
        #   o any tables already exist in the first place AND
        #   o we are to overwrite existing tables AND
        #   o we were not instructed to back up tables:
        if len(existing_tables) > 0 and not self.new_only and not self.skip_backups:
            if self.dryrun:
                print(f"Would back up tables {existing_tables}")
            else:
                # Backup the tables that are in the db:
                self.backup_tables(existing_tables) 
        
        if self.new_only:
            completed_tables = existing_tables
        else:
            # Just pretend no tables exist:
            completed_tables = []
        
        # Get a fresh copy of the Explore Courses .xml file?
        
        # We are supposed to refresh the ExploreCourses table.
        # Get pull a fresh .xml file, and convert it to .csv:
        if self.dryrun:
            print("Would fetch fresh copy of explore-courses.")
        else:
            self.pull_explore_courses()
            
        # Create the other tables that are needed.
        try:
            completed_tables = self.create_tables(completed_tables=completed_tables)
            
        finally:
            self.close()
        
        if self.dryrun:
            print(f"Would be done creating {len(completed_tables)} tables.")
        else:
            self.log_info(f"(Re)created {len(completed_tables)} tables. Done")

    #------------------------------------
    # close 
    #-------------------    

    def close(self):
        '''
        Close all resources. No need to call from outside. But
        useful during unittests where we reach into unusual spots
        of the code.
        '''
        try:
            self.log_info('Closing db...')
            try:
                self.db.close()
                self.log_info('Done closing db.')    
            except Exception as e:
                self.log_warn(f"Error during database close: {repr(e)}")
        finally:
            self.shutdown_logging()

    #------------------------------------
    # backup_tables 
    #-------------------    

    def backup_tables(self, table_names):
        '''
        Given a list of aux tables, rename them to have current
        datatime appended. Table_names can be a non-list
        single str. The microseconds part of the datetime
        object will be set to 0.
        
        @param table_names: list of aux tables to back up
        @type table_names: [str]
        @return: datetime object whose string representation was
            used to generate the backup table names.
        @rtype: datetime.datetime
        '''
        
        if type(table_names) != list:
            table_names = [table_names]
        
        # Get current-time datetime instance in local 
        # timezone (the 'None' parameter):
        curr_time = datetime.datetime.now()
        
        # Create backup table names:
        tbl_name_map = {}
        
        for tbl_nm in table_names:
            tbl_name_map[tbl_nm] = self.get_backup_table_name(tbl_nm, curr_time)
            
        # SQL lets us rename many table in one cmd:
        #   RENAME TABLE tb1 TO tb2, tb3 TO tb4;
        # Build the "tb_nm TO tb_backup_nm" snippets:
        
        tbl_rename_snippets = [f" {tbl_nm} TO {self.get_backup_table_name(tbl_nm, curr_time)} " 
                               for tbl_nm in table_names]
        
        # Stitch it all together into "RENAME TABLE foo TO foo_backup bar TO bar_backup;"
        # by combining the string snippets with spaces:

        rename_cmd = f"RENAME TABLE {','.join(tbl_rename_snippets)};"

        # Do it!
        
        self.log_info(f"Renaming {len(table_names)} tables to backup names...")
        (errors, _warns) = self.db.execute(rename_cmd, doCommit=False)
        if errors is not None:
            raise RuntimeError(f"Could not rename at least some of tables {str(table_names)}: {repr(errors)}")
            
        self.log_info(f"Done renaming {len(table_names)} tables to backup names.")
        return curr_time
              
    #------------------------------------
    # restore_from_backup 
    #------------------- 
    
    def restore_from_backup(self, table_root_or_backup_names, db_schema=None):
        '''
        Restores backup tables to be the 
        'current' tables. The parameter may be a single
        string or a list of strings. Each string may either
        be a table root name, or an entire backup name.
        
        Example 1 a backup table name: 
            Given Terms_2019_02_10_14_34_10 this method will
               1. Check that Terms_2019_02_10_14_34_10 exiss.
               2. If table Terms exists, drop table Terms
               3. Rename table Terms_2019_02_10_14_34_10 to Terms 
            If Terms_2019_02_10_14_34_10 does not exist: error
        
        Example 2 
            Given name Terms this method will:
               1. Find the most recent Term table backup, 
                  say Terms_2019_02_10_14_34_10.
               2. If no backup is found, nothing is done.
               3. Check whether table Terms exists. If
                  so, Terms is dropped.
               4. Table Terms_2019_02_10_14_34_10 is renamed
                  to 'Terms'.
        
        Backup tables and non-backup tables may be mixed in
        the table_root_or_backup_names list parameter
        
        @param table_root_or_backup_names: list or singleton 
        @type table_root_or_backup_names:
        @param db_schema: the MySQL schema (i.e. database). Default: CanvasPrep.canvas_db_aux
        @type db_schema: str
        '''
    
        if db_schema is None:
            db_schema = CanvasPrep.canvas_db_aux
              
        if type(table_root_or_backup_names) != list:
            table_root_or_backup_names = [table_root_or_backup_names]
        
        for table_name in table_root_or_backup_names:
            
            # Get (aux_tbl_name, data-str, datetime obj) if 
            # table_name is an aux table backup; else get False:
            tbl_nm_components = self.is_backup_name(table_name) 
            if tbl_nm_components:
                
                # Got an explicit backup filename to restore:
                (root_name, _date_str, _datetime_obj) = tbl_nm_components
                
                if not self.table_exists(table_name):
                    raise ValueError(f"Table {table_name} does not exist, so cannot be restored to the working copy.")
                
                self.db.dropTable(root_name)
                self.db.execute(f"RENAME TABLE {table_name} TO {root_name};")
                continue
            elif table_name in CanvasPrep.tables:
                # Table is a root name; find the most recent backup:
                res = self.db.query(f'''
                                     select table_name 
                                       from information_schema.tables
                                      where table_schema = '{db_schema}'
                                        AND table_name like '{table_name}%';   
                                    ''')
                backup_tbl_names = [tbl_name for tbl_name in res if self.is_backup_name(tbl_name)]

                if len(backup_tbl_names) == 0:
                    # No backup table available.
                    self.log_warn(f"No backup table found for table '{table_name}'.")
                    # Next table name to restore:
                    continue
                
                backup_tbl_names = self.sort_backup_table_names(backup_tbl_names)
                
                # Do the rename using the first (and therefore youngest backup 
                # table name:
                self.db.dropTable(table_name)
                self.db.execute(f'''RENAME TABLE {backup_tbl_names[0]} TO {table_name};''')
               
              
    #------------------------------------
    # remove_backup_tables 
    #-------------------
    
    def remove_backup_tables(self, table_root, num_to_keep=0):
        '''
        Removes backup aux tables from the database.
        Only the last num_to_keep are retained, if that many
        exist. Example, suppose table_root is 'Terms', num_to_keep
        is 2, and the following backups exist: Terms_2019_01_10_10_40_03,
        Terms_2019_02_20_14_60_03, and Terms_2019_06_16_22_24_15.
        Then this method will remove Terms_2019_01_10_10_40_03.
        
        @param table_root: name of table whose backups are to be removed 
        @type table_root: str
        @param num_to_keep: number of backups to keep
        @type num_to_keep: int
        @return: number of actually deleted tables
        '''
        
        # Find all backup tables:
        lst_tbl_cmd = f'''
                       SELECT table_name
                         FROM information_schema.tables
                        WHERE table_name REGEXP '{table_root}_{CanvasPrep.datetime_regx}';
                       '''
        tbl_names_res = self.db.query(lst_tbl_cmd)
        backup_table_names = [table_name for table_name in tbl_names_res]
        self.log_info(f"Found {len(backup_table_names)} backed up tables for {table_root}." )
        
        # Sort the backups by age, most recent first:
        # Method backup_table_name_components() returns a triplet:
        # (root_table_name, datetime_str, datetime_obj). Sort
        # using datetime obj by using sorted()'s 'key' function: 
        backup_table_names_sorted = sorted(backup_table_names,
                                           reverse=True,
                                           key=lambda tbl_name: CanvasPrep.backup_table_name_components(tbl_name)[2])
        
        num_deleted = 0
        # Remove tables beyond the desired number of keepers:
        for table_name in backup_table_names_sorted[num_to_keep:]:
            try:
                self.db.dropTable(table_name)
                num_deleted += 1
            except Exception as e:
                self.log_err(f"Could not drop backup table {table_name}: {repr(e)}")
          
        return num_deleted
                                      
    #-------------------------
    #  create_tables
    #--------------
        
    def create_tables(self, completed_tables=[]):
        '''
        Runs through the tl_creation_paths list of table
        creation .sql files, and executes each. 
        
        The completed_tables parameter is list of tables
        that are currently already in the target db. None of
        those tables will be replaced. If all tables are to
        be created, set the parameter to an empty table.
        
        @param completed_tables: dictionary of completed tables
        @type completed_tables: {str : bool}
        @return: a new, or augmented table completion dict
        @rtype: {str : bool}
        '''
        
        for tbl_file_path in CanvasPrep.tbl_creation_paths:
            tbl_nm = self.tbl_nm_from_file(tbl_file_path)
            
            special_handler = self.special_tables.get(tbl_nm, None)
            if special_handler is not None:
                self.handle_complicated_case(tbl_file_path, tbl_nm)
                continue
            
            # Simple case: get query as string:
            with open(tbl_file_path, 'r') as fd:
                query = fd.read().strip()
                
            # The following replacements should be done using 
            # the newer finalize_table module.
            # Replace the placeholder <canvas_db> with the
            # db name of the Canvas product db:
            # [Abandoned using marked up queries as too complex for maintenance]
            
            # query = query.replace('<canvas_db>', CanvasPrep.canvas_db_nm)
            # query = query.replace('<canvas_aux>', CanvasPrep.canvas_db_aux)
            # query = query.replace('<data_dir>', os.path.join(self.curr_dir, 'Data'))
            
            
            if self.dryrun:
                print(f"Would create table {tbl_nm}.")
                completed_tables.append(tbl_nm)
                continue
                      
            self.log_info('Working on table %s...' % tbl_nm)
            (errors, _warns) = self.db.execute(query, doCommit=False)
            if errors is not None:
                raise RuntimeError("Could not create table %s: %s" %\
                                   (tbl_nm, str(errors))
                                   )
            completed_tables.append(tbl_nm)
            self.log_info('Done working on table %s' % tbl_nm)
        return completed_tables
        
    #-------------------------
    # pull_explore_courses 
    #--------------
        
    def pull_explore_courses(self):
        
        # Need absolute path to XML file:
        ec_xml_path = os.path.join(self.curr_dir, CanvasPrep.ec_xml_file)
        
        puller = ECPuller(ec_xml_path,
                          overwrite_existing=True,
                          log_level=self.logger.level,
                          logger=self.logger
                          )
        # Do the retrieval of a new .xml file from
        # the HTTP server:
        puller.pull_ec()
                
        # Convert the .xml to .csv:
        (xml_file_root, _ext) = os.path.splitext(ec_xml_path)
        csv_outfile = xml_file_root + '.csv'
        puller.ec_xml_to_csv(ec_xml_path, csv_outfile)
        
    #-------------------------
    # handle_complicated_case 
    #--------------

    def handle_complicated_case(self, tbl_file_path, tbl_nm):
        # No unusual cases to handle:
        pass

    #-------------------------
    # create_quiz_dim 
    #--------------
    
    def create_quiz_dim(self):
        pass
    
    #-------------------------
    # get_existing_tables 
    #--------------
    
    def get_existing_tables(self, return_all=False, target_db=None):
        '''
        Returns list of auxiliary canvas table names that are
        already in the target db for auxiliary tables. So: even
        if tables other than the ones listed in CanvasPrep.tables
        exist in the target db, only members of CanvasPrep.tables
        are considered. Others are ignored.
        
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
            target_db = CanvasPrep.canvas_db_aux
        
        tbl_names_query = f'''
                          SELECT table_name 
                            FROM information_schema.tables 
                           WHERE table_schema = '{target_db}';
                         '''
        table_names = [tbl_nm for tbl_nm in self.db.query(tbl_names_query)]
        
        if return_all:
            return table_names
        
        # Some versions of MySQL mess with case, so 
        # normalize our table list for the purpose of 
        # comparing with tables found:
        
        all_relevant_tbls = [tbl_nm.lower() for tbl_nm in CanvasPrep.tables]
        
        existing_tbls = []
        for tbl_name in table_names:
            if tbl_name.lower() in all_relevant_tbls:
                existing_tbls.append(tbl_name)
                
        return existing_tbls
        
    #-------------------------
    # get_db_pwd
    #--------------

    def get_db_pwd(self):
        '''
        Find appropriate password for logging into MySQL. Normally
        a file is expected in CanvasPrep.canvas_pwd_file, and
        the pwd is taken from there.
        
        Password convention is different from 
        normal operation: If passed-in pwd is None
        and host is localhost, we assume that there
        is a user 'unittest' without a pwd.
        
        '''
        
        if self.host == 'localhost':
            return ''
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', CanvasPrep.canvas_pwd_file)
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
                db.execute('CREATE DATABASE %s;' % CanvasPrep.canvas_db_aux)
            else:
                raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        except Exception as e:
            raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        
        return db
    
    #-------------------------
    # prep_db 
    #--------------
    
    def prep_db(self):
        
        # All SQL is written assuming MySQL's current db is
        # the one where the new tables will be created:
        self.db.execute('USE %s' % CanvasPrep.canvas_db_aux)
        
        # MySQL 8 started to complain when functions do not 
        # specify DETERMINISTIC or NO_SQL, or one of several other
        # function characteristics. Avoid that complaint:
        
        self.db.execute("SET GLOBAL log_bin_trust_function_creators = 1;")
        
        # At least for MySQL 8.x we need to allow zero dates,
        # like '0000-00-00 00:00:00', which is found in the Canvas db:
        
        self.db.execute('SET sql_mode="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";')
        
        
        # Ensure that all the handy SQL functions are available.
        funcs_file = os.path.join(self.queries_dir, 'mysqlProcAndFuncBodies.sql')
        (errors, _warns) = self.db.execute('SOURCE %s' % funcs_file)
        if errors is not None:
            raise RuntimeError("Could not load MySQL funcs/procedures: %s" % str(errors))
        

    #-------------------------
    # save_table_done_dict 
    #--------------
    
    def save_table_done_dict(self, completed_tables_dict):

        with open(CanvasPrep.healthy_tables_dict_file, 'wb') as fd:                
                        pickle.dump(completed_tables_dict, fd)        
    
    #-------------------------
    # tbl_nm_from_file 
    #--------------
            
    def tbl_nm_from_file(self, tbl_creation_file_nm):
        '''
        Given the name of a .sql file that creates
        one of the aux tables, return the name of the
        table for which the .sql file is the creation
        code.
        
        @param tbl_creation_file_nm: path to .sql file
        @type tbl_creation_file_nm: str
        '''
        (nm_no_ext, _ext) = os.path.splitext(tbl_creation_file_nm)
        return os.path.basename(nm_no_ext)
    
    #-------------------------
    # file_nm_from_tble 
    #--------------
    
    def file_nm_from_tble(self, tbl_nm):
        '''
        Given a table name, return the .sql file name
        in the Queries subdirectory, which creates that
        table.
        
        @param tbl_nm: name of table whose creation-sql is to be found
        @type tbl_nm: str
        '''
        this_dir = self.curr_dir
        return os.path.join(this_dir, 'Queries', tbl_nm) + '.sql'

    #-------------------------
    # create_table_name_array 
    #--------------
    
    @classmethod
    def create_table_name_array(cls):
        '''
        Collect all file names in the Queries subdirectory.
        Each file name is a table name with '.sql' appended.
        Chop off that extension to get a table name.
        
        Initialize CanvasPrep.tables to be a list of all
        table names. Also return that list.
        
        @return: list of tables that are created by .sql files in the Query directory
        @rtype: [str]
        '''
        query_sorter = QuerySorter()
        CanvasPrep.tables = query_sorter.sorted_table_names
        return CanvasPrep.tables
    
    #------------------------------------
    # list_tables 
    #-------------------    

    @classmethod
    def list_tables(cls):
        '''
        List tables that can be created to screen.
        '''
        # Fill the tables array with all tables created
        # in the Queries dir:
        CanvasPrep.create_table_name_array()
        
        print("Tables that can be created:")
        for table in CanvasPrep.tables:
            print(f"{table},")
    
    #-------------------------
    # get_queries_dir 
    #--------------
    
    def get_queries_dir(self):
        
        # All queries are in subdir 'Queries' of
        # this script's directory:
        query_dir = os.path.join(self.curr_dir, 'Queries')
        return query_dir 

    #------------------------------------
    # get_backup_table_name 
    #-------------------    

    def get_backup_table_name(self, table_name, datetime_obj=None):
        '''
        Given a table name, return a new table name that
        has the current datetime added in local timezone. The 
        result string is acceptable as a table name. Example:
           input:  foo
           output: foo_2019_05_09_11_51_03_783456
           
        If datetime_obj is provided, it must be a datetime.datetime
        instance to use when creating the backup table name. 
        Useful if several tables are being backed up one after the
        other, and they are all to have the same datetime stamp.
        The given time is converted to local time.
        
        It is the caller's responsibility to ensure absence of
        name duplicates.
        
        @param table_name: name of table
        @type table_name: str
        @param datetime_obj: a datetime.datetime instance to use in
            the backup name generation
        @type datetime_obj: datetime.datetime
        @return: new table name with date attached
        @rtype: str
        @raise ValueError: if invalid datetime obj.
        '''
        
        if datetime_obj is None:
            # Local time (the 'None'):
            datetime_obj =  datetime.datetime.now().astimezone(None)
        elif type(datetime_obj) != datetime.datetime:
            raise ValueError("If supplied, parameter datetime_obj must be a datetime.datetime instance.")
        else:
            # Make sure we use local time:
            datetime_obj =  datetime_obj.astimezone(None)

        time_now = str(datetime_obj.strftime(CanvasPrep.datetime_format))
        return table_name + '_' + time_now

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
        return table_name in CanvasPrep.tables

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

    #------------------------------------
    # backup_table_name_components
    #-------------------
    
    @classmethod
    def backup_table_name_components(cls, backup_tbl_name):
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
                
                aux_backup_tbl_regex = f"{'|'.join(CanvasPrep.tables)}_{CanvasPrep.datetime_regx}"
                
                # The findall returns an empty array if no match is found. Else
                # it will return an array with the table name. E.g. ['Terms']:
                
                tbl_name = re.findall(aux_backup_tbl_regex, backup_tbl_name)[0]
            except Exception as _e:
                raise ValueError(f"Table name {backup_tbl_name} is not a valid backup name.")
            
                
            # Get a datetime obj from just the backup name's date string part:
            dt_str_part_match_obj = re.search(CanvasPrep.datetime_regx, backup_tbl_name)
            if dt_str_part_match_obj is None:
                # Datetime matches the aux_backup_tbl_regex, but is still not a proper datetime:
                raise ValueError(f"Table name {backup_tbl_name} is not a valid backup name.")
                
            # The date str part of the backup name is between the match's start and end: 
            dt_str = backup_tbl_name[dt_str_part_match_obj.start():dt_str_part_match_obj.end()]
            try:
                dt_obj = datetime.datetime.strptime(dt_str, CanvasPrep.datetime_format)
            except ValueError:
                # Maybe the date string had no sub-second component? Try
                # again to get a datatime obj using a no-subsecond datetime format.
                # if this fails, it will throw a value error:
                try:
                    dt_obj = datetime.datetime.strptime(dt_str, CanvasPrep.datetime_format_no_subsecond)
                except ValueError:
                    raise ValueError(f"Unrecognized datatime part of name '{backup_tbl_name}'")

            return(tbl_name, dt_str, dt_obj) 
        except IndexError:
            raise ValueError(f"Non-conformant backup table name '{backup_tbl_name}'")

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
                                           key=lambda tbl_name: CanvasPrep.backup_table_name_components(tbl_name)[2])
        return backup_table_names_sorted

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
        '''
        tables_res = db.query(f'''
                              SELECT TABLE_NAME 
                                FROM information_schema.tables 
                               WHERE table_schema = '{db_schema_name}';
                              ''')
        table_names = [res for res in tables_res]
        return table_names        

    #------------------------------------
    # find_backup_table_names 
    #-------------------    
        
    def find_backup_table_names(self, table_names, name_root_to_find=None, datetime_obj_used=None):
        '''
        Given a list of table names, return a (sub-)list of
        those names that are backup tables. The names must
        match the precise pattern created by CanvasPrep for
        backups. 
        
        @param table_names: table name list to search
        @type table_names: [str]
        @param name_root_to_find:
        @type name_root_to_find:
        @param name_to_find: table name, if a particular name is to be found
        @type name_to_find: str
        @param datetime_obj_used: a datetime object that was used to create
                the table name to search for in table_names
        @type datetime_obj_used: datetime.datetime
        @return: possibly empty list of backup tables found
        @rtype: [str]
        @raise ValueError: if bad parameters 
        '''

        if name_root_to_find is not None and type(datetime_obj_used) != datetime.datetime:
            raise ValueError(f"If name_root_to_find is non-None, then datetime_obj_used must be a datetime objec.")

        tables_found = []
        # Find the test_table_name with an attached date-time:
        for table_name in table_names:
            # One table name:
            # Try to get a datetime and the original table name root
            # from table_name. This will check whether the table was
            # some pre-existing non-backup table. If table_name is not
            # a backup table, examine the text name in the list:
            try:
                (recovered_table_name, _recovered_dt_str, recovered_dt_obj) =\
                    CanvasPrep.backup_table_name_components(table_name)
                    
                # If we didn't get an error, we found a backup table:
                tables_found.append(table_name)
                    
                if name_root_to_find is not None:
                    # We are searching for a particular backup table name:
                    if recovered_table_name != name_root_to_find or\
                        recovered_dt_obj != datetime_obj_used:
                        # Not the name we are looking for: 
                        continue
                    else:
                        # All good
                        tables_found = [table_name]
                        return tables_found
                    
                else:
                    # We are just to collect all backup tables:
                    continue  # to find more backup tables
                    
            except ValueError:
                # Not a backed-up table; next table name:
                continue
        # Ran through all table names without finding the 
        # backup
        return tables_found
 
        
    #------------------------------------
    # table_exists 
    #-------------------    
        
    def table_exists(self, table_name):
        '''
        Returns true if table_name exists in database,
        else false.
        
        @param table_name: name of table to check (no leading data schema name)
        @type table_name: str
        @return: True/False
        @rtype: bool
        '''
        
        res = self.db.query(f'''
                             SELECT table_name 
                               FROM information_schema.tables
                              WHERE table_name = '{table_name}'
                             '''
                             )
        return res is not None
        
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

# ----------------------------- Main ------------------------

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Create auxiliary canvas tables."
                                     )

    parser.add_argument('-l', '--list',
                        help='list the aux tables known to this program',
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-n', '--newonly',
                        help="only create tables that don't already exist; default: false",
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-s', '--skipbackup',
                        help="don't back up tables that are overwritten; default: false",
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-u', '--user',
                        help='user name for logging into the canvas database. Default: {}'.format(CanvasPrep.default_user),
                        default=CanvasPrep.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database. Default: content of $HOME/.ssh/canvas_db',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-o', '--host',
                        help='host name or ip of database. Default: Canvas production database.',
                        default=CanvasPrep.default_host)
                        #default='canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com')
                        #default='canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com')
                        
    parser.add_argument('-t', '--table',
                        nargs='+',
                        help='Name of specific table to create whether or not they already exist; option can be repeated.',
                        default=[]
                        )

    parser.add_argument('-d', '--database',
                        help='MySQL/Aurora database (schema) into which new tables are to be placed. Default: canvasdata_aux',
                        default='canvasdata_aux')
    
    parser.add_argument('-q', '--quiet',
                        help='if present, only error conditions are shown on screen. Default: False',
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-y', '--dryrun',
                        help='if present, only print what would be done. Default: False',
                        action='store_true',
                        default=False);

    args = parser.parse_args();

    # Just wants list of tables?
    if args.list:
        CanvasPrep.list_tables()
        sys.exit()

    if args.password:
        # Get pwd from CLI with invisible chars:
        pwd = getpass.getpass('Password for user {given_user} at {host}: '.format(given_user=args.user,
                                                                                host=args.host))
    else:
        pwd = None
    CanvasPrep(user=args.user,
               pwd=pwd,
               host=args.host,
               target_db=args.database,
               tables=args.table,
               new_only=args.newonly,
               skip_backups=args.skipbackup,
               dryrun=args.dryrun,
               logging_level=logging.ERROR if args.quiet else logging.INFO  
               ).run()
