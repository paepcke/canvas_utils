#!/usr/bin/env python
'''
Created on Jan 1, 2019

@author: Andreas Paepcke
'''

import argparse
import datetime
import logging
from os import getenv
import os
import pickle
import re
import shutil
import stat
import subprocess
import sys

from canvas_utils_exceptions import DatabaseError, ExploreCoursesError
from clear_old_backups import BackupRemover
from config_info import ConfigInfo
from pull_explore_courses import ECPuller
from utilities import Utilities


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

    log_table_name = 'LoadLog'
    
    # Recognize: '2019_11_02_11_02_03'
    #        or: '2019_11_02_11_02_03_1234':
    datetime_regx = '[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}[_]{0,1}[0-9]*$'
    
    datetime_pat = None # Will be set in __init__() to re.compile(CanvasPrep.datetime_regx)
    
    unittests = False
    
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 db_pwd=None, 
                 target_db=None, 
                 host=None,
                 tables=[],
                 excludes=[],
                 new_only=False,
                 skip_backups=False,
                 dryrun=False, 
                 logging_level=logging.INFO,
                 unittests=False):
        '''
        
        @param user: mysql user. Default set in setup.cfg
        @type user: str
        @param db_pwd: mysql password. Usually in ~/.ssh/canvas_pwd, or 
            other file as specified in setup.cfg. Else requested on the
            command line.
        @type db_pwd: {None | str}
        @param target_db: name of the database (a.k.a. MySQL schema) in
            the MySQL server. Can be set in setup.cfg
        @type target_db: str
        @param host: name, or IP of machine that runs the MySQL service.
            Can be set in setup.cfg
        @type host: str 
        @param tables: optional list of aux tables to create. Default is None,
            which means create all aux tables
        @type tables: [str]
        @param excludes: optional list of aux tables to *not* create
        @type excludes: [str]
        @param new_only: if true, only create aux tables that don't already exist
            in the database
        @type new_only: bool
        @param skip_backups: normally, when aux tables are created, and would overwrite
            an existing table, that existing table is backed up. With False here, 
            no backup is created.
        @type skip_backups: bool
#        @param dryrun: only print what would be done, make no changes
#        @type dryrun: bool
        @param logging_level: how much logging to do.
        @type logging_level: logging.{INFO|WARNING|ERROR|DEBUG|CRITICAL|NOTSET}
        @param unittests: whether or not this instance is created for unit testing
        @type unittests: bool
        '''

        # Access to convenience funcations:
        self.utils = Utilities()
        
        self.unittests = CanvasPrep.unittests = unittests

        # Regex-parsing date-time strings used to name 
        # backup tables.
        CanvasPrep.datetime_pat = re.compile(CanvasPrep.datetime_regx)

        # Under certain conditions __file__ is a relative path.
        # Ensure availability of an absolute path:
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        
        # Read any configs from the config file, if it exists:
        config_info = ConfigInfo()

        # The db where the raw Canvas exports reside:
        self.raw_data_db = config_info.raw_data_db

        if not unittests:
            CanvasPrep.default_host  = config_info.default_host
            CanvasPrep.canvas_db_aux = config_info.canvas_db_aux
            CanvasPrep.default_user  = config_info.default_user
        else:
            CanvasPrep.default_host = config_info.test_default_host
            CanvasPrep.default_user = config_info.test_default_user
        
        self.new_only = new_only
        self.skip_backups = skip_backups
        self.dryrun = dryrun
        if user is None:
            user = CanvasPrep.default_user

        self.user = user

        if host is None:
            host = CanvasPrep.default_host
            
        self.host = host
        
        if db_pwd is None:
            db_pwd = self.utils.get_db_pwd(host, unittests=unittests)
        elif db_pwd == True:
            db_pwd = self.utils.get_db_pwd(host, ask_user= True, unittests=unittests)
        
        self.pwd = db_pwd 
        
        if target_db is None:
            target_db = config_info.canvas_db_aux
        else:
            target_db = target_db
            
        self.target_db = target_db
        self.excludes = excludes
            
        self.pwd_file_pointer = config_info.canvas_pwd_file
            
        self.utils.setup_logging(logging_level)
        self.log_info = self.utils.log_info
        self.log_warn = self.utils.log_warn
        self.log_err  = self.utils.log_err

        # If user wants only particular tables to be created
        # then the tables arg will be a list of table names.
        # Get a list of true tables represented in Queries:
        
        legitimate_tables = self.utils.create_table_name_array()
        if tables is not None and len(tables) > 0:
            # Did user make typo in the -t/--table option?
            user_tbl_set   = set(tables)
            legit_tbl_set  = set(legitimate_tables)
            if not user_tbl_set.issubset(legit_tbl_set):
                bad_tables = user_tbl_set - legit_tbl_set
                print(f"Non-aux table(s): {bad_tables}")
                sys.exit(1)
            CanvasPrep.tables = tables
        else:
            # No particular tables named on command line,
            # So get all .sql file names in Query directory.
            # Names are the table names.
            CanvasPrep.tables = legitimate_tables

        # Allow other modules to see which tables are of
        # concern without having to import CanvasPrep
        self.utils.tables = CanvasPrep.tables

        # Create list of full paths to table
        # creation sql files:
        CanvasPrep.tbl_creation_paths = [self.file_nm_from_tble(tbl_nm) for tbl_nm in CanvasPrep.tables]
        
        # For convenience: figure out where all the
        # update queries, and the file with useful
        # mysql functions and procedures are stored:
        
        self.queries_dir = self.get_queries_dir()
        
        self.log_info('Connecting to db %s@%s.%s...' %\
                      (user, host, CanvasPrep.canvas_db_aux))
        
        self.db = self.utils.log_into_mysql(user, 
                                            self.pwd, 
                                            db=target_db,
                                            host=host)
        self.log_info('Done connecting to db.')
        
        # Used to have tables whose .sql was not self-contained;
        # it needed non-sql activity before or after. We took
        # care of these, but keep this mechanism for special
        # treatment in create_tables() in place for possible
        # future use:
        self.special_tables = {}
        
        if unittests:
            return

        # Set parameters such as acceptable datetime formats:
        self.prep_db()
        
    #------------------------------------
    # run 
    #-------------------    
        
    def run(self):
        
        # Determine how many tables need to be done.
        # If self.new_only is true, then only non-existing
        # tables are to be done, else all of them:
        
        existing_tables = self.utils.get_existing_tables_in_dir(self.db)
        
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
        
        if len(self.excludes) > 0:
            # Pretend the excluded tables are already done:
            completed_tables.extend(self.excludes)
                    
        # Get a fresh copy of the Explore Courses .xml file?
        
        # We are supposed to refresh the ExploreCourses table.
        # Get pull a fresh .xml file, and convert it to .csv:
        if self.dryrun:
            print("Would fetch fresh copy of explore-courses.")
        else:
            try:
                self.pull_explore_courses()
            except ExploreCoursesError as e:
                self.log_err(e.message)
            
        # Create the other tables that are needed.
        try:
            if self.dryrun:
                print("Would create fresh copies of the other courses.")
                print(f"Would remove all but {BackupRemover.num_to_keep} backups")

            else:
                completed_tables = self.create_tables(completed_tables=completed_tables)
                BackupRemover(user=self.user,
                              db_pwd=self.pwd,
                              target_db=self.target_db,
                              host=self.host
                              )
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
            self.utils.shutdown_logging()

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
                        WHERE table_name REGEXP '{table_root}_{CanvasPrep.datetime_regx}'
                          AND table_schema = '{self.db.dbName()}';
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
                                           key=lambda tbl_name: self.utils.backup_table_name_components(tbl_name)[2])
        
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
        Runs through the tbl_creation_paths list of table
        creation .sql files, and executes each. 
        
        The completed_tables parameter is a list of tables
        that are currently already in the target db. None of
        those tables will be replaced. If all tables are to
        be created, set the parameter to an empty list.
        
        @param completed_tables: dictionary of completed tables
        @type completed_tables: {str : bool}
        @return: a new, or augmented table completion dict
        @rtype: {str : bool}
        '''
        
        # Go through the Queries subdir, getting the query
        # creation file names. Chop off the .sql extensions
        # to get the table names: 
        for tbl_file_path in CanvasPrep.tbl_creation_paths:
            tbl_nm = self.tbl_nm_from_file(tbl_file_path)
            
            if tbl_nm in completed_tables:
                # Table already exists. Skip it:
                continue
            
            # Special handlers would be for table creations that
            # cannot be expressed in sql alone. Currently there
            # are no such cases:
            special_handler = self.special_tables.get(tbl_nm, None)
            if special_handler is not None:
                self.handle_complicated_case(tbl_file_path, tbl_nm)
                continue
            
            # Simple case: get query as string:
            with open(tbl_file_path, 'r') as fd:
                query = fd.read().strip()
            
            # Default aux table db is hard coded into the sql files
            # in the Queries dict. At first we used placeholders in
            # the sql files for the aux and canvas raw data tables.
            # Since these .sql files are user customizable, placeholders
            # were too complex. Therefore the hardcoding:
            #
            #    Table aux destination db: canvasdata_aux
            #    Raw canvas export tables: canvasdata_prd
            #
            # We now replace these hardcoded quantities with what was
            # set in setup.cfg (or setupSample.cfg if no setup.cfg was
            # created during installation):
            
            query = query.replace('canvasdata_aux', self.target_db)
            query = query.replace('canvasdata_prd', self.raw_data_db)
                
            if self.dryrun:
                print(f"Would create table {tbl_nm}.")
                completed_tables.append(tbl_nm)
                continue
                      
            self.log_info('Working on table %s...' % tbl_nm)
            (errors, _warns) = self.db.execute(query, doCommit=False)
            if errors is not None:
                # Include in error msg the tables that are not
                # yet done, so user can recover more easily:
                tbls_to_do = [tbl_name for tbl_name in CanvasPrep.tables if tbl_name not in completed_tables]
                raise DatabaseError(f"Could not create table {tbl_nm}: {str(errors)}. \n Still to do in order: {tbls_to_do}")

            completed_tables.append(tbl_nm)
            # The sql creation files in Queries sometimes 
            # leave the db USEing the db of the raw Canvas
            # db (canvasdata_prd). Make sure we start USEing
            # the aux tables one again:
            self.db.execute(f'USE {self.target_db}')
            # Make entry in table_refresh_log table:
            self.log_table_creation(tbl_nm)
            self.log_info('Done working on table %s' % tbl_nm)
        return completed_tables
        
    #-------------------------
    # log_table_creation 
    #--------------
    
    def log_table_creation(self, tbl_nm):
        '''
        Make an entry in table table_refresh_log, indicating
        that the given table name was refreshed at the given
        date and time. Also adds the new table's number of rows.
        
        @param tbl_nm: name of table that was refreshed
        @type tbl_nm: str
        '''

        # For convenience:
        load_log_tbl_nm = CanvasPrep.log_table_name
        curr_db_schema = self.db.dbName()
        
        # The information schema is updated lazily. Any
        # changes, such as addition of rows, or table creation
        # won't show in information_schema, unless one first
        # runs 'ANALYZE TABLE <tbl_name'
        
        self.db.execute(f'ANALYZE TABLE {tbl_nm}')
        
        self.utils.ensure_load_log_table_existence(load_log_tbl_nm, self.db)
        
        # Find number of rows in table:
        res = self.db.query(f'''SELECT COUNT(*) FROM {tbl_nm}''')
        num_rows = res.next()
             
        # Make the entry:
        (err, _warn) = self.db.execute(f'''INSERT INTO {curr_db_schema}.{load_log_tbl_nm} (tbl_name, num_rows)
                                                VALUES('{tbl_nm}', {num_rows})
                                                ''')
        if err is not None:
            raise DatabaseError(f"Cannot insert {tbl_nm}'s entry into load log {load_log_tbl_nm}: {repr(err)}")
        
    #-------------------------
    # pull_explore_courses 
    #--------------
        
    def pull_explore_courses(self):
        '''
        Attempts to retrieve a new XML file from
        the ExploreCourses site. If success, converts to
        .csv, and copies that file both to /tmp/ and to 
        the Data subdirectory of this file.
        
        @raise ExploreCoursesError: if site does not respond,
            or XML is not properly formatted.
        '''
        
        # Need absolute path to XML file:
        ec_xml_path = os.path.join(self.curr_dir, CanvasPrep.ec_xml_file)
        
        puller = ECPuller(ec_xml_path,
                          overwrite_existing=True,
                          log_level=self.utils.logger.level,
                          logger=self.utils.logger
                          )
        # Do the retrieval of a new .xml file from
        # the HTTP server:
        num_bytes_from_web_site = puller.pull_ec()
                
        # Convert the .xml to .csv:
        (xml_file_root, _ext) = os.path.splitext(ec_xml_path)
        csv_outfile = xml_file_root + '.csv'
        
        # If the Web site delivered, then convert
        # the XML to csv, and copy to /tmp for others to find:
        
        if num_bytes_from_web_site > 0:
            try:
                puller.ec_xml_to_csv(ec_xml_path, csv_outfile)
            except ExploreCoursesError as e:
                if self.try_use_old_ec_file():
                    raise ExploreCoursesError("Could not retrieve new ExploreCourses info; using old info.\n" +
                                              f"The problem: {e.message}")
                else:
                    raise ExploreCoursesError("Could not retrieve new ExploreCourses info; the table will be empty\n" +
                                              f"The problem: {e.message}")
        else:
            # See whether a previous .csv file is present, and
            # use that with a warning:
            if self.try_use_old_ec_file():
                self.utils.log_warn("EC site did delivered nothing. Using old ExploreCourses info!")
            else:
                # Not much we can do:
                raise ExploreCoursesError("EC site delivered nothing. ExploreCourses will be empty.")
                return
        # Copy the .csv file to /tmp so that the Queries/ExploreCourses.sql
        # can find it to import:
        shutil.copy(csv_outfile, '/tmp')
        os.chmod(csv_outfile,
                 stat.S_IRUSR | stat.S_IWUSR |  # RW owner
                 stat.S_IRGRP | stat.S_IWGRP |  # RW group
                 stat.S_IROTH | stat.S_IWOTH    # RW other
                 )

    #-------------------------
    # try_use_old_ec_file 
    #--------------
    
    def try_use_old_ec_file(self):
        '''
        See whether an old explore_courses.csv file
        exists. If so copy it to /tmp/ Return True
        if this attempt successful, else return False.
        
        @return: whether an explore_courses.csv file was
            found, and copied to /tmp
        @rtype: bool
        '''

        # Find path to earlier EC .csv file, if one exists: 
        ec_xml_path = os.path.join(self.curr_dir, CanvasPrep.ec_xml_file)

        (xml_file_root, _ext) = os.path.splitext(ec_xml_path)
        csv_outfile = xml_file_root + '.csv'
        
        # See whether a previous .csv file is present, and
        # use that with a warning:
        if os.path.exists(csv_outfile) and os.path.getsize(csv_outfile) > 0:
            shutil.copy(csv_outfile, '/tmp')
            return True
        else:
            return False
        
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
    # prep_db 
    #--------------
    
    def prep_db(self):
        '''
        Set selected MySQL session configurations.
        '''
    
        # MySQL 8 started to complain when functions do not 
        # specify DETERMINISTIC or NO_SQL, or one of several other
        # function characteristics. Avoid that complaint:
        
#         (err, _warn) = self.db.execute("SET GLOBAL log_bin_trust_function_creators = 1;")
#         if err is not None:
#             self.log_warn(f"Cannot set global log_bin_trus_function_creators: {repr(err)}")
        
        
        # At least for MySQL 8.x we need to allow zero dates,
        # like '0000-00-00 00:00:00', which is found in the Canvas db:
        
        (err, _warn) = self.db.execute('SET sql_mode="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";')
        if err is not None:
            self.log_warn(f"Cannot set sql_mode: {repr(err)}")
        
        
        # Ensure that all the handy SQL functions are available.
        # They are in file canvasMysqlProcs.sql. The file is imported
        # into a db (a.k.a. MySQL schema) via "SOURCE <path-to-sql-file>" 
        # We cannot use the usual self.db.execute(), b/c SOURCE is a
        # MySQL shell directive, not an SQL command. So must use the
        # call_mysql.sh script for the call
        
        # Location of .sql file, and command string:
        funcs_file = os.path.join(self.curr_dir, 'canvasMysqlProcs.sql')
        source_cmd = f'SOURCE {funcs_file}'
        
        # Location of call_mysql.sh script:
        shell_script = os.path.join(os.path.dirname(__file__), 'call_mysql.sh')
        
        # Get full path to mysql command:
        mysql_path = self.utils.get_mysql_path()
        src_load_stmt_arr =[shell_script,
                            self.host,
                            self.user, 
                            self.pwd_file_pointer,
                            self.target_db,
                            mysql_path,
                            '/dev/null',
                            source_cmd
                            ] 
        _completed_process = subprocess.run(src_load_stmt_arr, 
                                            #capture_output=True, # Only for debugging 
                                            shell=False)

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

    
    #------------------------------------
    # list_tables 
    #-------------------    

    @classmethod
    def list_tables(cls):
        '''
        List tables that can be created to screen.
        '''
        utils = Utilities()
        # Fill the tables array with all tables created
        # in the Queries dir:
        tables = Utilities.create_table_name_array()
        
        utils.print_columns(tables, '\nTables that can be created:')
    
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
                    self.utils.backup_table_name_components(table_name)
                    
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
 
        
# ----------------------------- Main ------------------------

if __name__ == '__main__':
    
    config_info = ConfigInfo()
    
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
    
    parser.add_argument('-e', '--excludes',
                        nargs='+',
                        help='Name of one or more specific table(s) to NOT create.',
                        default=[]
                        )
                        
    parser.add_argument('-s', '--skipbackup',
                        help="don't back up tables that are overwritten; default: false",
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-u', '--user',
                        help=f'user name for logging into the canvas database.\n' +
                             f'Default: {config_info.default_user}.',
                        default=config_info.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database.\n' +
                             'Default: content of $HOME/.ssh/canvas_db',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-o', '--host',
                        help=f'host name or ip of database.\n' +
                             f'Default: {config_info.default_host}',
                        default=config_info.default_host)
                        
    parser.add_argument('-t', '--table',
                        nargs='+',
                        help='Name of one or more specific table(s) to create whether or not they already exist.',
                        default=[]
                        )

    parser.add_argument('-d', '--database',
                        help=f'MySQL/Aurora database (schema) into which new tables are to be placed.\n' +
                             f'Default: {config_info.canvas_db_aux}',
                        default=config_info.canvas_db_aux)
    
    parser.add_argument('-q', '--quiet',
                        help='if present, only error conditions are shown on screen. Default: False',
                        action='store_true',
                        default=False);
                        
#     parser.add_argument('-y', '--dryrun',
#                         help='if present, only print what would be done. Default: False',
#                         action='store_true',
#                         default=False);

    args = parser.parse_args();

    # Just wants list of tables?
    if args.list:
        CanvasPrep.list_tables()
        sys.exit()

    try:
        CanvasPrep(user=args.user,
                   db_pwd=args.password,
                   host=args.host,
                   target_db=args.database,
                   tables=args.table,
                   excludes=args.excludes,
                   new_only=args.newonly,
                   skip_backups=args.skipbackup,
                   #dryrun=args.dryrun,
                   logging_level=logging.ERROR if args.quiet else logging.INFO  
                   ).run()
    except KeyboardInterrupt:
        print("\nCanvas aux table generation stopped by user.")