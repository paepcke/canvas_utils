#!/usr/bin/env python
'''
Created on Jan 1, 2019

@author: Andreas Paepcke
'''

import argparse
import datetime
import getpass
import logging
from os import getenv
import os
import pickle
import pwd
import re
import sys

from pymysql_utils.pymysql_utils import MySQLDB

from pull_explore_courses import ECPuller


class CanvasPrep(object):
    '''
    Draws on continuously changing data from Canvas.
    Prepares auxiliary tables to facilitate analysis
    and visualizations.
    '''

    # Production server
    #default_host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    
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
     
    
    tables = [
                'Terms',
                'Accounts',
                'AllUsers',
                'Students',
                'AssignmentSubmissions',
                'ExploreCourses',
                'Courses',
                'CourseAssignments',
                'Instructors',
                'CourseInstructor',
                'CourseInstructorTeams',
                'CourseEnrollment',
                'DiscussionTopics',
                'Graders',
                'GradingProcess',
                'RequirementsFill',
                'StudentUnits',
                'TeachingAssistants',
                'DiscussionMessages',
                ]
    
    # Paths to the SQL files that do the work.
    # Filled in constructor
    tbl_creation_paths = []
    
    # datetime format used for appending to table names
    # for backups:
    datetime_format = '%Y_%m_%d_%H_%M_%S'
    
    datetime_pat = re.compile('[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}$')
    
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 pwd=None, 
                 target_db=None, 
                 host='localhost',
                 tables=[],
                 new_only=False,
                 skip_backups=False, 
                 logging_level=logging.INFO):
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
        @param logging_level: how much of the run to document
        @type logging_level: logging.INFO/DEBUG/ERROR/...
        '''

        self.new_only = new_only
        self.skip_backups = skip_backups
        if user is None:
            user = CanvasPrep.default_user
            
        if pwd is None:
            pwd = self.get_db_pwd()
            
        if target_db is None:
            target_db = CanvasPrep.canvas_db_aux
        else:
            target_db = target_db

        # If user wants only particular tables to be created
        # then the tables arg will be a list of table names:
        if tables is not None and len(tables) > 0:
            CanvasPrep.tables = tables

        self.setup_logging()
        self.logger.setLevel(logging_level)
        
        # Under certain conditions __file__ is a relative path.
        # Ensure availability of an absolute path:
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        
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
                                      pwd, 
                                      db=target_db,
                                      host=host)
        
        self.log_info('Done connecting to db.')
        
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
            self.backup_tables(existing_tables) 
        
        if self.new_only:
            completed_tables = existing_tables
        else:
            # Just pretend no tables exist:
            completed_tables = []
        
        # Get a fresh copy of the Explore Courses .xml file?
        
        # We are supposed to refresh the ExploreCourses table.
        # Get pull a fresh .xml file, and convert it to .csv:
        self.pull_explore_courses()
            
        # Create the other tables that are needed.
        try:
            completed_tables = self.create_tables(completed_tables=completed_tables)
            
        finally:
            self.log_info('Closing db...')
            self.db.close()
            self.log_info('Done closing db.')
        
        self.log_info(f"(Re)created {len(completed_tables)} tables. Done")

    #------------------------------------
    # backup_tables 
    #-------------------    

    def backup_tables(self, table_names):
        '''
        Given a list of aux tables, rename them to have current
        datatime appended. 
        
        @param table_names: list of aux tables to back up
        @type table_names: [str]
        @return: datetime object whose string representation was
            used to generate the backup table names.
        @rtype: datetime.datetime
        '''
        
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

        rename_cmd = f"RENAME TABLE {' '.join(tbl_rename_snippets)};"

        # Do it!
        
        self.log_info(f"Renaming {len(table_names)} tables to backup names...")
        (errors, _warns) = self.db.execute(rename_cmd, doCommit=False)
        if errors is not None:
            raise RuntimeError(f"Could not rename at least some of tables {str(table_names)}")
            
        self.log_info(f"Done renaming {len(table_names)} tables to backup names.")
        return curr_time
                              
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
            query = query.replace('<canvas_db>', CanvasPrep.canvas_db_nm)
            query = query.replace('<canvas_aux>', CanvasPrep.canvas_db_aux)
            query = query.replace('<data_dir>', os.path.join(self.curr_dir, 'Data'))
            
            
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
    
    def get_existing_tables(self):
        '''
        Returns list of auxiliary canvas table names that are
        already in the target db for auxiliary tables. So: even
        if tables other than the ones listed in CanvasPrep.tables
        exist in the target db, only members of CanvasPrep.tables
        are considered. Others are ignored.
        
        @return: list of tables existing in target db
        @rtype: [str]
        '''
        
        # Some versions of MySQL mess with case, so 
        # normalize our table list for the purpose of 
        # comparing with tables found:
        
        all_relevant_tbls = [tbl_nm.lower() for tbl_nm in CanvasPrep.tables]
        
        existing_tbls = []
        tbl_names_query = '''
                          SELECT table_name 
                            FROM information_schema.tables 
                           WHERE table_schema = 'canvasdata_aux';
                         '''
        for tbl_name in self.db.query(tbl_names_query):
            if tbl_name.lower() in all_relevant_tbls:
                existing_tbls.append(tbl_name)
                
        return existing_tbls
        
    #-------------------------
    # get_db_pwd
    #--------------

    def get_db_pwd(self):
        
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
        (nm_no_ext, _ext) = os.path.splitext(tbl_creation_file_nm)
        return os.path.basename(nm_no_ext)
    
    #-------------------------
    # file_nm_from_tble 
    #--------------
    
    def file_nm_from_tble(self, tbl_nm):
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
           output: foo_2019_05_09_11_51_03
           
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
            dt_str = backup_tbl_name[-19:]
            try:
                dt_obj = datetime.datetime.strptime(dt_str, CanvasPrep.datetime_format)
            except ValueError:
                raise ValueError(f"Table name {backup_tbl_name} is not a valid backup name.")
            # Next, get the front of the name without the
            # separating underscore and datetime string:
            tbl_name = backup_tbl_name[0:len(backup_tbl_name) - 20] 
            return(tbl_name, dt_str, dt_obj) 
        except IndexError:
            raise ValueError(f"Non-conformant backup table name '{backup_tbl_name}'")
        
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
            handler = logging.FileHandler(logFile)
            print('Logging of control flow will go to %s' % logFile)
        else:
            # Create console handler:
            handler = logging.StreamHandler()
        handler.setLevel(loggingLevel)

        # Create formatter
        formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(handler)
        self.logger.setLevel(loggingLevel)

    #-------------------------
    # log_debug/warn/info/err 
    #--------------

    def log_debug(self, msg):
        self.logger.debug(msg)

    def log_warn(self, msg):
        self.logger.warn(msg)

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
                        default='canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com')
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
               logging_level=logging.ERROR if args.quiet else logging.INFO  
               ).run()
