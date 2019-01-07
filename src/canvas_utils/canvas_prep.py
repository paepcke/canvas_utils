#!/usr/bin/env python
'''
Created on Jan 1, 2019

@author: Andreas Paepcke
'''

import argparse
import getpass
import logging
from os import getenv
import os
import pickle
import pwd
import sys

from pymysql_utils.pymysql_utils import MySQLDB

from pull_explore_courses import ECPuller

class CanvasPrep(object):
    '''
    Draws on continuously changing data from Canvas.
    Prepares auxiliary tables to facilitate analysis
    and visualizations.
    '''

    #default_user = 'canvasdata_prd'
    default_user = getpass.getuser()
    
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
                'AssignmentSubmissions',
                'ExploreCourses',
                'CourseAssignments',
                'Instructors',
                'CourseInstructor',
                'CourseInstructorTeams',
                'CourseEnrollment',
                'Courses',
                'DiscussionMessages',
                'DiscussionTopics',
                'Graders',
                'GradingProcess',
                'QuizDim',
                'RequirementsFill',
                'StudentUnits',
                'Students',
                'TeachingAssistants',
                ]
    
    # Paths to the SQL files that do the work.
    # Filled in constructor
    tbl_creation_paths = []
    
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 pwd=None, 
                 target_db=None, 
                 host='localhost',
                 create_all=False, 
                 logging_level=logging.INFO):
        '''
        Constructor
        '''
        if user is None:
            user = CanvasPrep.default_user
            
        if pwd is None:
            pwd = self.get_db_pwd()
            
        if target_db is None:
            self.target_db = CanvasPrep.canvas_db_aux
        else:
            self.target_db = target_db

        self.setup_logging()
        self.logger.setLevel(logging_level)
        
        if not create_all:
            self.log_info("NOTE: only creating tables not already in {}. Use --all to replace all.".format(target_db))
        else:
            self.log_info("NOTE: creating all tables overwriting those already in {}. Use --all to replace all.".format(target_db))
            
        # Under certain conditions __file__ is a relative path.
        # Ensure availability of an absolute path:
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        
        # Number of tables that exist; only some of those
        # might need to be (re)created:
        num_tables = len(CanvasPrep.tables)
            
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
        
        # Construct a dict that maps table names that require
        # more than just running the SQL in the respective table
        # creation file to methods in this class that know how
        # to handle the necessary computations:
        
        # Used to have tables whose .sql was not self-contained;
        # it needed non-sql activity before or after. We took
        # care of these, but keep this mechanism for special
        # treatment in create_tables() in place for possible
        # use:
        self.special_tables = {}
        
        # Set parameters such as acceptable datetime formats:
        self.prep_db()
        
        # Determine how many tables need to be done, and 
        # initialize a dict of tables alreay done. The dict
        # is a persistent object:
        
        completed_tables = self.get_existing_tables()
        
        # Get a fresh copy of the Explore Courses .xml file?
        
        if create_all or 'ExploreCourses' not in completed_tables: 
            # We are supposed to refresh the ExploreCourses table.
            # Get pull a fresh .xml file, and convert it to .csv:
            self.pull_explore_courses()
            
        # Create the other tables that are needed.
        try:
            completed_tables = self.create_tables(completed_tables=completed_tables)
        finally:
            pass
        
        
        if create_all:
            self.log_info("Created all %s tables. Done." % num_tables)
        else:
            not_done = num_tables - len(completed_tables)
            self.log_info("(Re)created %s tables. Already existing: %s. Done" %\
                          (not_done, len(completed_tables)))
        
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
        
        if len(completed_tables) == 0 or completed_tables is None:
            do_all = True
            completed_tables = []
        else:
            do_all = False
          
        for tbl_file_path in CanvasPrep.tbl_creation_paths:
            tbl_nm = self.tbl_nm_from_file(tbl_file_path)
            
            # Do we need to create this table?
            if not do_all and \
                tbl_nm.lower() in completed_tables or \
                tbl_nm in completed_tables:
                # Nope, got that one already
                continue
                
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
    
    #-------------------------
    # get_queries_dir 
    #--------------
    
    def get_queries_dir(self):
        
        # All queries are in subdir 'Queries' of
        # this script's directory:
        query_dir = os.path.join(self.curr_dir, 'Queries')
        return query_dir 

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

    parser.add_argument('-u', '--user',
                        help='user name for logging into the canvas database. Default: {}'.format(CanvasPrep.default_user),
                        default=CanvasPrep.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database. Default: content of $HOME/.ssh/canvas_db',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-t', '--host',
                        help='host name or ip of database. Default: canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com',
                        default='canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com')
                        
    parser.add_argument('-d', '--database',
                        help='MySQL/Aurora database (schema) into which new tables are to be placed. Default: canvasdata_aux',
                        default='canvasdata_aux')
    
    parser.add_argument('-a', '--all',
                        help='if present, already existing tables are retained; others are added. Else all tables refreshed. Default: False',
                        action='store_true');
                        
    parser.add_argument('-q', '--quiet',
                        help='if present, only error conditions are shown on screen. Default: False',
                        action='store_true');
                        

    args = parser.parse_args();

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
               create_all=args.all,
               logging_level=logging.ERROR if args.quiet else logging.INFO  
               )