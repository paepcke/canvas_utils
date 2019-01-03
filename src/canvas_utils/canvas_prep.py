'''
Created on Jan 1, 2019

@author: paepcke
'''

import getpass
import os
import pwd
import logging
    
from pymysql_utils.pymysql_utils import MySQLDB
from .pull_explore_courses import ECPuller


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
    canvas_pwd_file = 'canvas_pwd' 
    
    # Name of MySQL db (schema) where new,
    # auxiliary tables will be placed:
    canvas_db_aux = 'canvasdata_aux'
    
    tables = [
                'Terms',
                'Accounts',
                'AllUsers',
                'AssignmentSubmissions',
                'CourseAssignments',
                'CourseEnrollment',
                'CourseInstructor',
                'CourseInstructorTeams',
                'Courses',
                'DiscussionMessages',
                'DiscussionTopics',
                'ExploreCourses',
                'Graders',
                'GradingProcess',
                'Instructors',
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

    def __init__(self, user=None, pwd=None, db=None, host='localhost', logging_level=logging.INFO):
        '''
        Constructor
        '''
        
        if user is None:
            user = CanvasPrep.default_user
            
        if pwd is None:
            pwd = self.get_db_pwd()
            
        self.setup_logging()
            
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
                                      db=CanvasPrep.canvas_db_aux,
                                      host=host)
        
        self.log_info('Done connecting to db.')
        
        # Construct a dict that maps table names that require
        # more than just running the SQL in the respective table
        # creation file to methods in this class that know how
        # to handle the necessary computations:
        
        self.special_tables = {
            'ExploreCourses': self.pull_explore_courses,
            'QuizDim'       : self.create_quiz_dim
            }
        
        self.prep_db()
        self.pull_ec()
        self.create_tables()
        
    #-------------------------
    #  create_tables
    #--------------
        
    def create_tables(self):
        
        for tbl_file_path in CanvasPrep.tbl_creation_paths:
            tbl_nm = self.tbl_nm_from_file(tbl_file_path)
            special_handler = self.special_tables.get(tbl_nm, None)
            if special_handler is not None:
                self.handle_complicated_case(tbl_file_path, tbl_nm)
                continue
            # Simple case: get query as string:
            with open(tbl_file_path, 'r') as fd:
                query = fd.read().strip()
                
            # Replace the placeholder <canvas_db> with the
            # db name of the Canvas product db:
            query = query.replace('<canvas_db>', CanvasPrep.canvas_db_nm)
            query = query.replace('<canvas_aux>', CanvasPrep.canvas_db_aux)
            
            self.log_info('Working on table %s...' % tbl_nm)
            (errors, _warns) = self.db.execute(query, doCommit=False)
            if errors is not None:
                raise RuntimeError("Could not create table %s: %s" %\
                                   (tbl_nm, str(errors))
                                   )
            self.log_info('Done working on table %s' % tbl_nm)
        
    #-------------------------
    # pull_ec 
    #--------------
        
    def pull_ec(self):
        _puller = ECPuller('Data/ec.xml',
                           overwrite_existing=True,
                           log_level=logging.INFO,
                           logger=self.logger)
        
        
    #-------------------------
    # pull_explore_courses 
    #--------------
        
    def pull_explore_courses(self):
        pass
    
    #-------------------------
    # create_quiz_dim 
    #--------------
    
    def create_quiz_dim(self):
        pass
        
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
    # tbl_nm_from_file 
    #--------------
            
    def tbl_nm_from_file(self, tbl_creation_file_nm):
        (nm_no_ext, _ext) = os.path.splitext(tbl_creation_file_nm)
        return os.path.basename(nm_no_ext)
    
    #-------------------------
    # file_nm_from_tble 
    #--------------
    
    def file_nm_from_tble(self, tbl_nm):
        this_dir = os.path.dirname(__file__)
        return os.path.join(this_dir, 'Queries', tbl_nm) + '.sql'
    
    #-------------------------
    # get_queries_dir 
    #--------------
    
    def get_queries_dir(self):
        
        # All queries are in subdir 'Queries' of
        # this script's directory:
        curr_dir  = os.path.dirname(__file__)
        query_dir = os.path.join(curr_dir, 'Queries')
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
    
    CanvasPrep()