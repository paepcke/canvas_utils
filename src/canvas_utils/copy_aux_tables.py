'''
Created on May 1, 2019

@author: paepcke
'''
import argparse
import getpass
import logging
from os import getenv
import os
import sys
import subprocess
from subprocess import PIPE

from pymysql_utils.pymysql_utils import MySQLDB


class AuxTableCopier(object):
    '''
    classdocs
    '''
    
    #default_user = 'canvasdata_prd'
    default_user = 'canvasdata_prd'
    
    # Host of db where tables are stored:
    host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    
    # Name of MySQL canvas data schema (db):
    canvas_db_nm = 'canvasdata_prd'
    
    # Canvas pwd file name:
    canvas_pwd_file = os.path.join(getenv("HOME"), '.ssh', 'canvas_pwd') 
    
    # Name of MySQL db (schema) where new,
    # auxiliary tables will be placed:
    canvas_db_aux = 'canvasdata_aux'
    
    file_ext = None
        
    tables = [
                'Terms',
                'Accounts',
                'AllUsers',
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
                'Students',
                'TeachingAssistants',
                'DiscussionMessages',
                ]
        
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 pwd=None, 
                 dest_dir='/tmp', 
                 overwrite_existing=False,
                 tables=None,    # Default: all tables are copied 
                 copy_format='sql', 
                 logging_level=logging.INFO):
        '''
        
        @param user: login user for database. Default: AuxTableCopier.default_user  
        @type user: str
        @param pwd: password for database. Default: read from file AuxTableCopier.canvas_pwd_file
        @type pwd:{str | None}
        @param dest_dir: directory where to place the .sql/.csv files. Default /tmp 
        @type dest_dir: str
        @param overwrite_existing: whether or not to delete files in the target
              dir. If false, must manually delete first.
        @type overwrite_existing: bool
        @param tables: optional list of tables to copy. Default all in AuxTableCopier.tables
        @type tables: [str]
        @param copy_format: whether to copy as mysqldump or csv
        @type copy_format: {'sql' | 'csv'}
        @param logging_level: how much of the run to document
        @type logging_level: logging.INFO/DEBUG/ERROR/...
        '''

        self.setup_logging()
        self.logger.setLevel(logging_level)
        
        if user is None:
            self.user = AuxTableCopier.default_user
        else:
            self.user = user

        # If pwd is None, as it ought to be,
        # then any use of pwd will use the get_db_pwd()
        # method below:
        self.pwd = pwd
            
        if dest_dir is None:
            self.dest_dir = '/tmp'
        else:
            self.dest_dir = dest_dir
            
        if copy_format in ['csv', 'sql']:
            if copy_format == 'csv':
                raise NotImplementedError("Copying as CSV is not implemented yet.")
            self.copy_format = copy_format
        else:
            raise ValueError(f"Only copy_format 'csv', and 'sql' are allowed; not {copy_format}")
        self.overwrite_existing = overwrite_existing
        
        self.host = AuxTableCopier.host

        # If user wants only particular tables to be copied
        # (subject to overwrite_existing), then the tables arg will be
        # a list of table names. If tables is an empty list,
        # nothing will be copied: 
        
        if tables is None:
            self.tables = AuxTableCopier.tables 
        else:
            # Note: could be empty table, in which
            # case nothing will be copied. Used for
            # unittests.
            self.tables = tables

            
        # Create list of full paths to local target tables
        AuxTableCopier.tbl_creation_paths = [self.file_nm_from_tble(tbl_nm) for tbl_nm in AuxTableCopier.tables]
              
        # Determine how many tables need to be done, and 
        # initialize a dict of tables alreay done. The dict
        # is a persistent object:
        
        self.completed_tables = self.get_existing_tables(set(tables))

    #------------------------------------
    # copy_tables 
    #-------------------    
        
    def copy_tables(self, table_names=None, overwrite_existing=None):
        
        if table_names is None:
            table_names = self.tables
            
        if overwrite_existing is None:
            overwrite_existing = self.overwrite_existing
            
        if overwrite_existing:
            self.log_info(f"NOTE: copying all tables overwriting those already in {self.dest_dir}.")
        else:
            self.log_info(f"NOTE: only copying tables not already in {self.dest_dir}. Use --all to replace all.")
            
        existing_tables = self.get_existing_tables(table_names)
        
        # Delete existing files, if requested to do so:
        if overwrite_existing:
            for table_name in existing_tables:
                table_file = self.file_nm_from_tble(table_name)
                os.remove(table_file)
            
            # Ready to copy all:
            tables_to_copy = set(table_names)
        else:
            tables_to_copy = set(table_names) - existing_tables 
            
        table_to_file_map = {table_name : self.file_nm_from_tble(table_name) for table_name in tables_to_copy}
        
        if self.copy_format == 'csv':
            copy_result = self.copy_to_csv_files(table_to_file_map)
        else:
            copy_result = self.copy_to_sql_files(table_to_file_map)

        if copy_result.errors is not None:
            for (table_name, err_msg) in copy_result.errors.items():
                self.log_err(f"Error copying table {table_name}: {err_msg}") 
            
        if overwrite_existing:
            self.log_info(f"Copied all {len(copy_result.completed_tables)} tables. Done.")
        else:
            self.log_info(f"Copied {len(copy_result.completed_tables)} of all {len(AuxTableCopier.tables)} tables. Done")
            
        return copy_result

    #------------------------------------
    # copy_to_sql_files 
    #-------------------
    
    def copy_to_sql_files(self, table_to_file_map):
        
        bash_cmd_template = ['mysqldump', '-u', 'canvasdata_prd', '-p', '<pwd>', '-h', 
                             'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com canvasdata_aux',
                             '<tbl_name>', 
                             '>',
                             '<file_name>']
        completed_tables = []
        error_map = {}
        for (table_name, file_name) in table_to_file_map.items():
            bash_cmd = [table_name if cmd_fragment == '<tbl_name>' 
                                   else cmd_fragment 
                        for cmd_fragment 
                        in bash_cmd_template]
            bash_cmd = [file_name if cmd_fragment == '<file_name>' 
                                  else cmd_fragment 
                        for cmd_fragment 
                        in bash_cmd]
            pwd = self.get_db_pwd() 
            bash_cmd = [pwd if cmd_fragment == '<pwd>' 
                                  else cmd_fragment 
                        for cmd_fragment 
                        in bash_cmd]

            completed_process = subprocess.run(bash_cmd,
                                               stderr=PIPE,
                                               stdout=PIPE,
                                               text=True,
                                               shell=True
                                               ) 
            if completed_process.returncode == 0:
                completed_tables.append((table_name, file_name))
                self.log_info(f'Done copying table {table_name}.')
            else:
                error_map[table_name] = completed_process.stdout.decode('UTF-8').strip()
                
        return CopyResult(completed_tables=completed_tables, errors=error_map)
        
    #------------------------------------
    # copy_to_csv_files 
    #-------------------    
    
    #**** To be implemented *******
    def copy_to_csv_files(self, table_names):
        pass
#         self.connect_to_src_db(self.user, 
#                                self.host, 
#                                self.pwd, 
#                                self.src_db)

        # Have to get schema for each table to make
        # the CSV header.

            
    #------------------------------------
    # connect_to_src_db 
    #-------------------
    
    def connect_to_src_db(self, user, host, pwd, src_db):
        
        self.log_info(f'Connecting to db {user}@{host}:{src_db}...')
                       
        try:
            # Try logging in, specifying the database in which all the tables
            # to be copied reside: 
            db = MySQLDB(user=user, passwd=pwd, db=src_db, host=host)
        except Exception as e:
            raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        
        self.log_info('Done connecting to db.')
        
        return db
         

            

    #------------------------------------
    # tables_to_copy 
    #-------------------    

    def tables_to_copy(self, table_list_set=None):
        
        if table_list_set is None:
            table_list_set = set(AuxTableCopier.tables)
        
        tables_done_set = self.get_existing_tables(table_list_set)
        tables_to_do_set = table_list_set - tables_done_set
        return tables_to_do_set
        
    #------------------------------------
    # get_existing_tables 
    #-------------------    
        
    def get_existing_tables(self, table_name_set):
        '''
        Return name of tables whose .csv or .sql files
        are in the target directory.
        
        @return: table names whose .csv/.sql files are in 
            self.dest_dir. Whether .csv or .sql is taken
            from self.file_ext
        @rtype: {str}
        '''
    
        if table_name_set is None:
            return set()
        if type(table_name_set) == list:
            table_name_set = set(table_name_set)
        
        dest_dir   = self.dest_dir
        file_ext   = self.copy_format
          
        # Get a list of *.sql files in the destination dir.
        # os.path.splittext(path) returns tuple: (front-part, extension with .)
        # '/tmp/trash.txt' ==> ('/tmp/trash', '.txt'):
        files_in_dir = [os.path.basename(one_file) for one_file in os.listdir(dest_dir) if os.path.splitext(one_file)[1] == file_ext]
        
        # Chop off the .sql or .csv extension from each file,
        # getting a set of (possibly) table names:
        table_copies_set = {os.path.splitext(one_file)[0] for one_file in files_in_dir}
        
        # Remove table names that aren't in the list
        # of tables: the intersection of table files
        # in the dest dir, and the tables we are to get:
        
        existing_tables = table_name_set & table_copies_set
        return existing_tables

    #-------------------------
    # get_db_pwd
    #--------------

    def get_db_pwd(self):
        
        if self.pwd is not None:
            return self.pwd()
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', AuxTableCopier.canvas_pwd_file)
            if os.path.exists(default_pwd_file):
                with open(default_pwd_file, 'r') as fd:
                    pwd = fd.readline().strip()
                    return pwd
            
        # Ask on console:
        pwd = getpass.getpass("Password for Canvas database: ")
        return pwd
    
    #-------------------------
    # file_nm_from_tble 
    #--------------
    
    def file_nm_from_tble(self, tbl_nm):
        return os.path.join(self.dest_dir, tbl_nm) + '.sql' if self.copy_format == 'sql' else '.csv'
    
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
        self.logger.warning(msg)

    def log_info(self, msg):
        self.logger.info(msg)

    def log_err(self, msg):
        self.logger.error(msg)
   
# -------------------------- Class CopyResult ---------------

class CopyResult(object):

    def __init__(self, completed_tables, errors):
        self.completed_tables = completed_tables
        self.errors = errors
    
# --------------------------- Main ------------------        
if __name__ == '__main__':
    
    copier = AuxTableCopier(tables=['Terms'])
    copy_result = copier.copy_tables()
    print(copy_result.completed_tables)
    print(copy_result.errors)
    
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Create auxiliary canvas tables."
                                     )

    parser.add_argument('-u', '--user',
                        help='user name for logging into the canvas database. Default: {}'.format(AuxTableCopier.default_user),
                        default=AuxTableCopier.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database. Default: content of $HOME/.ssh/canvas_db',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-o', '--host',
                        help='host name or ip of database. Default: Canvas production database.',
                        default='canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com')
                        
    parser.add_argument('-t', '--table',
                        nargs='+',
                        help='Name of specific table to create (subject to --all arg); option can be repeated.',
                        default=[]
                        )
        