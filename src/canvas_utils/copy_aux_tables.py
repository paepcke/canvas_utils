'''
Created on May 1, 2019

@author: paepcke
'''
from _collections import OrderedDict
import argparse
import collections
import csv
import getpass
import logging
from os import getenv
import os
from subprocess import PIPE
import subprocess
import sys

from pymysql_utils.pymysql_utils import MySQLDB

from canvas_prep import CanvasPrep


class AuxTableCopier(object):
    '''
    classdocs
    '''
    
    #default_user = 'canvasdata_prd'
    default_user = 'canvasdata_prd'
    
    # Host of db where tables are stored:
    #host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    
    # Name of MySQL canvas data schema (db):
    canvas_db_nm = 'canvasdata_prd'
    
    # Canvas pwd file name:
    canvas_pwd_file = os.path.join(getenv("HOME"), '.ssh', 'canvas_pwd') 
    
    # Name of MySQL db (schema) where new,
    # auxiliary tables will be placed:
    canvas_db_aux = 'canvasdata_aux'
    
    file_ext = None
        
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
            self.tables = CanvasPrep.create_table_name_array() 
        else:
            # Note: could be empty table, in which
            # case nothing will be copied. Used for
            # unittests.
            self.tables = tables
            
        # Create list of full paths to local target tables
        self.tbl_creation_paths = [self.file_nm_from_tble(tbl_nm) for tbl_nm in self.tables]
              
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
            self.log_info(f"Copied {len(copy_result.completed_tables)} of all {len(self.tables)} tables. Done")
            
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
        self.connect_to_src_db(self.user, 
                               self.host, 
                               self.pwd, 
                               self.src_db)

        # Have to get schema for each table to make
        # the CSV header.
        
        table_schemas = [self.populate_table_schema(table_name) for table_name in table_names]
        for table_schema in table_schema:
            self.copy_one_table_to_csv(table_schema)
            
    #-------------------------
    # copy_one_table_to_csv 
    #--------------
            
    def copy_one_table_to_csv(self, table_schema):
        
      
        table_name   = table_schema.table_name
        file_name    = os.path.join(self.dest_dir, table_name) + '.csv'
        
        # Array of col names for the header line.
        # The csv writer will add quotes around the col names:
        col_name_arr = table_schema.col_names(quoted=False)
        
        with open(file_name, 'w') as fd:
            csv_writer = csv.writer(file_name, delimiter=',')
            # Write header line:
            csv_writer.writerow(col_name_arr)
            

    #-------------------------
    # populate_table_schema 
    #--------------
    
    def populate_table_schema(self, table_name):
        '''
        Return an SQL table create statement for the
        given table.
        
        Assumption: self.db holds a MySQLDB instance.
        I.e. connect_to_src_db() has been called.
        
        @param table_name: aux table name whose creation SQL is to be produced
        @type table_name: str
        @return: an SQL statement that would create the table in an Oracle database
        @rtype: str
        @raise RuntimeErrer: when MySQL database cannot be contacted. 
        '''
        
        table_metadata_cmd = f'''SELECT column_name, data_type, column_default, ordinal_position 
                                   FROM information_schema.columns 
                                  WHERE table_schema = '{AuxTableCopier.canvas_db_aux}' 
                                    AND table_name = '{table_name}';
                            '''
        schema_obj = Schema(table_name)
        
        table_metadata = self.db.query(table_metadata_cmd)
        for (col_name, col_type, col_default, position) in table_metadata:
            # Add info about one column to this schema:
            schema_obj.push(col_name, col_type, col_default, position)
    
        # For each column (i.e. SchemaColumn instance): if the col has 
        # an index, create SchemaIndex that defines the schema, and add 
        # it as the 'index' property to the SchemaColumn.
        
        index_metadata_cmd = f'''SELECT index_name, column_name, seq_in_index
                                   FROM information_schema.statistics
                                  WHERE TABLE_SCHEMA = '{AuxTableCopier.canvas_db_aux}'
                                    AND TABLE_NAME   = '{table_name}';  
                              '''
        idx_info = self.db.query(index_metadata_cmd)
        
        for (index_name, col_name, seq_in_index) in idx_info:
            
            schema_col_obj = schema_obj[col_name]
            schema_idx_obj = SchemaIndex(index_name, col_name, seq_in_index)
            schema_col_obj.index = schema_idx_obj
    
    #------------------------------------
    # connect_to_src_db 
    #-------------------
    
    def connect_to_src_db(self, user, host, pwd, src_db):
        
        self.log_info(f'Connecting to db {user}@{host}:{src_db}...')
                       
        try:
            # Try logging in, specifying the database in which all the tables
            # to be copied reside: 
            self.db = MySQLDB(user=user, passwd=pwd, db=src_db, host=host)
        except Exception as e:
            raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        
        self.log_info('Done connecting to db.')
        
        return self.db
         
    #------------------------------------
    # tables_to_copy 
    #-------------------    

    def tables_to_copy(self, table_list_set=None):
        '''
        Given a set of table names, check which of those correspond
        to files in the copy target directory. Either as .csv or .sql.
        Return a subset of the provided table_list_set, with tables
        that do not have an associated file.
        
        By default, the set to check is all aux tables.
        
        @param table_list_set: names of tables whose status is to be checked. 
        @type table_list_set: set(str)
        '''
        
        if table_list_set is None:
            table_list_set = set(self.tables)
        
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
    
# ------------------------------------------------  Class Schema -----------------------
    
class Schema(collections.MutableMapping):
    '''
    Instances hold information about one table schema.
    Behaves like an ordered dict. Keys are column names,
    values are associated data types.
    
       my_schema['my_col']  ==> a SchemaColumn instance.
    
    A Schema instance holds one SchemaColumn instances
    for each column in the table.
    
    SchemaColumn instances have properties:
        
        my_col.col_name    
        my_col.col_type
        my_col.position   # Position of column in CREATE TABLE statement
        my_col.index      # A SchemaIndex instance
        
        setter: my_col.index = <SchemaIndex> instance
        
    SchemaIndex instances have properties:
    
        my_idx.idx_name
        my_idx.col_name
        my_idx.seq_in_index  # Position of col in composite
                             # indexes: create index on Foo(col1,col2)
        
     
    '''

    #-------------------------
    # constructor 
    #--------------
    
    def __init__(self, table_name, *args, **kwargs):
        
        self.table_name  = table_name
        self.column_dict = OrderedDict()
        
        self.update(dict(*args, **kwargs))
        
    
    #-------------------------
    # push 
    #--------------
    
    def push(self, col_name, 
                   col_type, 
                   col_default,
                   col_position, 
                   ):
        '''
        Add information about one column. All information is
        provided, except the indexe(s) on the column. Use
        the add_index() method to add each index after calling
        this method.
        
        @param col_name: name of new column
        @type col_name: str
        @param col_type: SQL type of column
        @type col_type: str
        @param col_default: default value of column
        @type col_default: str
        @type col_default: str
        @param col_position: position of the column in the CREATE TABLE statement
        @type col_position: int
        '''
        
        self.column_dict[col_name] = SchemaColumn(col_name, col_type, col_default, col_position)
    
    #-------------------------
    # add_index 
    #--------------
    
    def add_index(self, index_name, col_name, seq_in_index=1):
        '''
        Add one index to this column instance. The seq_in_index
        parameter is relevant only for composite indexes:
        
           CREATE INDEX foo_idx ON MyTable(col1, col2)
           
        The col1 index would have seq_in_index = 1, that of col2 
        would be 2. 
        
        @param index_name: name of the index as in the CREATE INDEX statement
        @type index_name: str
        @param col_name: name of column on which the index is defined
        @type str
        @param seq_in_index: the position the column has in the index
        @type seq_in_index: int
        '''
        
        index_obj = SchemaIndex(index_name, col_name, seq_in_index)
        self.column_dict[col_name].indexes = index_obj

    #-------------------------
    # col_names 
    #--------------

    def col_names(self, quoted=True):
        '''
        Return an array of quoted or unquoted column names 
        of this table. The names will be ordered by how
        they appeared in the table's original CREATE TABLE
        statement.
        
        @param quoted: if True, each column name in the returned array will
            have a double quote char around it. 
        @type quoted: boolean
        @return: sorted list of column names; quoted or unquoted
        @rtype: [str]
        '''
        attr_getter = lambda col_obj: col_obj.position
        col_objs = self.column_dict.values()
        sorted_col_objs = col_objs.sort(key=attr_getter)
        if quoted:
            res = [f'"{col_obj.col_name}"' for col_obj in sorted_col_objs]
        else:
            res = [f'{col_obj.col_name}' for col_obj in sorted_col_objs]
            
        return res

    #-------------------------
    # construct_create_table 
    #--------------
        
    def construct_create_table(self):
        '''
        Called when all the column schema objects have been
        added. Construct a legal CREATE TABLE statement,
        such as: 
        
		   CREATE TABLE `Trash` (
		     `id` int(11) NOT NULL AUTO_INCREMENT,
		     `var1` int(11) DEFAULT NULL,
		     `var2` int(11) DEFAULT NULL,
		     `var3` varchar(40) DEFAULT NULL,
		     PRIMARY KEY (`id`),
		     KEY `var3_idx` (`var3`),
		     KEY `var1_2_idx` (`var1`,`var2`)
		           
        '''
        
        create_stmt = f"CREATE TABLE {self.table_name} (\n"
        
        # Dict where we collect all index objects:
        #
        #   {idx_name : [idx_obj1, idx_obj2, ...]
        #
        # Composite indexes will have more than one entry
        # in the array. Indexes on just one column will have
        # single index obj in the array:  
        
        index_objs = {}
        
        # Function for sorting a list of index objects by 
        # their seq_in_index property:
        attr_getter = lambda idx_obj: idx_obj.seq_in_index
    
        
        for schema_col_obj in self.column_dict.values():
            # Add one line to the CREATE TABLE stmt for this column:
            create_stmt += f"{schema_col_obj.col_name}  {schema_col_obj.col_type},\n"
            if schema_col_obj.index is not None:
                # Get this column's index obj:
                idx_obj = schema_col_obj.index
                try:
                    # Add it to the dict where we collect
                    # index objs with the same name:
                    index_objs[idx_obj.idx_name].append(idx_obj)
                except KeyError:
                    index_objs[idx_obj.idx_name] = [idx_obj]
                
        # Go through the indexes we might have found for some
        # of the columns, and add them to the end of the create stmt.
        
        for idx_obj_array in index_objs.values():
            
            if idx_obj.idx_name == 'PRIMARY':
                create_stmt += f"PRIMARY KEY ("
            else:
                create_stmt += f"KEY {idx_obj.idx_name} ("
            
            # Each entry is an array of SchemaIndex objs. If
            # that array has more than one element, the index
            # is composite. Else it is simple:
            
            if len(idx_obj_array) == 1:
                idx_obj = idx_obj_array[0]
                # Simple case: index is not a composite:
                    create_stmt += f"({idx_obj.col_name}),\n"
                # next array of indexes: 
                continue
            
            # Composite index. Sort the array by seq_in_index to
            # correctly get the order of the columns that participate
            # in the index composite. I.e. add the "(var1, var2)":
            
            sorted_indexes = idx_obj_array.sort(key=attr_getter)
            for index_obj in sorted_indexes:
                create_stmt += f"{index_obj.col_name},"
            # Now that all columns are listed, replace the last comma
            # with a closed paren:
            create_stmt = create_stmt[:-1] + '),'
        
        # Replace the trailing comma with a closing paren and semicolon:
        create_stmt = create_stmt[:-1] + ');'
        
        return create_stmt
    
    #-------------------------
    # standard override methods for dict 
    #--------------
     
    def __getitem__(self, key):
        return self.column_dict[key]

    def __setitem__(self, key, value):
        self.column_dict[key] = value

    def __delitem__(self, key):
        raise NotImplemented("Cannot delete columns from schema instances.")

    def __iter__(self):
        return iter(self.column_dict)

    def __len__(self):
        return len(self.column_dict)        
    
        
# --------------------------- SchemaColumn Class ---------------------        

class SchemaColumn(object):
    
    def __init__(self, col_name, col_type, position):
        self.col_name = col_name
        self.col_type = col_type
        self.position = position
        self.index    = None
        
    @property
    def col_name(self):
        return self.col_name
    
    @property
    def col_type(self):
        return self.col_type
        
    @property
    def position(self):
        return self.position

    @property
    def index(self):
        return self.index
    
    @index.setter
    def index(self, schemaIndex_instance):
        if not isinstance(schemaIndex_instance, SchemaIndex):
            raise TypeError("The index information in SchemaColumn instances must be a SchemaIndex instance.")
        self.index = schemaIndex_instance

        
# -------------------------- SchemaIndex Class ---------------------        
            
class SchemaIndex(object):
    
    def __init__(self, idx_name, col_name, seq_in_index):
        self.idx_name = idx_name
        self.col_name = col_name
        self.seq_in_index = seq_in_index
        
    @property
    def idx_name(self):
        return self.idx_name
    
    @property
    def col_name(self):
        return self.col_name
    
    @property
    def seq_in_index(self):
        return self.seq_in_index
            
            


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
        