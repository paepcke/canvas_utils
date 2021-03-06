#!/usr/bin/env python
'''
Created on May 1, 2019

@author: paepcke
'''
from _collections import OrderedDict
import argparse
import collections.abc
import datetime
import logging
import os
import sys
from subprocess import PIPE
import subprocess
from pathlib import Path

from canvas_utils_exceptions import DatabaseError
from config_info import ConfigInfo
from query_sorter import TableError
from utilities import Utilities

class AuxTableCopier(object):
    '''
    Exports aux tables as .tsv from aux database into a
    given directory.
    '''
    
    file_ext = None
    
    # Compute year from which onward GradingProcess 
    # records are to be exported to .tsv. We use
    # <current_year> - 4. Just change the 4 to taste.
    # But the smaller the start year, the longer the
    # export will take:    
    
    GRADING_PROCESS_START_YEAR = datetime.datetime.now().year - 1
    
    # Number of rows to pull in each batch
    # when a table needs to be pulled in batches
    # via an auto-increment seq number:
    SEQ_NUM_BATCH_SIZE = 50000
        
    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, 
                 user=None, 
                 db_pwd=None,
                 host=None, 
                 dest_dir=None, 
                 overwrite_existing=True,
                 tables=None,    # Default: all tables are copied 
                 copy_format='csv', 
                 logging_level=logging.INFO,
                 unittests=False,
                 unittest_db_name=None
                 ):
        '''
        
        @param user: login user for database. Default: default_user in setup.cfg  
        @type user: str
        @param db_pwd: password for database. Default: read from file set in setup.cfg
        @type db_pwd:{str | None | bool}
        @param host: host of the source db. Default: default_host in setup.cfg
        @type host: str
        @param dest_dir: directory where to place the .sql/.tsv files. Default /tmp 
        @type dest_dir: str
        @param overwrite_existing: whether or not to delete files in the target
              dir. If false, must manually delete first.
        @type overwrite_existing: bool
        @param tables: optional list of tables to copy. Default all Queries subdirectory
        @type tables: [str]
        @param copy_format: whether to copy as mysqldump or csv
        @type copy_format: {'sql' | 'csv'}
        @param logging_level: how much of the run to document
        @type logging_level: logging.INFO/DEBUG/ERROR/...
        @param unittests: set to True to do nothing significant, and let 
            unittests call methods in isolation.
        @type bool.
        @param: unittests_db_nm: Only relevant if unittests is True. Name of
            database where unittests will be performed.
        
        '''
        
        # Access to common functionality:
        self.utils = Utilities()
        self.config_info = ConfigInfo()
        
        self.unittests = unittests
        self.utils.setup_logging(logging_level)
        # For convenience:
        self.log_info = self.utils.log_info
        self.log_debug = self.utils.log_debug
        
        if user is None:
            if self.unittests:
                self.user = self.config_info.test_default_user
            else:
                self.user = self.config_info.default_user
        else:
            self.user = user
        
        if dest_dir is None:
            self.dest_dir = self.config_info.oracle_tbl_dest_dir
        else:
            if os.path.isfile(dest_dir):
                raise ValueError(f"Destination 'directory' {dest_dir} is a file.")
            if not os.path.exists(dest_dir):
                os.mkdir(dest_dir, mode=0o740)
            elif not os.access(dest_dir, os.W_OK):
                raise ValueError(f"Directory '{dest_dir}' is not writeable.")
            
            self.dest_dir = dest_dir
            
        if copy_format in ['csv', 'sql']:
            self.copy_format = copy_format
        else:
            raise ValueError(f"Only copy_format 'csv', and 'sql' are allowed; not {copy_format}")
        self.overwrite_existing = overwrite_existing
        
        if host is None:
            if self.unittests:
                self.host = self.config_info.test_default_host
            else:
                self.host = self.config_info.default_host
        else:
            self.host = host
            
        if self.unittests:
            self.src_db = unittest_db_name
        else:
            self.src_db = self.config_info.canvas_db_aux

        # Find the mysql executable. Normally not a problem,
        # but if running in Eclipse for debugging, the executable
        # isn't findable:
        self.mysql_path = self.utils.get_mysql_path()
        
        # The db obj:
        self.db = None
        
        # No schema created yet for this table.
        # We will create a read-only property
        # for this quantity: 
        self.__schema = None

        if db_pwd is None:
            db_pwd = self.utils.get_db_pwd(host, unittests=unittests)
        elif db_pwd == True:
            db_pwd = self.utils.get_db_pwd(host, ask_user= True, unittests=unittests)

        self.pwd = db_pwd
        
        if unittests:
            
            self.db = self.utils.log_into_mysql(self.user, 
                                                self.pwd, 
                                                db=self.src_db, 
                                                host=self.host
                                                )

            # Don't do anything further. Allow unittests
            # to call methods in isolation.
            return
        
        # If user wants only particular tables to be copied
        # (subject to overwrite_existing), then the tables arg will be
        # a list of table names. If tables is an empty list,
        # nothing will be copied: 
        
        if tables is None or len(tables) == 0:
            self.tables = self.utils.create_table_name_array() 
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
        
        self.completed_tables = self.get_existing_tables_in_dir(set(tables))

    #-------------------------
    # PROPERTY: schema 
    #--------------

    @property
    def schema(self):
        # Return this AuxTableCopier's Schema instance.
        # That instance can be used for getting details
        # about the table via schema[<col-name>] to get
        # SchemeColumn instances.
        
        return self.__schema

    #------------------------------------
    # copy_tables 
    #-------------------    
        
    def copy_tables(self, table_names=None, overwrite_existing=None):
        
        try:
            if table_names is None:
                table_names = self.tables
                
            if overwrite_existing is None:
                overwrite_existing = self.overwrite_existing
                
            if overwrite_existing:
                self.log_info(f"NOTE: will overwrite aux table-related files in {self.dest_dir}.")
            else:
                self.log_info(f"NOTE: only copying tables not already in {self.dest_dir}. Use --all to replace all.")
                
            existing_tables = self.get_existing_tables_in_dir(table_names)
            
            # Delete existing files, if requested to do so:
            if overwrite_existing:
                for table_name in existing_tables:
                    table_file = self.file_nm_from_tble(table_name)
                    os.remove(table_file)
                
                # Ready to copy all:
                tables_to_copy = set(table_names)
            else:
                tables_to_copy = set(table_names) - existing_tables
            self.log_info(f'Will copy {len(tables_to_copy)} table(s).') 
                
            table_to_file_map = {table_name : self.file_nm_from_tble(table_name) for table_name in tables_to_copy}

            self.db = self.utils.log_into_mysql(self.user, 
                                                self.pwd, 
                                                db=self.src_db, 
                                                host=self.host
                                                )

            if self.copy_format == 'csv':
                copy_result = self.copy_to_csv_files(table_to_file_map)
            else:
                copy_result = self.copy_to_sql_files(table_to_file_map)
    
            if copy_result.errors is not None:
                for (table_name, err_msg) in copy_result.errors.items():
                    self.utils.log_err(f"Error copying table {table_name}: {err_msg}") 
                
            if overwrite_existing:
                self.log_info(f"Copied all {len(copy_result.completed_tables)} tables to {self.dest_dir}. Done.")
            else:
                self.log_info(f"Copied {len(copy_result.completed_tables)} of all " +
                              f"{len(self.tables)} tables to {self.dest_dir}. Done")
                
            return copy_result
        finally:
            if self.db is not None:
                self.db.close()

    #------------------------------------
    # copy_to_sql_files 
    #-------------------
    
    def copy_to_sql_files(self, table_to_file_map):
        '''
        NOT TESTED or REVIEWED!
        
        @param table_to_file_map:
        @type table_to_file_map:
        '''
        
        raise NotImplemented("Only copy_to_csv_files is currently implemented.")
    
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
            pwd = self.utils.get_db_pwd(self.host, self.unittests) 
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
    
    def copy_to_csv_files(self, table_names):
        '''
        Copies all table_names tables to the destination
        directory, including their CREATE TABLE statements.
        
        @param table_names: names of tables whose contents to pull into dest_dir
        @type table_names: [str]
        @return: a CopyResult instance with tables copied, and errors encountered.
        @rtype CopyResult
        '''
        
        # Have to get schema for each table to make
        # the CSV header.
        
        table_schemas = [self.populate_table_schema(table_name) for table_name in table_names]
        copy_result = CopyResult()
        for table_schema in table_schemas:
            table_name = table_schema.table_name
            
            self.log_info(f"Copying {table_name} to {self.dest_dir}/{table_name}.tsv...")
            try:
                self.copy_one_table_to_csv(table_schema)
            except DatabaseError as e:
                # Rather than reporting each error spread out
                # across the log, report them all in the caller:
                # self.utils.log_err(f"Error exporting {table_name}: {e.message}.")
                copy_result.add_error(table_name, e)
                continue
            self.log_info(f"Done copying {table_schema.table_name}.")
            copy_result.add_completed_table(table_schema.table_name)

            self.log_info(f"Writing {table_name}'s schema to {self.dest_dir}/{table_name}_schema.sql")
            self.write_table_schema(table_schema)
            self.log_info(f"Done writing {table_name}'s schema to {self.dest_dir}/{table_name}_schema.sql")

        return copy_result

    #-------------------------
    # write_table_schema 
    #--------------
    
    def write_table_schema(self, table_schema):
        '''
        Writes a MySQL CREATE TABLE statement to
        the destination directory. Filename will be
        the table name with "_schema.sql" appended.
        
        @param table_schema: the table's Schema instance
        @type table_schema: Schema
        '''
        table_name = table_schema.table_name
        dest_file = os.path.join(self.dest_dir, table_name) + '_schema.sql'
        create_statement = table_schema.construct_create_table()
        self.log_info(f'Writing schema for table {table_name} to {dest_file}...')
        with open(dest_file, 'w') as fd:
            fd.write(create_statement)
            fd.write('\n')
        self.log_info(f'Done writing schema for table {table_name} to {dest_file}.')
            
        
    #-------------------------
    # copy_one_table_to_csv 
    #--------------
            
    def copy_one_table_to_csv(self, table_schema=None):
        '''
        Copy the table that populates the current
        schema to <self.dest_dir>/<table_name>
        
        Assume that self.schema contains a schema
        as a result of calling populate_table_schema()
        
        To copy another file, call populate_table_schema()
        again with a different table name, and then call
        this method again.

        @raise TableError: if cannot retrieve table schema.
        '''
        
        if table_schema is None:
            # Use the schema that is currently populated:
            table_schema = self.schema
                  
        table_name    = table_schema.table_name
        out_file_name = os.path.join(self.dest_dir, table_name) + '.tsv'
        
        # Array of col names for the header line.
        # The csv writer will add quotes around the col names:
        col_name_arr = table_schema.col_names(quoted=False)
        field_list   = [f'{col_name}' for col_name in col_name_arr]
        if len(field_list) == 0:
            raise TableError((table_name,None),
                             f"Table {table_schema.table_name} has no metadata " +
                             f"(likely does not exist in db {self.config_info.canvas_db_aux}).")
        shell_script = os.path.join(os.path.dirname(__file__), 'call_mysql.sh')
        
        # Tell shell script where to find the MySQL pwd:
        if self.unittests and self.host == 'localhost':
            pwd_file_pointer = ''
        else:
            pwd_file_pointer = self.config_info.canvas_pwd_file

        # Ensure the destination for the tsv file does not
        # exist at the outset:
        
        if os.path.exists(out_file_name):
            os.remove(out_file_name)

        # Write the column names at the top:            
        field_list_str = '\t'.join(field_list)
        with open(out_file_name, 'w') as out_fd:
            out_fd.write(f"{field_list_str}\n")
        
        mysql_cmd = f'''SELECT {', '.join(field_list)}
                          FROM {table_name};
                     '''
           
        retrieve_parms = OrderedDict({
            'shell_script' : shell_script,
            'host'         : self.host,
            'user'         : self.user,
            'pwd_file_ptr' : pwd_file_pointer,
            'src_db'       : self.src_db,
            'mysql_path'   : self.mysql_path,
            'out_file_name': out_file_name,
            'mysql_cmd'    : mysql_cmd      # Method pull_by_account_id() relies on this being last!
            })
        
        
        # Tables other than AssignmentSubmissions are small enough
        # that they can be output to tsv in one call to MySQL. But
        # beyond some limit of rows the MySQL server disconnects
        # in protest, unless its configuration allows larger chunks.
        # To leave the MySQL server's default settiungs, we chop
        # retrieval up for AssignmentSubmissions:
                
        if table_name == 'AssignmentSubmissions': 
            # Get account numbers, and pull just rows of
            # one account number at a time. Accumulate into the temp file,
            # then transfer to the final .tsv:
            self.pull_by_account_id(retrieve_parms, table_name, field_list, out_file_name)
        elif table_name == 'GradingProcess':
            # Pull only the records since AuxTableCopier.GRADING_PROCESS_START_YEAR
            self.pull_by_term_year(retrieve_parms, table_name, field_list, out_file_name)
        elif table_name == 'AllUsers':
            # Pull batches by autoincrement sequence numbers,
            # b/c else MySQL server balks:
            self.pull_by_seq_num(retrieve_parms, table_name, field_list, out_file_name)
        else:
            retrieve_stmt_arr = [val for val in retrieve_parms.values()]
            _completed_process = subprocess.run(retrieve_stmt_arr, 
                                                #capture_output=True, # Only for debugging 
                                                shell=False)
    
            if _completed_process.returncode != 0:
                raise DatabaseError(f"Call to MySQL '{mysql_cmd[:20]}...' failed")
            
        # Sanity check: is tsv file empty:
        tsv_file_name = retrieve_parms['out_file_name']
        tsv_path = Path(tsv_file_name)
        if tsv_path.stat().st_size == 0:
            raise DatabaseError(f"Destination file {tsv_path} is empty; table {table_name} retrieval failed.")
        
    #-------------------------
    # pull_by_seq_num
    #--------------

    def pull_by_seq_num(self, retrieve_parms, table_name, field_list, out_file_name):
        '''
        For the very large AllUsers table, pull records in batches
        of AuxTableCopier.SEQ_NUM_BATCH_SIZE rows. Assume that out_file_name
        is empty or does not exist. 
        
        @param retrieve_parms: ordered dict of parameters to pass to the 
            call_mysql.sh script
        @type retrieve_parms: {str : <any>}
        @param table_name: name of table to pull from
        @type table_name: str
        @param field_list: list of column names to retrieve in proper order
        @type field_list: [str]
        @param out_file_name: path to the .tsv file where the data is to land
        @type out_file_name: str
        '''
        
        col_names = ','.join(field_list)
        
        # For each seq number range, pull the corresponding
        # rows, and put them into a .tsv tmp file.
        
        # Find max row number:
        
        max_row = self.db.query("SELECT MAX(seq_num) FROM AllUsers").next()
        # For convenience:
        batch_size = AuxTableCopier.SEQ_NUM_BATCH_SIZE

        for seq_num in range(1, max_row, batch_size):
            mysql_cmd = f'''
                      SELECT {col_names}
                        FROM {table_name}
                    WHERE seq_num BETWEEN {seq_num} AND {seq_num + batch_size-1};
                      '''
            retrieve_parms['mysql_cmd'] = mysql_cmd
            retrieve_stmt_arr = retrieve_stmt_arr = [val for val in retrieve_parms.values()]
            _completed_process = subprocess.run(retrieve_stmt_arr, #capture_output=True, # Only for debugging
                shell=False)
            if _completed_process.returncode != 0:
                raise DatabaseError(f"Call to MySQL '{mysql_cmd[:20]}...' failed")
            
            self.log_info(f"Pulled batch of (up to) {batch_size} rows from table {table_name}")

    #-------------------------
    # pull_by_term_year 
    #--------------

    def pull_by_term_year(self, retrieve_parms, table_name, field_list, out_file_name):
        '''
        For the very large GradingProcess table, pull 
        only records since AuxTableCopier.GRADING_PROCESS_START_YEAR
        
        @param retrieve_parms: ordered dict of parameters to pass to the 
            call_mysql.sh script
        @type retrieve_parms: {str : <any>}
        @param table_name: name of table to pull from
        @type table_name: str
        @param field_list: list of column names to retrieve in proper order
        @type field_list: [str]
        @param out_file_name: path to the .tsv file where the data is to land
        @type out_file_name: str
        '''
        # Convenience copy:
        start_year = AuxTableCopier.GRADING_PROCESS_START_YEAR
        
        # Create table with all term_id numbers since the
        # year from which we wish to pull records. Term_name format
        # in Terms table is 'Winter 2016'. The substring() call below
        # extracts the year from these entries:
        
        self.log_info(f"Getting list of enrollment_term_id since {start_year}...")
        term_ids = self.db.query(f'''
								  SELECT term_id                    
								    FROM Terms
								   WHERE SUBSTRING_INDEX(term_name, ' ', -1) >= {start_year};        
                                '''
        )
        
        if self.db.result_count() == 0:
            raise DatabaseError("No data in Terms table (detected during GradingProcess enrollment_term_id select).")
        
        enrollment_term_ids = [str(term_id) for term_id in term_ids]
        self.log_info(f"Done getting list of enrollment_term_id since {start_year}...") 
        
        # For each enrollment_term_id, retrieve each
        # account separately to keep the MySQL server from
        # croaking due to the number of rows being retrieved:

        for enrollment_term_id in enrollment_term_ids:
            and_clause = f'''AND enrollment_term_id = {enrollment_term_id}'''
            
            retrieve_parms['mysql_clause'] = and_clause
            self.pull_by_account_id(retrieve_parms, table_name, field_list, out_file_name)

    #-------------------------
    # pull_by_account_id
    #--------------
    
    def pull_by_account_id(self, retrieve_parms, table_name, field_list, out_file_name):
        '''
        For the very large AssignmentSubmissions table we
        need to pull rows in batches if mysql server is left
        at default config values. Find those chunks, pull them
        into a temp .tsv file, and transfer them to the final .tsv file
        piece by piece. 
        
        @param retrieve_parms: ordered dict of parameters to pass to the 
            call_mysql.sh script (see header comment for method pull_from_account_list()
        @type retrieve_parms: {str : <any>}
        @param table_name: name of table to pull from
        @type table_name: str
        @param field_list: list of column names to retrieve in proper order
        @type field_list: [str]
        @param out_file_name: path to the .tsv file where the data is to land
        @type out_file_name: str
        '''
        
        # Get list of AccountIdCollection instances. Each
        # will have a list of account numbers whose rows
        # we are to get:
        
        self.log_info(f"Getting list of account_ids for table {table_name}...")
        account_id_seq_objs = self.utils.get_account_ids_from_table(self.db, table_name)
        self.log_info(f"Found {len(account_id_seq_objs)} distinct account_ids in {table_name}")
        
        col_names = ','.join(field_list)
        
        self.pull_from_account_list(table_name, retrieve_parms, out_file_name, account_id_seq_objs, col_names)
            
    #-------------------------
    # pull_from_account_list 
    #--------------
    
    def pull_from_account_list(self, table_name, retrieve_parms, out_file_name, account_id_seq_objs, col_names):
        '''
        Given appropriate information, including a list of
        AccountIdCollection instances, pull all records with the
        given account ids into a tsv file.
        
        The retrieve_parms dict (see below) may contain a
        'mysql_clause' key, which adds an AND clause to the
        sql used. Example, as this method cycles through 
        pulling each batch of account_id_seq that are in 
        list account_id_seq_objs, the following query might
        be constructed:
        
        SELECT account_id,course_id, ...
		  FROM GradingProcess
		 WHERE account_id IN (35910000000000030)
		   AND enrollment_term_id = 35910000000000022;
		   
		The portion up to and including the WHERE clause gets
		constructed in each run through the loop below, 
		changing the range of account_id's each time. But
		the   
		      "AND enrollment_term_id = 35910000000000022;"
		      
		was passed in as the mysql_clause, and will be constant.
		
		Note: this method check whether any content is already
		in out_file_name. If not, the first line of the first loop's
		db result will be written to the out_file_name. But if
		the out_file_name is already partially filled, the first
		line of each query result (i.e. the header) will be discarded.
        
        This method may be called multiple times with different
        AND clauses.
        
        @param table_name: name of table from which to pull
        @type table_name: str
        @param retrieve_parms: data structure describing the query parms:
           OrderedDict({
            'shell_script' : shell_script,    # usually full path to call_mysql.sh
            'host'         : host,            # MySQL host
            'user'         : user,            # MySQL user
            'pwd_file_ptr' : pwd_file_pointer,# path to file with MySQL password
            'src_db'       : src_db,          # database in MySQL server that contains table_name
            'mysql_path'   : mysql_path,      # local path to the mysql client binary
            'tmp_file_name': tmp_file_name,   # the .tsv file name in which to put result
            'mysql_clause' : mysql_cmd        # if not Null, then a MySQL AND clause to 
                                              #   add to the WHERE account id in <range>... 
            })
        @type retrieve_parms: OrderedDict
        @param out_file_name: path to the final .tsv file
        @type out_file_name: str
        @param account_id_seq_objs: list of 
        @type account_id_seq_objs: [AccountCollection]
        @param col_names: comma-separated list of column names to include in output
        @type col_names: str
        '''
        
        # For each seq of account_id, pull the corresponding
        # rows, and put them into a .tsv tmp file. So, for
        # accounts with lots of rows, we'll pull just rows for
        # that account. For accounts with smaller number or rows,
        # we pull all the rows from a few account_ids together.
        
        # Get the MySQL AND clause:
        and_clause = retrieve_parms.get('mysql_clause', None)
        # Don't want AND clause in the dict any more:
        if and_clause is not None:
            del retrieve_parms['mysql_clause']

        for account_id_seq_obj in account_id_seq_objs:
            range_str = ','.join([str(account_id) for account_id in account_id_seq_obj.account_ids])
            mysql_cmd = f'''
                      SELECT {col_names}
                        FROM {table_name}
                    WHERE account_id IN ({range_str})
                      '''
            # Add the AND clause if one was given:
            if and_clause is not None:
                mysql_cmd += and_clause + ';'
            
            retrieve_parms['mysql_cmd'] = mysql_cmd
            retrieve_stmt_arr = retrieve_stmt_arr = [val for val in retrieve_parms.values()]
            _completed_process = subprocess.run(retrieve_stmt_arr, #capture_output=True, # Only for debugging
                shell=False)
            if _completed_process.returncode != 0:
                raise DatabaseError(f"Call to MySQL '{mysql_cmd[:20]}...' failed")
            
            self.log_info(f"Pulled {account_id_seq_obj.num_rows} rows from table {table_name}")
                
    #-------------------------
    # populate_table_schema 
    #--------------
    
    def populate_table_schema(self, table_name):
        '''
        Return a Schema instance that reflects the 
        table whose name is passed in.
        
        Assumption: self.db holds a MySQLDB instance.
        
        @param table_name: aux table name whose creation SQL is to be produced
        @type table_name: str
        @return: a Schema instance that describes the table
        @rtype: Schema
        @raise RuntimeError: when MySQL database cannot be contacted. 
        '''
        # Ensure that the information schema is updated
        # for table table_name:
        self.db.execute(f'ANALYZE TABLE {table_name}')
        
        table_metadata_cmd = f'''SELECT column_name, data_type, character_maximum_length, column_default, ordinal_position, extra 
                                   FROM information_schema.COLUMNS 
                                  WHERE table_schema = '{self.src_db}' 
                                    AND table_name = '{table_name}';
                            '''
        schema_obj = Schema(table_name)
        
        table_metadata = self.db.query(table_metadata_cmd)
        for (col_name, col_type, col_max_len, col_default, position, is_auto_increment) in table_metadata:
            # Add info about one column to this schema:
            schema_obj.push(col_name, 
                            col_type,
                            col_max_len, 
                            col_default, 
                            position, 
                            col_is_auto_increment=(True if is_auto_increment.lower()=='auto_increment' else False))
    
        # For each column (i.e. SchemaColumn instance): if the col has 
        # an index, create SchemaIndex that defines the schema, and add 
        # it as the 'index' property to the SchemaColumn.
        # The 'sub_part' column is an the length of the index.
        # The data types 'text' and 'blob' require specifying a
        # length if an index is built on them.
        
        index_metadata_cmd = f'''SELECT index_name, column_name, seq_in_index, sub_part
                                   FROM information_schema.statistics
                                  WHERE TABLE_SCHEMA = '{self.src_db}'
                                    AND TABLE_NAME   = '{table_name}';  
                              '''
        idx_info = self.db.query(index_metadata_cmd)
        
        for (index_name, col_name, seq_in_index, index_length) in idx_info:
            
            schema_col_obj = schema_obj[col_name]
            schema_idx_obj = SchemaIndex(index_name, col_name, seq_in_index, index_length)
            schema_col_obj.index = schema_idx_obj
            
        self.__schema = schema_obj
            
        return schema_obj
    
    #-------------------------
    # close_db 
    #--------------
    
    def close_db(self):
        if self.db is not None and self.db.isOpen():
            self.db.close()
            self.db = None
         
    #-------------------------
    # close 
    #--------------
         
    def close(self):
        '''
        Release resources.
        '''
        self.log_info(f'Copier close(): Closing db at {self.host}...')
        try:
            self.close_db()
        except AttributeError:
            # Called before self.db was initialized
            pass
        self.log_info(f'Done Copier close(): Closing db at {self.host}.')
         
    #------------------------------------
    # tables_to_copy 
    #-------------------    

    def tables_to_copy(self, table_list_set=None):
        '''
        Given a set of table names, check which of those correspond
        to files in the copy target directory. Either as .tsv or .sql.
        Return a subset of the provided table_list_set, with tables
        that do not have an associated file.
        
        By default, the set to check is all aux tables.
        
        @param table_list_set: names of tables whose status is to be checked. 
        @type table_list_set: set(str)
        '''
        
        if table_list_set is None:
            table_list_set = set(self.tables)
        
        tables_done_set = self.get_existing_tables_in_dir(table_list_set)
        tables_to_do_set = table_list_set - tables_done_set
        return tables_to_do_set
        
    #------------------------------------
    # get_existing_tables_in_dir 
    #-------------------    
        
    def get_existing_tables_in_dir(self, table_name_set):
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
        # '/tmp/trash.txt' ==> ('/tmp/trash', '.txt') ==> trash:
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
    # file_nm_from_tble 
    #--------------
    
    def file_nm_from_tble(self, tbl_nm):
        return os.path.join(self.dest_dir, tbl_nm) + '.sql' \
            if self.copy_format == 'sql' \
            else os.path.join(self.dest_dir, tbl_nm) + '.tsv'

   
# -------------------------- Class CopyResult ---------------


class CopyResult(object):

    #-------------------------
    # Constructor 
    #--------------

    def __init__(self):
        self._completed_tables = []
        # Dict {table_name : err_msg}
        self._errors = {}
    
    #-------------------------
    # add_error 
    #--------------
    
    def add_error(self, tbl_name, error_obj):
        '''
        Add a table-copy error to the record.
        
        @param tbl_name: name of table whose copying failed
        @type tbl_name: str
        @param error_obj: an Exception subclass instance
        @type error_obj: Exception
        '''
        self._errors[tbl_name] = error_obj.message

    #-------------------------
    # add_completed_table
    #--------------
    
    def add_completed_table(self, table_name):
        '''
        Record a completed table.
        
        @param table_name: name of table that was successfully copied.
        @type table_name: str
        '''
        self._completed_tables.append(table_name)

    #-------------------------
    # no_errors
    #--------------

    def no_errors(self):
        return len(self._errors) == 0
        
    #-------------------------
    # error_msg_list 
    #--------------
    
    @property
    def errors(self):
        '''
        
        @return: None if now errors were recorded, else dict {table_name : errorMmsg}
        @rtype: {None | {str : str}}
        '''
        
        if self.no_errors():
            return None
        else:
            return self._errors
    
    #-------------------------
    # completed_tables 
    #--------------
    
    @property
    def completed_tables(self):
        return self._completed_tables
    
# ------------------------------------------------  Class Schema -----------------------
    
class Schema(collections.abc.MutableMapping):
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
        
        self.__table_name  = table_name
        self.column_dict = OrderedDict()
        
        self.update(dict(*args, **kwargs))
        
    #-------------------------
    # table_name 
    #--------------
    
    @property
    def table_name(self):
        return self.__table_name
    
    #-------------------------
    # push 
    #--------------
    
    def push(self, col_name, 
                   col_type, 
                   col_max_len=None,
                   col_default=None,
                   col_position=None,
                   col_is_auto_increment=False
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
        @param col_max_len: for varchar type: the max length of the values
        @type col_max_len: int 
        @param col_default: default value of column
        @type col_default: str
        @type col_default: str
        @param col_position: position of the column in the CREATE TABLE statement
        @type col_position: int
        @param col_is_auto_increment: True if this column is auto_increment.
        @type col_is_auto_increment: bool
        '''
        # Need to ensure that col position is defined,
        # because that attr is used to sort col defs in
        # generated CREATE TABLE statements:
        num_cols = len(self.column_dict)
        if col_position is None:
            col_position = num_cols + 1
        # If we are inserting a col, rather than just appending,
        # renumber cols after the inserting point:
        if col_position < num_cols + 1:
            for col_obj in self.column_dict.values():
                if col_obj.col_position >= col_position:
                    col_obj.col_position += 1 
        self.column_dict[col_name] = SchemaColumn(col_name, col_type, col_max_len, col_default, col_position, col_is_auto_increment)
    
    #-------------------------
    # add_index 
    #--------------
    
    def add_index(self, index_name, col_name, seq_in_index=1, index_length=None):
        '''
        Add one index to this column instance. The seq_in_index
        parameter is relevant only for composite indexes:
        
           CREATE INDEX foo_idx ON MyTable(col1, col2)
           
        The col1 index would have seq_in_index = 1, that of col2 
        would be 2. 
        
        @param index_name: name of the index as in the CREATE INDEX statement
        @type index_name: str
        @param col_name: name of column on which the index is defined
        @type col_name:
        @type str
        @param seq_in_index: the position the column has in the index
        @type seq_in_index: int
        @param index_length: length of index in case of 'text' or 'blob' col
        @type index_length: int
        '''
        
        index_obj = SchemaIndex(index_name, col_name, seq_in_index, index_length)
        self.column_dict[col_name].index = index_obj

    #-------------------------
    # col_names 
    #--------------

    def col_names(self, quoted=True):
        '''
        Return an array of quoted or unquoted column names 
        of this table. The names will be ordered by how
        they appeared in the table's original CREATE TABLE
        statement.
        
        Output example with quoted == True:  ['"foo"','"bar"']
                            quoted == False: ['foo','bar']
        
        @param quoted: if True, each column name in the returned array will
            have a double quote char around it. 
        @type quoted: boolean
        @return: sorted list of column names; quoted or unquoted
        @rtype: [str]
        '''
        attr_getter = lambda col_obj: col_obj.col_position
        # dict.values() returns a special odict_list structure.
        # Must turn that into a normal array; therefore the 'list()':
        col_objs = list(self.column_dict.values())
        # Sort the col objs in place to mirror their 
        # position in the create table statement: 
        col_objs.sort(key=attr_getter)
        if quoted:
            res = [f'"{col_obj.col_name}"' for col_obj in col_objs]
        else:
            res = [f'{col_obj.col_name}' for col_obj in col_objs]
            
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
        
        index_objs = OrderedDict()
        
        # Function for sorting a list of index objects by 
        # their seq_in_index property:
        attr_getter = lambda col_obj: col_obj.col_position
        col_objs = list(self.column_dict.values())
        col_objs.sort(key=attr_getter)
        
        for schema_col_obj in col_objs:
            # Add one line to the CREATE TABLE stmt for this column:
            if schema_col_obj.col_default is None:
                create_stmt += f"\t{schema_col_obj.col_name}  {schema_col_obj.col_type}"
                if schema_col_obj.col_type == 'varchar':
                    # Add length of the varchar:
                    create_stmt += f"({schema_col_obj.col_max_len})"
            else:
                create_stmt += f"\t{schema_col_obj.col_name}  {schema_col_obj.col_type}"
                if schema_col_obj.col_type == 'varchar':
                    # Add length of the varchar:
                    create_stmt += f"({schema_col_obj.col_max_len})"
                create_stmt += f" DEFAULT {schema_col_obj.col_default}"
            
            if schema_col_obj.col_is_auto_increment:
                create_stmt += ' AUTO_INCREMENT'
            # Close this col def line:
            create_stmt += ',\n'
            if schema_col_obj.index is not None:
                # Get this column's index obj:
                idx_obj = schema_col_obj.index
                try:
                    # Add it to the dict where we collect
                    # index objs with the same name:
                    index_objs[idx_obj.idx_name].append(idx_obj)
                except KeyError:
                    # Was first index:
                    index_objs[idx_obj.idx_name] = [idx_obj]
        
        create_stmt = self._add_indexes_to_create_statement(create_stmt, index_objs)
        # Replace the trailing comma with a closing paren and semicolon:
        # The -2 skips over the already placed \n to the comma:
        create_stmt = create_stmt[:-2] + ');'
        
        return create_stmt

    #-------------------------
    # _add_indexes_to_create_statement 
    #--------------
    
    def _add_indexes_to_create_statement(self, statement_so_far, index_objs):
        '''
        Given the column definitions part of a CREATE TABLE
        statement, add the indexes. Ex.:
           --------------- 
            CREATE TABLE Unittest (
              id int NOT NULL AUTO_INCREMENT,
              var1 int,
              var2 int DEFAULT 10,
              var3 varchar(40) DEFAULT NULL,
           ---------------- The text above is passed in
           The following is added in this method:
              PRIMARY KEY (id),
              KEY var3_idx (var3),
              KEY var1_2_idx (var1,var2);        
        
        @param statement_so_far: partially constructed CREATE TABLE statement
        @type statement_so_far: str
        @param index_objs: dict of index name ===> list index objects
        @type index_objs: {str : [SchemaIndex]}
        '''
        
        if len(index_objs) == 0:
            return(statement_so_far)
              
        create_stmt = statement_so_far
        # Function to fetch one Index sequence position:
        attr_getter = lambda idx_obj: idx_obj.seq_in_index
        
        # Go through the indexes we might have found for some
        # of the columns, and add them to the end of the create stmt.
        # the SchemaIndex instances list for each index name
        # may be longer than 1, in which case we have a composite index: 
        
        for idx_obj_array in index_objs.values():

            # If this is a composite index, there will
            # be more than one index obj. Sort those 
            # by their position in the composite.
            # See var_1_2_idx in header example:
              
            idx_obj_array.sort(key=attr_getter)
            wrote_key_name = False
            for idx_obj in idx_obj_array:
                if not wrote_key_name:
                    # The part "...PRIMARY KEY ("
                    # Or "KEY ...tbl_col_idx (": 
                    if idx_obj.idx_name.upper() == 'PRIMARY':
                        create_stmt += f"\tPRIMARY KEY ("
                    else:
                        create_stmt += f"\tKEY {idx_obj.idx_name} ("
                    wrote_key_name = True
                create_stmt += f"{idx_obj.col_name}"
                # If there is an index length (as will be for 'test' and
                # 'blob' cols, then add "(<length>)" after the col name now:
                if idx_obj.index_length is not None:
                    create_stmt += f"({idx_obj.index_length}),"
                else:
                    create_stmt += f","
                
            # Close this index decl: add '),\n to 
            # "KEY foo_idx (col1,...coln,"
            # Replace the last comma with '),\n':
            create_stmt = create_stmt[:-1] + '),\n'
        # Chop off the '\n', and replace with closing
        # paren and NL:
        create_stmt = create_stmt[:-3] + '),\n'
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
    
    def __init__(self, col_name, col_type, col_max_len, col_default, position, is_auto_increment):
        self.__col_name     = col_name
        self.__col_type     = col_type
        self.__col_max_len  = col_max_len
        self.__col_default  = col_default
        self.__col_position = position
        self.__is_auto_increment = is_auto_increment
        self.__index        = None

    def __str__(self):
        return f"<column {self.col_name} pos {self.col_position}>"
                
    @property
    def col_name(self):
        return self.__col_name
    
    @property
    def col_type(self):
        return self.__col_type

    @property
    def col_max_len(self):
        return self.__col_max_len
        
    @property
    def col_position(self):
        return self.__col_position

    @col_position.setter
    def col_position(self, new_val):
        self.__col_position = new_val

    @property
    def col_default(self):
        return self.__col_default

    @property
    def col_is_auto_increment(self):
        return self.__is_auto_increment
    
    @property
    def index(self):
        return self.__index
    
    @index.setter
    def index(self, schemaIndex_instance):
        if not isinstance(schemaIndex_instance, SchemaIndex):
            raise TypeError("The index information in SchemaColumn instances must be a SchemaIndex instance.")
        self.__index = schemaIndex_instance

        
# -------------------------- SchemaIndex Class ---------------------        
            
class SchemaIndex(object):
    
    def __init__(self, idx_name, col_name, seq_in_index, index_length):
        self._idx_name = idx_name
        self._col_name = col_name
        self._seq_in_index = seq_in_index
        self._idx_length = index_length
    
    def __str__(self):
        return(f"<{self.idx_name} on {self.col_name} pos {self.seq_in_index}>")
    @property
    def idx_name(self):
        return self._idx_name
    
    @property
    def col_name(self):
        return self._col_name
    
    @property
    def seq_in_index(self):
        return self._seq_in_index

    @property
    def index_length(self):
        return self._idx_length
            
            


# --------------------------- Main ------------------        
if __name__ == '__main__':

    config_info = ConfigInfo()
        
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Copy one of more auxiliary tables from AWS production to /tmp/<TableName>.csv"
                                     )

    parser.add_argument('-u', '--user',
                        help=f'user name for logging into the canvas database.\n' +
                             f'Default: {config_info.default_user}',
                        default=config_info.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database.\n' +
                             f'Default: content of {config_info.canvas_pwd_file}',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-o', '--host',
                        help='host name or ip of database.\n' +
                            f'Default: {config_info.default_host}',
                        default=f'{config_info.default_host}')
                        
    parser.add_argument('-d', '--destdir',
                        help='directory where tables will be deposited locally',
                        default=None)
                        
    parser.add_argument('-f', '--format',
                        choices=['csv'],
                        help='format for local file.',
                        default='csv')
    
    parser.add_argument('-r', '--remove',
                        help='if set, remove already locally existing files. Default: do remove existing files',
                        action='store_true',
                        default=True)

    parser.add_argument('-t', '--table',
                        nargs='+',
                        help='Name of one or more specific table(s) to copy. Default: all tables in aux',
                        default=[]
                        )
    
    parser.add_argument('-l', '--loglevel',
                        choices=['info','debug','warning','error'],
                        help="Level of logging messages. Default: 'info'",
                        default=logging.INFO
                        )

    args = parser.parse_args()

    # Translate the logging level to official
    # logging constants:
    log_levels = {'info' : logging.INFO,
                  'debug': logging.DEBUG,
                  'warning': logging.WARN,
                  'error': logging.ERROR
                  }
    if isinstance(args.loglevel, str):
        args.loglevel = log_levels[args.loglevel]
        
    # copier = AuxTableCopier(tables=['Terms'])
    try:
        copier = AuxTableCopier(user=args.user,
                                db_pwd=args.password,
                                host=args.host,
                                dest_dir=args.destdir,
                                tables=args.table,
                                copy_format=args.format,
                                overwrite_existing=args.remove,
                                logging_level=args.loglevel    
                                )
        copy_result = copier.copy_tables()
    except KeyboardInterrupt:
        print("\nCanvas aux table copy stopped by user.")    
    
    
        
