#!/usr/bin/env python
'''
Created on May 13, 2019

@author: paepcke
'''
import argparse
import logging
import os
import sys

from canvas_utils_exceptions import DatabaseError
from utilities import Utilities
from config_info import ConfigInfo


class TableRestorer(object):
    '''
    classdocs
    '''

    #------------------------------------
    # Constructor 
    #-------------------    

    def __init__(self, 
                 user=None, 
                 pwd=None, 
                 target_db=None, 
                 host=None,
                 tables=[],
                 force=False,
                 logging_level=logging.INFO,
                 unittests=False):
        '''
        Constructor
        '''
        
        self.config_info = ConfigInfo()
        
        # Access to common functionality:
        self.utils = Utilities()
        
        if target_db is None:
            target_db = self.config_info.canvas_db_aux
        
        # Unittests expect a db name in self.db:
        self.db = target_db

        self.utils.setup_logging(loggingLevel=logging_level)
        self.db_obj = self.utils.log_into_mysql(user, pwd, db=target_db, host=host)

        if unittests:
            self.db_name = target_db
            return
        
        # Get names of all tables in the target_db
        all_tables = self.utils.get_existing_tables_in_dir(self.db_obj, 
                                                           return_all=True, 
                                                           target_db=target_db)
        
        # If only explicitly named tables are to be restored,
        # remove all others from the all_tables list. Also
        # check whether all the requested tables are actually
        # aux tables:
        # If caller didn't name any tables, no need for
        # the following loop:

        if len(tables) > 0: 
            all_tbls_copy = all_tables.copy()
            for candidate_tbl in all_tbls_copy:
                if not candidate_tbl in tables:
                    all_tables.remove(candidate_tbl)
                
        # If told not to overwrite existing aux
        # tables, remove those, and their backups
        # from all_tables:
        if not force:
            all_tables = self.remove_table_groups(all_tables)
            
        # If particular tables are to be restored,
        # remove all unrelated tables:
        
        self.restore_tables(target_db, all_tables, force)
        
    #------------------------------------
    # restore_tables 
    #-------------------
    
    def restore_tables(self, table_names=None, target_db=None):
        '''
        Restores tables from backups. Backup tables are assumed to be
        in the same db schema as the root tables. I.e. Terms.2019_...
        is in the same db as Terms.
        
        @param table_names: list of table names to restore. List may
            be a mix of root and backup tables names. See restore_from_backup
            in CanvasPrep.
            If None, all tables with backup tables will be restored.
        @type table_names: [str]
        @param target_db: name of db where backup and root tables reside.
        @type target_db: str
        @return: list of tables
        @rtype: [str]
        '''

        if table_names is None:
            table_names = self.utils.tables
        
        self.restore_from_backup(table_names, db_schema=target_db)
        
        # Get the aux table names that exist now, after the restore:
        tbls_now = self.utils.get_existing_tables_in_dir(self.db_obj, return_all=False, target_db=target_db)
        return tbls_now
            
                
    #------------------------------------
    # remove_table_groups
    #-------------------    

    def remove_table_groups(self, all_tables, table_names_to_keep=[]):
        '''
        Terminology: a table name that denotes an aux table is
                     called a table root name. For example, 
                     AssignmentSubmissions is a root name. 
                     
                    Backup table names are root names, followed by
                    an underscore and a date-time. Example:
                    AssignmentSubmissions_2019_01_20_14_23_30_123456 is
                    a backup name.
                    
                    A set of table names that share a root name, including
                    the root name itself is called a table family.
                    
        Given a list of table names:
           o Find names that denote aux tables, such as
             Terms, AssignmentSubmissions.
           o If those names are not in table_names_to_keep, then
             remove them from the given names list
           o If removed in step above, Remove all corresponding backup 
             table names as well from the given list. 
           o Return the new list
           o If a backup table name is in table_names_to_keep,
             only that backup table name is retained of its family
             
        Used to exclude already existing aux tables from 
        being replaced by their backups. 
        
        @param all_tables: list of all table names to consider
        @type all_tables: [str]
        @param table_names_to_keep: if provided, the table names
               will be restored, and no others.
        @type table_names_to_keep: [str]
        @return: list of table names with all aux table names removed,
                 and their backup table names removed as well
        '''
        all_tables_copy = all_tables.copy()
        for tbl_nm in all_tables_copy:
            # See whether table name is to be kept:
            # Is this a non-backup aux table?
            if self.utils.is_aux_table(tbl_nm):
                # Found an aux table. Is it in the names-to-keep list?
                if tbl_nm in table_names_to_keep:
                    continue
            elif self.utils.is_backup_name(tbl_nm):
                # Found a backup table. If its root name is in 
                # the keep list, keep the name in:
                if self.utils.get_root_name(tbl_nm) in table_names_to_keep or\
                    tbl_nm in table_names_to_keep:
                    continue
                
            # Found a table name that is not to be spared.
            # Remove the name
            all_tables.remove(tbl_nm)
        return all_tables

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
            db_schema = self.config_info.canvas_db_aux
              
        if type(table_root_or_backup_names) != list:
            table_root_or_backup_names = [table_root_or_backup_names]
        
        for table_name in table_root_or_backup_names:
            
            # Get (aux_tbl_name, data-str, datetime obj) if 
            # table_name is an aux table backup; else get False:
            tbl_nm_components = self.utils.is_backup_name(table_name) 
            if tbl_nm_components:
                
                # Got an explicit backup filename to restore:
                (root_name, _date_str, _datetime_obj) = tbl_nm_components
                
                if not self.table_exists(table_name):
                    raise ValueError(f"Table {table_name} does not exist, so cannot be restored to the working copy.")
                
                self.db_obj.dropTable(root_name)
                (err, _warn) = self.db_obj.execute(f"RENAME TABLE {table_name} TO {root_name};")
                if err is not None:
                    raise DatabaseError(f"Cannot restore table {table_name} to table {root_name}: {repr(err)}")
                
                continue
            elif table_name in self.utils.tables:
                # Table is a root name; find the most recent backup:
                res = self.db_obj.query(f'''
                                         select table_name 
                                           from information_schema.tables
                                          where table_schema = '{db_schema}'
                                            AND table_name like '{table_name}%';   
                                        ''')
                backup_tbl_names = [tbl_name for tbl_name in res if self.utils.is_backup_name(tbl_name)]

                if len(backup_tbl_names) == 0:
                    # No backup table available.
                    self.utils.log_warn(f"No backup table found for table '{table_name}'.")
                    # Next table name to restore:
                    continue
                
                backup_tbl_names = self.utils.sort_backup_table_names(backup_tbl_names)
                
                # Do the rename using the first (and therefore youngest backup 
                # table name:
                self.db_obj.dropTable(table_name)
                self.db_obj.execute(f'''RENAME TABLE {backup_tbl_names[0]} TO {table_name};''')

    #-------------------------
    # close  
    #--------------

    def close(self):
        try:
            self.db_obj.close()
        except Exception:
            pass

# ----------------------------- Main ------------------------

if __name__ == '__main__':
    
    config_info = ConfigInfo()
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Restore backed-up aux tables."
                                     )

    parser.add_argument('-f', '--force',
                        help="restore *all* aux tables from available backups, or only non-existing tables; default=False",
                        action='store_true',
                        default=False);
                        
    parser.add_argument('-u', '--user',
                        help=f'user name for logging into the canvas database.' +
                             f'Default: {config_info.default_user}',
                        default=config_info.default_user)
                        
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
                        help='Name of one or more specific table(s) to restore.',
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
    
    TableRestorer(user=args.user,
                  pwd=args.password, 
                  target_db=args.database, 
                  host=args.host,
                  tables=args.tables,
                  force=args.force,
                  logging_level=logging.INFO,
                  unittests=False)
    