#!/usr/bin/env python
'''
Created on Aug 20, 2019

@author: paepcke
'''

import argparse
import logging
import os
import sys

from utilities import Utilities
from config_info import ConfigInfo


class BackupRemover(object):
    '''
    Utility to remove all but a given number of
    table backups from the aux directory.
    '''

    default_num_backups_to_keep = 2
    #------------------------------------
    # Constructor 
    #-------------------    

    def __init__(self,
                 num_to_keep=None,
                 user=None, 
                 pwd=None, 
                 target_db=None, 
                 host=None,
                 tables=[],
                 logging_level=logging.INFO,
                 unittests=False):
        '''
        Most parameters have defaults that can be
        set once and for all. The main params that
        might change are num_to_keep, and tables.
        
        The num_to_keep integer value declares how many
        of the newest backup tables to keep for each table.
        
        The tables list of table names may contain a mix
        of table root names (e.g. AssignmentSubmission, Terms),
        and backup table names (e.g. Terms_2019_01_10_14_14_40_123456)
        For root names, all backup tables are collected and the 
        num_to_keep newest are retained. For backup table names
        only those specific tables are removed.
        
        @param num_to_keep: how many of the latest backup tables
            to retain for each aux table
        @type num_to_keep: int
        @param user: MySQL user for login
        @type user: str
        @param pwd: password for logging into MySQL. Don't use
            for security reasons. Instead, put the pwd into
            $HOME/.ssh/canvas_pwd
        @type pwd: str
        @param target_db: MySQL where aux tables reside. 
        @type target_db: str
        @param host: MySQL host name
        @type host: str
        @param tables: list of specific tables to consider. If None,
            backups for all aux tables are trimmed. 
        @type tables: [str]
        @param logging_level: how much information to provide during runtime
        @type logging_level: logging.loglevel
        @param unittests: whether this instantiation is from a unittest
        @type unittests: boolean
        '''

        # Get local configuration info:        
        self.config_info = ConfigInfo()
        
        # Access to common functionality:
        self.utils       = Utilities()
        
        if target_db is None:
            target_db = self.config_info.canvas_db_aux
        
        if num_to_keep is None:
            self.num_to_keep = BackupRemover.default_num_backups_to_keep,
        else:
            self.num_to_keep = num_to_keep
        # Better name for tables to consider removing:
        tables_to_consider = tables
        
        # Unittests expect a db name in self.db:
        self.db = target_db

        self.db_obj = self.utils.log_into_mysql(user, pwd, db=target_db, host=host)

        self.utils.setup_logging(logging_level)
        if unittests:
            self.db_name = target_db
            return
        
        # Get names of all tables in the target_db
        all_tables = self.utils.get_existing_tables_in_dir(self.db_obj, return_all=True, target_db=target_db)
        
        # If caller specified only specific tables/backup tables to 
        # remove, week out all table names not in caller's list:
        all_tables_to_consider = self.find_tables_to_consider(all_tables, tables_to_consider)
        
        self.remove_old_backups(all_tables_to_consider)

    #-------------------------
    # find_tables_to_consider 
    #--------------
    
    def find_tables_to_consider(self, table_nm_list, specific_tables):
        '''
        Given mixed list of root and backup table names,
        and a list of specific table names, return a new
        list of effectively wanted tables.
        
        If a table root name occurs in specific_tables, then 
        all of that root name's backup versions in table_nm_list
        are retained. For backup table names in specific_tables,
        only those backup names are retained, not the others of
        the same root.
        
        If specific_tables is empty or None, table_nm_list is returned.   
        
        @param table_nm_list: list of all aux tables and backups
        @type table_nm_list: [str]
        @param specific_tables: possibly empty list of specific tables
            to remove. If empty list or none: return the full table_nm_list
        @type specific_tables: [str]
        '''
    
        if specific_tables is None:
            return table_nm_list
        if len(specific_tables) == 0:
            return table_nm_list
        
        # Collect all root names in specific_tables:
        roots = [tbl_name for tbl_name in specific_tables 
                 if self.utils.is_aux_table(tbl_name)]
        
        # Remember root table names, so we can
        # keep all their backup names in the returned list:
        new_all = table_nm_list.copy()
        for tbl_nm in table_nm_list:
            # Is it a backup name whose root table name
            # is in the list to consider?
            if self.utils.is_backup_name(tbl_nm) and \
                self.utils.get_root_name(tbl_nm) in roots:
                # Keep the backup name:
                continue
            # At this pt the table must explicitly be
            # in the keep list to survive:
            if tbl_nm not in specific_tables:
                new_all.remove(tbl_nm)
                
        return new_all

    #-------------------------
    # remove_old_backups 
    #--------------
        
    def remove_old_backups(self, all_table_names):
        '''
        Given a list of aux table names, find the backup 
        tables among them. Then delete all but the newest
        self.num_to_keep backup tables from the database. 
        
        @param all_table_names: list of table names to consider removing.
        @type all_table_names: [str]
        '''
        
        # Map table name --> list of its backups
        backup_tables = {}
        
        for tbl_nm in all_table_names:
            if self.utils.is_aux_table(tbl_nm):
                # Found root of an official table name.
                # (as apposed to a backup table)
                if tbl_nm not in backup_tables:
                    # Initialize an entry for this tbl:
                    backup_tables[tbl_nm] = []
                continue
            # Is it a backup table name?
            if self.utils.is_backup_name(tbl_nm):
                # Get root of the backup table name:
                root_nm = self.utils.get_root_name(tbl_nm)
                # Add to dict:
                try:
                    backup_tables[root_nm].append(tbl_nm)
                except KeyError:
                    # Hadn't seen this root table yet:
                    backup_tables[root_nm] = [tbl_nm]
        
        # Go through dict; for each table, sort its existing backup tables
        # by their dates, which are part of the names:

        # We'll modify backup_tables in the following
        # loop, so use a copy:
        backup_tables_copy = backup_tables.copy()
        
        for (tbl_nm, backup_names_list) in backup_tables_copy.items():
            # Sort the backup tbl names by their date, newest first:
            sorted_backups = sorted(backup_names_list, 
                                    key=lambda name: self.get_date(name),
                                    reverse=True)
            backup_tables[tbl_nm] = sorted_backups
            
        # Go through, and remove all but the first num_to_keep 
        # backup tables in each list:
        for backup_nm_list in backup_tables.values():
            # Chop off all names after the first num_to_keep names,
            # unless the name list is shorter than num_to_keep,
            # in which case we keep what we have:
            if len(backup_nm_list) <= self.num_to_keep:
                continue
            for to_delete in backup_nm_list[self.num_to_keep:]:
                self.db_obj.dropTable(to_delete)

    #-------------------------
    # get_date
    #--------------

    def get_date(self, backup_tbl_nm):
        (_root, date_str, _dateobj) = self.utils.backup_table_name_components(backup_tbl_nm)
        return date_str

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

    # Get default user, db, etc.
    config_info = ConfigInfo()
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Remove some of the backed-up aux tables."
                                     )

    parser.add_argument('-n', '--num_to_keep',
                        help=f'Number of backup tables to keep for each table. Default is ' +
                            f'{BackupRemover.default_num_backups_to_keep}',
                        default=BackupRemover.default_num_backups_to_keep)
    parser.add_argument('-u', '--user',
                        help=f'user name for logging into the canvas database. ' +
                             f'Default: {config_info.default_user}',
                        default=config_info.default_user)
                        
    parser.add_argument('-p', '--password',
                        help='password for logging into the canvas database.\n' +
                             'Default: content of $HOME/.ssh/canvas_db',
                        action='store_true',
                        default=None)
                        
    parser.add_argument('-o', '--host',
                        help='host name or ip of database. Default: Canvas production database.',
                        default='canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com')
                        #default='canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com')
                        
    parser.add_argument('-t', '--table',
                        nargs='+',
                        help='Name of one or more specific table(s) whose backups are to be removed. \n' +
                             'May be mix of backup table names and root table names. Default: all',
                        default=[]
                        )

    parser.add_argument('-d', '--database',
                        help='MySQL/Aurora database (schema) into which new tables are \n' +
                             'to be placed. Default: canvasdata_aux',
                        default='canvasdata_aux')
    
    args = parser.parse_args();
    BackupRemover(args.num_to_keep,
                  user=args.user,
                  pwd=args.password, 
                  target_db=args.database, 
                  host=args.host,
                  tables=args.table,
                  logging_level=logging.INFO,
                  unittests=False)
