#!/usr/bin/env python
'''
Created on May 13, 2019

@author: paepcke
'''
import argparse
import os
import sys
import logging

from canvas_prep import CanvasPrep

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
        if target_db is None:
            target_db = CanvasPrep.canvas_db_aux
        
        # Unittests expect a db name in self.db:
        self.db = target_db

        self.canvas_prepper = CanvasPrep(user=user,
                                         pwd =pwd,
                                         target_db=target_db,
                                         host=host,
                                         logging_level=logging_level,
                                         unittests=unittests)

        self.setup_logging()
        self.db_obj = self.canvas_prepper.db
        if unittests:
            self.db_name = target_db
            return
        
        # Get names of all tables in the target_db
        all_tables = self.canvas_prepper.get_existing_tables(return_all=True, target_db=target_db)
        
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
            table_names = self.canvas_prepper.tables
        
        self.canvas_prepper.restore_from_backup(table_names, db_schema=target_db)
        
        # Get the aux table names that exist now, after the restore:
        tbls_now = self.canvas_prepper.get_existing_tables(return_all=False, target_db=target_db)
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
            if self.canvas_prepper.is_aux_table(tbl_nm):
                # Found an aux table. Is it in the names-to-keep list?
                if tbl_nm in table_names_to_keep:
                    continue
            elif self.canvas_prepper.is_backup_name(tbl_nm):
                # Found a backup table. If its root name is in 
                # the keep list, keep the name in:
                if self.canvas_prepper.get_root_name(tbl_nm) in table_names_to_keep or\
                    tbl_nm in table_names_to_keep:
                    continue
                
            # Found a table name that is not to be spared.
            # Remove the name
            all_tables.remove(tbl_nm)
        return all_tables

    #-------------------------
    # close  
    #--------------

    def close(self):
        self.canvas_prepper.close()

    #------------------------------------
    # setup_logging 
    #-------------------    
        
    def setup_logging(self):
        self.log_info = self.canvas_prepper.log_info
        self.log_err = self.canvas_prepper.log_err
        self.log_warn = self.canvas_prepper.log_warn
        
# ----------------------------- Main ------------------------

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Restore backed-up aux tables."
                                     )

    parser.add_argument('-f', '--force',
                        help="restore *all* aux tables from available backups, or only non-existing tables; default=False",
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
    