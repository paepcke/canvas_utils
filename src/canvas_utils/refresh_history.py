'''
Created on Aug 24, 2019

@author: paepcke
'''

import configparser
import os

from pymysql_utils.pymysql_utils import MySQLDB

from canvas_utils_exceptions import DatabaseError


class LoadHistoryLister(object):
    '''
    Reads table LoadLog. Lists date of latest
    refresh for each table. Lists missing tables,
    and list of all tables.
    '''

    #-------------------------
    # Constructor 
    #--------------

    def __init__(self):
        '''
        Constructor
        '''
        # Read any configs from the config file, if it exists:
        self.read_configuration()
        self.prep_db()

    #-------------------------
    # read_configuration 
    #--------------
    
    def read_configuration(self):
        
        config_parser = configparser.ConfigParser()
        config_parser.read(os.path.join(proj_root_dir, 'setup.cfg'))
        
        try:
            default_host = config_parser['DATABASE']['default_host']
        except KeyError:
            raise 

        try:
            canvas_db_aux = config_parser['DATABASE']['canvas_auxiliary_db_name']
        except KeyError:
            pass

        try:
            default_user = config_parser['DATABASE']['default_user']
        except KeyError:
            pass
        

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
            else:
                raise DatabaseError("Cannot open Canvas database: %s" % repr(e))
        except Exception as e:
            raise DatabaseError("Cannot open Canvas database: %s" % repr(e))
        
        return db
    
    #-------------------------
    # prep_db 
    #--------------
    
    def prep_db(self):
        
        # All SQL is written assuming MySQL's current db is
        # the one where the new tables will be created:
        (err, _warn) = self.db.execute('USE %s' % CanvasPrep.canvas_db_aux)
        if err is not None:
            raise DatabaseError(f"Cannot switch to db {CanvasPrep.canvas_db_aux}: {repr(err)}")

        
        # MySQL 8 started to complain when functions do not 
        # specify DETERMINISTIC or NO_SQL, or one of several other
        # function characteristics. Avoid that complaint:
        
        (err, _warn) = self.db.execute("SET GLOBAL log_bin_trust_function_creators = 1;")
        if err is not None:
            self.log_warn(f"Cannot set global log_bin_trus_function_creators: {repr(err)}")
        
        
        # At least for MySQL 8.x we need to allow zero dates,
        # like '0000-00-00 00:00:00', which is found in the Canvas db:
        
        (err, _warn) = self.db.execute('SET sql_mode="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";')
        if err is not None:
            self.log_warn(f"Cannot set sql_mode: {repr(err)}")
        
        
        # Ensure that all the handy SQL functions are available.
        funcs_file = os.path.join(self.queries_dir, 'mysqlProcAndFuncBodies.sql')
        (errors, _warns) = self.db.execute('SOURCE %s' % funcs_file)
        if errors is not None:
            self.log_warn("Could not load MySQL funcs/procedures: %s" % str(errors))
        