'''
Created on Aug 21, 2019

@author: paepcke
'''

import logging
import os

from utilities import Utilities


class UnittestDbFinder(object):
    '''
    Utility class for finding and creating databases
    for unittests. If the unittests occur on localhost,
    we assume that a database 'Unittests' exists, and that
    user 'unittest' has password-free access.
    
    If tests occur on a remote machine, where we may not
    be able to create the Unittest db, or do without a pwd,
    then we find a database name "Unittests_xx" where xx is
    a number. We ensure that the db does not already exist.
    
    The property unittest_db_nm returns the name, after the
    respective db has been created.
    '''

    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, db_obj, logging_level=logging.INFO, unittests=False):
        '''
        Provided with an open db instance, we return the
        name of a db where unittests may be performed.
        If db_obj is None, or the db is open on localhost,
        then we return 'Unittest'. Else we search for a
        free name.
        
        @param db_obj: a database instance open to the host where tests
            are to be performed.
        @type db_obj: MySQLDB
        @param logging_level: desired logging behavior
        @type logging_level: {logging.INFO, logging.WARN, etc.}
        @param unittests: if true, caller is a unittest, and the constructor
            will search for Unittest_xx even if host == localhost
            In that case it is the unittest's responsibility to remove the 
            created database.
        @type unittests: bool
        '''
        
        # Access to common functionality:
        self.utils = Utilities()
                
        # If not working on localhost, where we expect a db
        # 'Unittest" Ensure there is a unittest db for us to work in.
        # We'll delete it later:
        
        self.utils.setup_logging(logging_level)
        
        if db_obj is None:
            test_host = 'localhost'
        else:
            test_host = db_obj.dbHost()
            
        if (test_host == 'localhost' or test_host == '127.0.0.1') and not unittests:
            self.db_name = 'Unittest'
        else:
            unittest_db_nm = 'unittests_'
            nm_indx = 0
            try:
                print("Looking for unused database name for unittest activity...")
                while True:
                    nm_indx += 1
                    db_name = unittest_db_nm + str(nm_indx)
                    db_exists_cmd = f'''
                                     SELECT COUNT(*) AS num_dbs 
                                       FROM information_schema.schemata
                                      WHERE schema_name = '{db_name}';
                                     '''
                    try:
                        num_existing = db_obj.query(db_exists_cmd).next()
                        if num_existing == 0:
                            # Found a db name that doesn't exist:
                            break
                    except Exception as e:
                        db_obj.close()
                        raise RuntimeError(f"Cannot probe for existing db '{db_name}': {repr(e)}")
            
                print(f"Creating database {db_name} for unittest activity...")
                # Create the db to play in:
                try:
                    (errors, _warnings) = db_obj.execute(f"CREATE DATABASE {db_name};")
                    if errors is not None:
                        raise RuntimeError(f"Cannot create temporary db '{db_name}': {str(errors)}")
                except Exception as e:
                    if isinstance(e, RuntimeError):
                        raise
                    raise RuntimeError(f"Cannot create temporary db '{db_name}': {repr(e)}")
            finally:
                self.db_name = db_name

    #-------------------------
    # unittest_db_name
    #--------------

    @property
    def unittest_db_name(self):
        return self.db_name
    