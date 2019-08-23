'''
Created on Aug 21, 2019

@author: paepcke
'''
import configparser
import getpass
from os import getenv
import os
import unittest

from pymysql_utils.pymysql_utils import MySQLDB

from unittest_db_finder import UnittestDbFinder


TEST_ALL = True
#TEST_ALL = False

class FindUnittestDbTester(unittest.TestCase):

    # Canvas pwd file name:
    canvas_pwd_file = os.path.join(getenv("HOME"), '.ssh', 'canvas_pwd') 


    #-------------------------
    # setUpClass 
    #--------------

    @classmethod
    def setUpClass(cls):
        super(FindUnittestDbTester, cls).setUpClass()
        
        # Get whether to test on localhost, or on 
        # remote host:
        
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), '../../setup.cfg'))
        cls.test_host = config['TESTMACHINE']['mysql_host']
        cls.user = config['TESTMACHINE']['mysql_user']
        
        cls.db = MySQLDB(user=cls.user, 
                         passwd=cls.get_db_pwd(),  # Assume pwd in in ~/.ssh/canvas_pwd 
                         db='information_schema', 
                         host=cls.test_host)
    
    #-------------------------
    # tearDownClass 
    #--------------
    
    @classmethod
    def tearDownClass(cls):
        super(FindUnittestDbTester, cls).tearDownClass()
        cls.db.close()
        
    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.db = FindUnittestDbTester.db
    
    #-------------------------
    # testFindUnittest 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testFindUnittest(self):

        found_name = "<not yet determined>"
        
        # If localhost, we just assume a db called 'unittest'.
        # Nothing else is needed:
        if FindUnittestDbTester.test_host == 'localhost':
            existence_query = f'''select count(schema_name)
                                    from information_schema.schemata
                                   where schema_name = 'unittest';
                                   '''
            num_unittest_dbs = self.db.query(existence_query).next()
            self.assertEqual(num_unittest_dbs, 1)
            return
        
        # Check which unittests_xx are already in the db.
        # Remember them, and leave them alone:
        
        existence_query = f'''select schema_name
                                from information_schema.schemata
                               where schema_name REGEXP('unittests_[0-9]*$');
                               '''
        existing_unittest_dbs_res = self.db.query(existence_query)
        existing_unittest_dbs = [schema_name for schema_name in existing_unittest_dbs_res]
        
        # Find the highest xx_max of the existing unittests_xx:
        xx_max = 0
        for db_name in existing_unittest_dbs:
            # Get the number after the '_':
            xx_new = int(db_name.split('_')[-1])
            if xx_new > xx_max:
                xx_max = xx_new
         
        expected_new_unittest_db_name = f'unittests_{str(xx_max + 1)}'
        
        try:
            finder = UnittestDbFinder(self.db, unittests=True)
            # Make sure found_name is bound in case we bomb
            # in the finder:
            found_name = finder.unittest_db_name
            self.assertEqual(found_name, expected_new_unittest_db_name)
        finally:
            try:
                self.db.execute(f"DROP DATABASE {found_name}")
            except Exception as e:
                print(f"Could not remove unittest db {found_name}: {repr(e)}")

        # Do it again, but with one unittest already there:
        try:
            finder = UnittestDbFinder(self.db, unittests=True)
            found_name_1 = finder.unittest_db_name
            finder = UnittestDbFinder(self.db, unittests=True)
            found_name_2 = finder.unittest_db_name
            expected_new_unittest_db_name_2 = f'unittests_{xx_max + 2}'
            self.assertEqual(found_name_2, expected_new_unittest_db_name_2)
        finally:
            try:
                self.db.execute(f"DROP DATABASE {found_name_1}")
                self.db.execute(f"DROP DATABASE {found_name_2}")
            except Exception as e:
                print(f"Could not remove unittest db {found_name}: {repr(e)}")

# ------------------------- Utilities -------------------

    #-------------------------
    # get_db_pwd
    #--------------

    @classmethod
    def get_db_pwd(cls):
        '''
        Find appropriate password for logging into MySQL. Normally
        a file is expected in CanvasPrep.canvas_pwd_file, and
        the pwd is taken from there.
        
        Password convention is different from 
        normal operation: If passed-in pwd is None
        and host is localhost, we assume that there
        is a user 'unittest' without a pwd.
        
        '''
        
        if cls.test_host == 'localhost':
            return ''
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', cls.canvas_pwd_file)
            if os.path.exists(default_pwd_file):
                with open(default_pwd_file, 'r') as fd:
                    pwd = fd.readline().strip()
                    return pwd
            
        # Ask on console:
        pwd = getpass.getpass("Password for Canvas database: ")
        return pwd
                
# ----------------------------- Main ------------------

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()