'''
Created on May 1, 2019

@author: paepcke
'''
import datetime
import unittest

from pymysql_utils.pymysql_utils import MySQLDB

from config_info import ConfigInfo
from restore_tables import TableRestorer
from unittest_db_finder import UnittestDbFinder
from utilities import Utilities

TEST_ALL = True
#TEST_ALL = False

class CanvasRestoreTablesTests(unittest.TestCase):

    #------------------------------------
    # setupClass 
    #-------------------    

    @classmethod
    def setUpClass(cls):
        super(CanvasRestoreTablesTests, cls).setUpClass()
        
        cls.utils = Utilities()
        cls.utils.setup_logging()
        # Read config file to see which MySQL server test_host we should
        # run the tests on. If setup.py does not exist, copy
        # setupSample.py to setup.py:
        
        config_info     	= ConfigInfo()
        test_host       	= cls.test_host = config_info.test_default_host
        cls.user        	= config_info.test_default_user
        cls.canvas_pwd_file = config_info.canvas_pwd_file

        # If not working on localhost, where we expect a db
        # 'Unittest" Ensure there is a unittest db for us to work in.
        # We'll delete it later:
        
        if test_host == 'localhost':
            db_name = 'Unittest'
        else:
            db = None
            db = MySQLDB(host=test_host,
                         user=cls.user,
                         passwd=cls.utils.get_db_pwd(test_host,
                                                     unittests=True),
                         )
            try:
                db_name = UnittestDbFinder(db).db_name
            except Exception as e:
                raise AssertionError(f"Cannot open db to find a unittest db: {repr(e)}")
            finally:
                if db is not None:
                    db.close()
        
        CanvasRestoreTablesTests.unittests_db_nm = db_name
       
    #------------------------------------
    # tearDownClass 
    #-------------------    
        
    @classmethod
    def tearDownClass(cls):
        super(CanvasRestoreTablesTests, cls).tearDownClass()
        # If testing on localhost, we assumed that 
        # a db 'Unittest' was available ahead of time.
        # So no need to remove anything:
        
        if cls.test_host == 'localhost':
            return
        
        try:
            if isinstance(CanvasRestoreTablesTests.restore_obj, TableRestorer):
                CanvasRestoreTablesTests.restore_obj.close()
        except AttributeError:
            # No restore_obj exists yet:
            pass
            
        restore_obj = TableRestorer(host=cls.test_host,
                                    user=cls.user,
                                    target_db='information_schema',
                                    db_pwd=cls.utils.get_db_pwd(cls.test_host,
                                                                unittests=True),
                                    unittests=True
                                    )
        db = restore_obj.db_obj
        db_name = CanvasRestoreTablesTests.unittests_db_nm
        cls.utils.log_info(f"Removing database '{db_name}'...")
        try:
            db.execute(f"DROP DATABASE {db_name};")
        except Exception as e:
            raise RuntimeError(f"Could not remove temporary unittest db '{db_name}': {repr(e)}")
        finally:
            cls.utils.log_info(f"Closing TableRestorer instance.")
            restore_obj.close()
        
    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        cls = CanvasRestoreTablesTests
        self.utils = cls.utils
        self.db_schema = cls.unittests_db_nm 
        try:
            self.restore_obj = TableRestorer(host=cls.test_host,
                                             user=cls.user,
                                             target_db=cls.unittests_db_nm,
                                             db_pwd=self.utils.get_db_pwd(cls.test_host,
                                                                          unittests=True),
                                             unittests=True
                                             )
        except Exception as e:
            raise ValueError(f"Error opening restorer: {repr(e)}")
        # To give class obj access to the db:
        cls.restore_obj = self.restore_obj
        
        # For convenience: 
        self.db = self.restore_obj.db_obj
        self.db_name = self.restore_obj.db_name

    #-------------------------
    # tearDown 
    #--------------

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.db.close()
        self.restore_obj.close()
        
    #------------------------------------
    # testRemoveTableGroups 
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testRemoveTableGroups(self):
        
        tbl0    = 'foo'
        tbl1    = 'Terms'
        tbl1_b1 = 'Terms_2019_01_10_14_14_40_123456'
        tbl2    = 'AssignmentSubmissions'
        tbl2_b1 = 'AssignmentSubmissions_2018_01_10_14_14_40_123456'
        tbl2_b2 = 'AssignmentSubmissions_2019_01_10_14_14_40_123456'
        #tbl4    = 'StudentUnits'

        tbl_grp = [tbl1]
        res = self.restore_obj.remove_table_groups(tbl_grp)
        # Expect no names left, b/c did not ask for
        # any name families to be retained:
        self.assertEqual(len(res), 0)
        
        tbl_grp = [tbl0]
        res = self.restore_obj.remove_table_groups(tbl_grp, table_names_to_keep=[tbl1])
        res = self.restore_obj.remove_table_groups(tbl_grp)
        # Expect name to be removed, b/c it's not
        # a real aux table name:
        self.assertEqual(len(res), 0)
        
        tbl_grp = [tbl1, tbl1_b1]
        res = self.restore_obj.remove_table_groups(tbl_grp, table_names_to_keep=[tbl1])
        # Expect both tables left:
        # a real aux table name:
        self.assertEqual(len(res), 2)
        
        tbl_grp = [tbl2, tbl2_b1, tbl2_b2]
        res = self.restore_obj.remove_table_groups(tbl_grp, table_names_to_keep=[tbl2])
        # Expect all tables left:
        # a real aux table name:
        self.assertEqual(len(res), 3)
        
        # Specifying one backup table for keepers:
        tbl_grp = [tbl2, tbl2_b1, tbl2_b2]
        res = self.restore_obj.remove_table_groups(tbl_grp, table_names_to_keep=[tbl2_b2])
        # Expect all tables left:
        # a real aux table name:
        self.assertEqual(len(res), 1)
        
        # Specifying two backup table for keepers:
        tbl_grp = [tbl2, tbl2_b1, tbl2_b2]
        res = self.restore_obj.remove_table_groups(tbl_grp, table_names_to_keep=[tbl2_b1, tbl2_b2])
        # Expect both tables left:
        # a real aux table name:
        self.assertEqual(len(res), 2)

    #-------------------------
    # testGetExistingTables 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testGetExistingTables(self):
        # Test finding tables in a target db that are
        # aux tables, ignoring all others.

        self.removeAllUnittestTables()
        
        self.db.createTable('Table1', {'foo': 'int'})
        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=True, target_db=self.db_name)
        self.assertEqual(tbl_names, ['Table1'])
        
        # Test ignoring all none-aux-table tables:
        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=False, target_db=self.db_name)
        self.assertEqual(len(tbl_names), 0)
        
        # Add a second table:
        self.db.createTable('Table2', {'bar': 'int'})

        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=True, target_db=self.db_name)
        self.assertEqual(tbl_names, ['Table1', 'Table2'])
        
        # Add a legitimate aux table:
        self.db.createTable('Terms', {'fum': 'int'})
        
        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=False, target_db=self.db_name)
        self.assertEqual(tbl_names, ['Terms'])
        
        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=True, target_db=self.db_name)
        self.assertCountEqual(tbl_names, ['Table1', 'Table2', 'Terms'])
        
        # Another legit table:
        self.db.createTable('ModuleItems', {'fum': 'int'})
        # ... and take out the irrelevant ones:
        
        self.db.dropTable('Table1')
        self.db.dropTable('Table2')
        tbl_names = self.utils.get_existing_tables_in_dir(self.db, return_all=True, target_db=self.db_name)
        self.assertCountEqual(tbl_names, ['Terms', 'ModuleItems'])
                
    #-------------------------
    # testIsBackupName 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testIsBackupName(self):

        is_backup_name = self.utils.is_backup_name('Terms_2019_05_09_11_51_03')
        self.assertTrue(is_backup_name)
        
        is_backup_name = self.utils.is_backup_name('foo_2019_05_09_11_51_03')
        self.assertFalse(is_backup_name)
        
        is_backup_name = self.utils.is_backup_name('2019_05_09_11_51_03')
        self.assertFalse(is_backup_name)

    #-------------------------
    # sortBackupTableNames 
    #--------------

    # Tested in test_canvas_prep.py
    
#     @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
#     def testSortBackupTableNames(self):
#         
#         names = ['Terms_2019_05_09_11_51_03']
#         sorted_names = self.restore_obj.sort_backup_table_names(names)
#         self.assertEqual(sorted_names, names)
#         
#         names = ['Terms_2019_05_09_11_51_03', 'Terms_2018_05_09_11_51_03']
#         sorted_names = self.restore_obj.sort_backup_table_names(names)        
#         self.assertEqual(sorted_names, names)
#         
#         names = ['Terms_2018_05_09_11_51_03', 'Terms_2019_05_09_11_51_03']
#         sorted_names = self.restore_obj.sort_backup_table_names(names)        
#         self.assertEqual(sorted_names, ['Terms_2019_05_09_11_51_03', 'Terms_2018_05_09_11_51_03'])

    #-------------------------
    # testSomeRestoring 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testRestoring(self):
        
        date_str1 = '2019_01_01_11_00_10'
        date_str2 = '2019_11_01_11_00_10'
        
        self.removeAllUnittestTables()
        
        # Create a 'Current' table:
        self.db.createTable('Terms', {'foo' : 'int'})
        self.db.insert('Terms', {'foo' : 10})
        
        # Create a backup table:
        self.db.createTable(f'Terms_{date_str1}', {'foo' : 'int'})
        self.db.insert(f'Terms_{date_str1}', {'foo' : 20})
        
        # Are they all there?
        show_tables_query_it = self.db.query("SHOW TABLES;")
        tables_in_db = [res for res in show_tables_query_it]
        self.assertCountEqual(tables_in_db, ['Terms', f'Terms_{date_str1}'])
        
        try:
            # Restore the backup tbl with foo=20 to overwrite
            # the Terms table that currently has foo==10
            self.restore_obj.restore_tables(['Terms'], target_db=self.db_name)
            foo_val_cur_tbl = self.db.query("select foo from Terms;").next()
            self.assertEqual(foo_val_cur_tbl, 20)
            
            # Re-create the backup table, then create a second
            # one with a later date. Set its foo=30. (Re-creating the 
            # first backup is needed b/c the above restore cmd renamed
            # it to 'Terms':
            self.db.createTable(f'Terms_{date_str1}', {'foo' : 'int'})
            self.db.insert(f'Terms_{date_str1}', {'foo' : 20})

            self.db.createTable(f'Terms_{date_str2}', {'foo' : 'int'})
            self.db.insert(f'Terms_{date_str2}', {'foo' : 30})

            # Restore the newest backup tbl with foo=30 to overwrite
            # the Terms table that currently has foo==20
            self.restore_obj.restore_tables(['Terms'], target_db=self.db_name)
            foo_val_cur_tbl = self.db.query("select foo from Terms;").next()
            self.assertEqual(foo_val_cur_tbl, 30)

        finally:
            pass
        
    # ------------------------------- Utilities -------------------------

    #------------------------------------
    # check_table_existence 
    #-------------------    
    
    def check_table_existence(self, db, _db_schema, num_expected_tables=0):
        # Get remaining tables:
        table_names = self.utils.get_tbl_names_in_schema(db, _db_schema)
        
        self.assertEqual(len(table_names), num_expected_tables)
        
    #-------------------------
    # removeAllUnittestTables 
    #--------------
    
    def removeAllUnittestTables(self):
        '''
        Find all tables in the Unittest db,
        and remove them.
        '''
        tbl_names = self.utils.get_tbl_names_in_schema(self.db, self.db_name)
        for tbl_name in tbl_names:
            self.db.dropTable(tbl_name)
    
    #------------------------------------
    # find_backup_table_names 
    #-------------------    
        
    def find_backup_table_names(self, table_names, name_root_to_find=None, datetime_obj_used=None):
        '''
        Given a list of table names, return a (sub-)list of
        those names that are backup tables. The names must
        match the precise pattern created by CanvasPrep for
        backups. 
        
        @param table_names: table name list to search
        @type table_names: [str]
        @param name_root_to_find:
        @type name_root_to_find:
        @param name_to_find: table name, if a particular name is to be found
        @type name_to_find: str
        @param datetime_obj_used: a datetime object that was used to create
                the table name to search for in table_names
        @type datetime_obj_used: datetime.datetime
        @return: possibly empty list of backup tables found
        @rtype: [str]
        @raise ValueError: if bad parameters 
        '''

        if name_root_to_find is not None and type(datetime_obj_used) != datetime.datetime:
            raise ValueError(f"If name_root_to_find is non-None, then datetime_obj_used must be a datetime objec.")

        tables_found = []
        # Find the test_table_name with an attached date-time:
        for table_name in table_names:
            # One table name:
            # Try to get a datetime and the original table name root
            # from table_name. This will check whether the table was
            # some pre-existing non-backup table. If table_name is not
            # a backup table, examine the text name in the list:
            try:
                (recovered_table_name, _recovered_dt_str, recovered_dt_obj) =\
                    self.utils.backup_table_name_components(table_name)
                    
                # If we didn't get an error, we found a backup table:
                tables_found.append(table_name)
                    
                if name_root_to_find is not None:
                    # We are searching for a particular backup table name:
                    if recovered_table_name != name_root_to_find or\
                        recovered_dt_obj != datetime_obj_used:
                        # Not the name we are looking for: 
                        continue
                    else:
                        # All good
                        tables_found = [table_name]
                        return tables_found
                    
                else:
                    # We are just to collect all backup tables:
                    continue  # to find more backup tables
                    
            except ValueError:
                # Not a backed-up table; next table name:
                continue
        # Ran through all table names without finding the 
        # backup
        return tables_found
        

            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetPreexistingTables']
    unittest.main()
