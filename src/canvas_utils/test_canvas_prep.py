'''
Created on May 1, 2019

@author: paepcke
'''
import datetime
import re
import time
import unittest

from pymysql_utils.pymysql_utils import MySQLDB

from canvas_prep import CanvasPrep
from config_info import ConfigInfo
from unittest_db_finder import UnittestDbFinder
from utilities import Utilities
from restore_tables import TableRestorer


TEST_ALL = True
#TEST_ALL = False

class CanvasUtilsTests(unittest.TestCase):

    #------------------------------------
    # setUpClass 
    #-------------------    

    @classmethod
    def setUpClass(cls):
        super(CanvasUtilsTests, cls).setUpClass()
        
        # Read config file to see which MySQL server test_host we should
        # run the tests on. If setup.py does not exist, copy
        # setupSample.py to setup.py:
              
        config_info     	= ConfigInfo()        
        test_host       	= config_info.test_default_host
        user            	= config_info.test_default_user
        cls.canvas_pwd_file = config_info.canvas_pwd_file

        # Access to common functionality:
        cls.utils = Utilities()


        # If not working on localhost, where we expect a db
        # 'Unittest" Ensure there is a unittest db for us to work in.
        # We'll delete it later:
        
        if test_host == 'localhost':
            db_name = 'Unittest'
        else:
            db = None
            db = MySQLDB(host=test_host,
                         user=config_info.test_default_user,
                         passwd=cls.utils.get_db_pwd(test_host,
                                                     unittests=True)
                         )
            try:
                db_name = UnittestDbFinder(db).db_name
            except Exception as e:
                raise AssertionError(f"Cannot open db to find a unittest db: {repr(e)}")
            finally:
                if db is not None:
                    db.close()
        
        CanvasUtilsTests.test_host = test_host
        CanvasUtilsTests.unittests_db_nm = db_name
        CanvasUtilsTests.user = user
        
    #------------------------------------
    # tearDownClass 
    #-------------------    
        
    @classmethod
    def tearDownClass(cls):
        super(CanvasUtilsTests, cls).tearDownClass()
        # If testing on localhost, we assumed that 
        # a db 'Unittest' was available ahead of time.
        # So no need to remove anything:
        
        if cls.test_host == 'localhost':
            return
            
        db_name = CanvasUtilsTests.unittests_db_nm
        
        prep_obj = CanvasPrep(host=cls.test_host,
                              user=cls.user,
                              target_db=db_name,
                              db_pwd=cls.utils.get_db_pwd(cls.test_host,
                                                          unittests=True),
                              unittests=True
                              )

        db = prep_obj.db

        prep_obj.log_info(f"Removing database '{db_name}'...")
        try:
            db.execute(f"DROP DATABASE {db_name};")
        except Exception as e:
            raise RuntimeError(f"Could not remove temporary unittest db '{db_name}': {repr(e)}")
        finally:
            prep_obj.log_info(f"Closing CanvasPrep instance.")
            prep_obj.close()
  
    #-------------------------
    # setUp 
    #--------------
           
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        # Convenience copy of the unittest db name determined in 
        # the class setup:
        self.db_schema = CanvasUtilsTests.unittests_db_nm
        self.user      = CanvasUtilsTests.user
        self.test_host = CanvasUtilsTests.test_host
        self.utils     = CanvasUtilsTests.utils
        self.prep_obj  = CanvasPrep(user=self.user, 
                              host=self.test_host, 
                              target_db=self.db_schema,
                              db_pwd=self.utils.get_db_pwd(self.test_host,
                                                           unittests=True),
                              unittests=True)
        self.db        = self.prep_obj.db
        self.utils     = CanvasUtilsTests.utils

    #-------------------------
    # tearDown 
    #--------------
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.prep_obj.close()

    #------------------------------------
    # testGetBackupNames 
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testGetBackupNames(self):
        backup_nm_pat = re.compile(r"[^_]*_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}")

        backup_nm = self.prep_obj.get_backup_table_name('my_table')
        # Does the backup name look like a name with a datetime
        # attached?
        self.assertIsNotNone(backup_nm_pat.search(backup_nm))
        
    #------------------------------------
    # testRecoverTableNmFromBackupName 
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testRecoverTableNmFromBackupName(self):
        
        backup_name = 'GradingProcess_2019_02_28_15_34_10_654321'
        recovered_name = 'GradingProcess'
        recovered_dt_str = '2019_02_28_15_34_10_654321'
        (nm, dt_str, dt_obj) = self.utils.backup_table_name_components(backup_name)
        self.assertEqual(nm, recovered_name)
        self.assertEqual(dt_str, recovered_dt_str)
        self.assertTrue(dt_obj < datetime.datetime.now())
        
    #------------------------------------
    # testSaveTables
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')    
    def testSaveTables(self):

        try:
            test_table_name1 = 'Terms'
            # Create a test table in the unittests db:
            db = self.prep_obj.db
            db.execute(f"USE {self.db_schema}")
            
            self.removeAllUnittestTables(db)
            
            db.createTable(test_table_name1, {'col1': 'int'}, temporary=False)
            
            # Find all tables in the unittest db
            # (likely only the one we just created):
            
            table_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
            
            # But the tbl we created must indeed be there:
            self.assertIn(test_table_name1, table_names)
    
            # The actual test: create a backup table of the
            # one we just created:
            datetime_obj_used = self.prep_obj.backup_tables([test_table_name1])
            
            # Get all the tables in the db schema again.
            # Should include the new backup table, but
            # not the original table:
            
            table_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
            
            # Find our test table in the list of names:            
            name_list = self.find_backup_table_names(table_names, 
                                                     name_root_to_find=test_table_name1, 
                                                     datetime_obj_used=datetime_obj_used)
            if len(name_list) == 0:
                # Ran through all table names without finding the 
                # backup; fail:
                self.fail()
                
            # Add two more tables, and ensure it all works
            # the mix of two non-backup tables, and one backup table:
            test_table_name2 = 'Courses'
            test_table_name3 = 'StudentUnits'
            db.createTable(test_table_name2, {'col1': 'int'}, temporary=False)
            db.createTable(test_table_name3, {'col1': 'int'}, temporary=False)
            
            _datetime_obj_used1_2 = self.prep_obj.backup_tables([test_table_name2, test_table_name3])
            table_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
            
            # Find our test table in the list of names:            
            name_list = self.find_backup_table_names(table_names) 
            self.assertEqual(len(name_list), 3)
            
        finally:
            # We don't delete tables; tearDownClass() will
            # removed the unittest db:
            self.prep_obj.close()
            
    #------------------------------------
    # testRestoreFromBackup 
    #-------------------    
            
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testRestoreFromBackup(self):
        
        restorer = TableRestorer(
                     user=self.user,
                     db_pwd=self.utils.get_db_pwd(self.test_host,
                                                  unittests=True), 
                     target_db=self.db_schema, 
                     host=self.test_host,
                     unittests=True)     
        
        test_table_name = 'Terms'
        restorer_db = restorer.db_obj
        
        self.removeAllUnittestTables(restorer_db)
        
        # Create a test table in the unittests db:
        restorer_db.createTable(test_table_name, {'col1': 'int'}, temporary=False)
        
        table_names_now = self.utils.get_tbl_names_in_schema(restorer_db, self.db_schema)
        self.assertIn(test_table_name, table_names_now)
        
        # Now do the backup of this table:
        the_dt_obj = self.prep_obj.backup_tables(test_table_name)
        
        # Get list of tables again; should be a single backup table name:
        # NOTE: must flush restorer db so it does not return old information
        #       from the cache. 
        restorer_db.execute('FLUSH TABLES')
        table_name_now = self.utils.get_tbl_names_in_schema(restorer_db, self.db_schema)[0]
        (root_nm, _dt_str, dt_obj) = self.utils.backup_table_name_components(table_name_now)

        self.assertEqual(root_nm, test_table_name)
        self.assertEqual(dt_obj, the_dt_obj)
        
        # Restore the backup to be the original:
        restorer.restore_from_backup(test_table_name, self.db_schema)
        
        # Is the table back?
        table_names_now = self.utils.get_tbl_names_in_schema(restorer_db, self.db_schema)
        self.assertIn(test_table_name, table_names_now)
        self.assertEqual(len(table_names_now), 1)
        
        # Now restore when two backups exist.
        _the_dt_obj1 = self.prep_obj.backup_tables(test_table_name)
        restorer_db.createTable(test_table_name, {'col1': 'int DEFAULT 200'}, temporary=False)
        restorer_db.insert(test_table_name, {'col1': 10})

        # Change col1 to 10:
        restorer_db.update(test_table_name, 'col1', 10)           
        _the_dt_obj2 = self.prep_obj.backup_tables(test_table_name)
        
        # Restore the backup to be the most recent:
        restorer.restore_from_backup(test_table_name, self.db_schema)
        
        # Is the table back?
        table_names_now = self.utils.get_tbl_names_in_schema(restorer_db, self.db_schema)
        self.assertIn(test_table_name, table_names_now)
        self.assertEqual(len(table_names_now), 2)
        res = self.db.query(f"SELECT COL1 FROM {test_table_name} LIMIT 1;")
        self.assertEqual(res.next(), 10)
            
    #------------------------------------
    # testClearBackups 
    #-------------------    

    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')    
    def testClearBackups(self):

        test_table_name1 = 'Terms'
        test_table_name2 = 'Courses'
        table_names_list = [test_table_name1, test_table_name2]
        
        
        # Create test tables in the unittests db:
        db = self.prep_obj.db
        db.execute(f"USE {self.db_schema}")
        
        self.removeAllUnittestTables(db)

        # Create three tables:
        db.createTable(test_table_name1, {'col1': 'int'}, temporary=False)
        db.createTable(test_table_name2, {'col1': 'int'}, temporary=False)
        
        # Back up table1 three times with one sec in between, getting 
        # the datetime objects used. The third time we backup table1,
        # we backup table2 along with it. So the third backup of tbl1,
        # and the single backup of tbl2 will share a datetime:

        datetime_obj1_1 = self.prep_obj.backup_tables([test_table_name1])
        # Recreate a fresh tbl 1 copy that we can then backup:
        db.createTable(test_table_name1, {'col1': 'int'}, temporary=False)
        time.sleep(1)
        
        # Same table again:
        datetime_obj1_2 = self.prep_obj.backup_tables([test_table_name1])
        # Again: make a new tbl1:
        db.createTable(test_table_name1, {'col1': 'int'}, temporary=False)
        time.sleep(1)
        
        # And a third time, but have its datetime the same as
        # the backup for tbl 3:
        datetime_obj1_3_2 = self.prep_obj.backup_tables([test_table_name1, test_table_name2])
        
        dt_objs_list  = [datetime_obj1_1, datetime_obj1_2, datetime_obj1_3_2]
        
        # Make sure the backups are there:
        table_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
        for backup_tbl_name in table_names:
            (tbl_root, _dt_str, dt_obj) = self.utils.backup_table_name_components(backup_tbl_name)
            self.assertIn(tbl_root, table_names_list)
            self.assertIn(dt_obj, dt_objs_list)
        
        # Create the non-backup tables again (the originals were
        # renamed to be the backps):
        db.createTable(test_table_name1, {'col1': 'int'}, temporary=False)            
        db.createTable(test_table_name2, {'col1': 'int'}, temporary=False)
        
        # Now remove the backups for table 1, keeping the 2 latest ones.
        # So only the first backup should be removed:
        self.prep_obj.remove_backup_tables(test_table_name1, num_to_keep=2) #<----- Takes away 2, instead of 1
        
        # Should have tbl1, tbl2, 2 tbl1 backups, and one tbl2 backup:
        self.check_table_existence(db, self.db_schema, num_expected_tables=5)
        
        # Remove tbl2's single backup, but asking for one remaining. 
        # So nothing should be removed:
        self.prep_obj.remove_backup_tables(test_table_name2, num_to_keep=1)
        
        # Should still have 2 non-backups, 2 tbl1 backups, and one tbl2 backup:
        self.check_table_existence(db, self.db_schema, num_expected_tables=5)
        
        # Finally, remove all tl1 backups:
        self.prep_obj.remove_backup_tables(test_table_name1)
                                      
        # Should have just 1 tbl2 backup, and the two non-backups
        self.check_table_existence(db, self.db_schema, num_expected_tables=3)
            
    #------------------------------------
    # testSortBackupTableNames 
    #-------------------
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testSortBackupTableNames(self):
        
        tbl_nm1 = 'Terms_2019_01_10_14_14_40_123456' # 2
        tbl_nm2 = 'Terms_2018_01_10_14_14_40_123456' # 3
        tbl_nm3 = 'Terms_2019_01_10_14_14_40_123456' # 2
        tbl_nm4 = 'Terms_2019_02_10_14_14_40_123456' # 1
        
        sorted_nms = self.utils.sort_backup_table_names([tbl_nm1,
                                                         tbl_nm2,
                                                         tbl_nm3,
                                                         tbl_nm4])
        self.assertEqual(sorted_nms, [tbl_nm4,
                                      tbl_nm3,
                                      tbl_nm1,
                                      tbl_nm2,
                                      ])
                 
    #-------------------------
    # testLoadLogging 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testLoadLogging(self):
        '''
        The table LoadLog, which should be created automatically,
        if it is not present, and should recorde table name, loade time,
        and number of rows.
        '''
        test_table_name = 'Terms'
        db = self.prep_obj.db
        self.removeAllUnittestTables(db)
        
        # For convenience:
        load_log_tbl_nm = CanvasPrep.log_table_name
        db.createTable(test_table_name, {'col1': 'int'}, temporary=False)
        
        # Add three rows
        db.insert(test_table_name, {'col1': 10})
        db.insert(test_table_name, {'col1': 20})
        db.insert(test_table_name, {'col1': 30})
                
        tbl_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
        
        self.assertEqual(tbl_names, [test_table_name])

        # Get date before log entry (datetime below yields, e.g. '2019-08-23T20:31:26.926837')
        (date_str_before, _time_str) = datetime.datetime.utcnow().isoformat().split('T')
        
        # Log the table:
        self.prep_obj.log_table_creation(test_table_name)
        
        # Get current date ('2019-08-23T20:31:26.926837')
        (date_str_after, _time_str) = datetime.datetime.utcnow().isoformat().split('T')
        
        
        # Check that the load table now exists:
        tbl_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
        self.assertCountEqual(tbl_names, [test_table_name, load_log_tbl_nm])
        
        # Check the content, ensuring the refresh timestamps
        # comes back in UTC:
        query = f'''SELECT tbl_name, 
                           CONVERT_TZ(time_refreshed, @@session.time_zone, '+00:00') AS `utc_time_refreshed`,
                           num_rows
                      FROM {load_log_tbl_nm}'''
                      
        res = self.db.query(query)
        
        # Should be one row:
        self.assertEqual(db.result_count(), 1)
        (tbl_nm, utc_time_loaded, num_rows) = res.next()
        
        self.assertEqual(tbl_nm, test_table_name)
        
        # Date must be either the date just before the 
        # log entry, or the one just after (in case the
        # test happens just at midnight):
        # The load log entry timestamps have form: 2019-08-23 20:25:25
        (load_date, _load_time) = utc_time_loaded.isoformat().split('T')
        self.assertTrue(load_date == date_str_before or load_date == date_str_after)
        
        self.assertEqual(num_rows, 3)
                
    
    # ------------------------------- Utilities -------------------------

    #------------------------------------
    # check_table_existence 
    #-------------------    
    
    def check_table_existence(self, db, _db_schema, num_expected_tables=0):
        # Get remaining tables:
        table_names = self.utils.get_tbl_names_in_schema(db, _db_schema)
        
        self.assertEqual(len(table_names), num_expected_tables)
        
               
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
              
    #-------------------------
    # removeAllUnittestTables 
    #--------------
    
    def removeAllUnittestTables(self, db):
        '''
        Find all tables in the Unittest db,
        and remove them.
        
        @param db: db object
        @type db: pymysql_utils
        '''
        tbl_names = self.utils.get_tbl_names_in_schema(db, self.db_schema)
        for tbl_name in tbl_names:
            db.dropTable(tbl_name)
            
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetPreexistingTables']
    unittest.main()
