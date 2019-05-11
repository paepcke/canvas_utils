'''
Created on May 1, 2019

@author: paepcke
'''
import datetime
#import os
import re
#from tempfile import mkstemp
#import tempfile
import unittest

from canvas_prep import CanvasPrep

# The following is abandoned for now:
#from copy_aux_tables import AuxTableCopier


TEST_ALL = True
#TEST_ALL = False

class CanvasUtilsTests(unittest.TestCase):


    # Production server:
    test_host = 'canvasdata-prd-db1.cupga556ks1y.us-west-1.rds.amazonaws.com'
    # Kathy server:
    #test_host = 'canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com'


    #------------------------------------
    # testGetBackupNames 
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testGetBackupNames(self):
        backup_nm_pat = re.compile(r"[^_]*_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}")
        
        prep_obj = CanvasPrep(host=CanvasUtilsTests.test_host)
        backup_nm = prep_obj.get_backup_table_name('my_table')
        # Does the backup name look like a name with a datetime
        # attached?
        self.assertIsNotNone(backup_nm_pat.search(backup_nm))
        
    #------------------------------------
    # testRecoverTableNmFromBackupName 
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testRecoverTableNmFromBackupName(self):
        prep_obj = CanvasPrep(host=CanvasUtilsTests.test_host)
        backup_name = 'myTable_2019_02_28_15_34_10'
        recovered_name = 'myTable'
        recovered_dt_str = '2019_02_28_15_34_10'
        (nm, dt_str, dt_obj) = prep_obj.backup_table_name_components(backup_name)
        self.assertEqual(nm, recovered_name)
        self.assertEqual(dt_str, recovered_dt_str)
        self.assertTrue(dt_obj < datetime.datetime.now())
        
    #------------------------------------
    # testSaveTables
    #-------------------    
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')    
    def testSaveTables(self):

        try:
            test_table_name = 'unittest_tmp_tbl'
            prep_obj = CanvasPrep(host=CanvasUtilsTests.test_host)
            _db_schema = CanvasPrep.canvas_db_aux
            # Create a test table:
            db = prep_obj.db
            
            db.createTable(test_table_name, {'col1': 'int'}, temporary=False)
            tables_res = db.query(f'''
                                  SELECT TABLE_NAME 
                                    FROM information_schema.tables 
                                   WHERE table_schema = '{_db_schema}';
                                  ''')
            table_names = [res for res in tables_res]
            self.assertIn(test_table_name, table_names)
    
            # Create the backup
            datetime_obj_used = prep_obj.backup_tables([test_table_name])
            # Remember the backup table name, so we can
            # remove it at the end:
            backup_table_name = prep_obj.get_backup_table_name(test_table_name, datetime_obj_used)

            # Get all the tables in the db schema now.
            # Should include the new backup table, but
            # not the original table:
            
            tables_res = db.query(f'''
                                  SELECT TABLE_NAME 
                                    FROM information_schema.tables 
                                   WHERE table_schema = '{_db_schema}';
                                  ''')
            # db.query returned an iterator, get all
            # the db schema's table names:
            table_names = [res for res in tables_res]
            
            # Find the test_table_name with an attached date-time:
            for table_name in table_names:
                # One table name:
                # Try to get a datetime and the original table name
                # from the table name (it could be other, pre-existing
                # tables):
                try:
                    (recovered_table_name, _recovered_dt_str, recovered_dt_obj) =\
                        prep_obj.backup_table_name_components(table_name)
                    if recovered_table_name != test_table_name or\
                        recovered_dt_obj != datetime_obj_used.replace(microsecond=0): 
                        continue
                    else:
                        # All good
                        return
                except ValueError:
                    # Not the backed-up table; next table name:
                    continue
            # Ran through all table names without finding the 
            # backup; fail:
            self.fail()
        finally:
            try:
                # Should fail, unless error happened
                # between creating the test tbl, and
                # turning it into a backup table:
                db.dropTable(test_table_name)
            except:
                pass
            try:
                db.dropTable(backup_table_name)
            except:
                pass
        
    #------------------------------------
    # testGetPreexistingTables 
    #-------------------    

    # Abandoned for now.
#    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
#     def testGetPreexistingTables(self):
#         
#         with tempfile.TemporaryDirectory() as tmpdirname:
#             print(f"tmpdirname is '[tmpdirname]'")
#             
#             copier = AuxTableCopier(dest_dir=tmpdirname,
#                                     tables={'table1.csv',
#                                             'table2.csv'},
#                                     copy_format='sql'
#                                     )
#             _tbl1_fd = open(os.path.join(tmpdirname, 'table1.sql'), 'w')
#             _tbl3_fd = open(os.path.join(tmpdirname, 'table3.sql'), 'w')
#             
#             # Only table1.csv is in the tables list and also in
#             # the directory as a .csv file:
#             
#             existing_tbls = copier.get_existing_tables()
#             self.assertEqual(existing_tbls, 'table1')
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetPreexistingTables']
    unittest.main()