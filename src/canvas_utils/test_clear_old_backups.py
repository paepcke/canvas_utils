'''
Created on Aug 20, 2019

@author: paepcke
'''
import unittest

from clear_old_backups import BackupRemover
from config_info import ConfigInfo


TEST_ALL = True
#TEST_ALL = False


class ClearBackupTablesTester(unittest.TestCase):

    #-------------------------
    # setUpClass 
    #--------------

    @classmethod
    def setUpClass(cls):
        super(ClearBackupTablesTester, cls).setUpClass()
        cls.num_to_keep = 1
        
        # Get whether to test on localhost, or on 
        # remote host:
        
        config_info = ConfigInfo()
        cls.test_host = config_info.test_default_host
        cls.user = config_info.test_default_user
        
        cls.tbl_remover = BackupRemover(num_to_keep=cls.num_to_keep,
                                        target_db=('unittest' if cls.test_host=='localhost' else None),
                                        unittests=True)
    
    #-------------------------
    # tearDownClass 
    #--------------
    @classmethod
    def tearDownClass(cls):
        super(ClearBackupTablesTester, cls).tearDownClass()
        cls.tbl_remover.close()
        
    #-------------------------
    # setUp 
    #--------------
        
    def setUp(self):
        self.tbl_remover = ClearBackupTablesTester.tbl_remover
        self.db_obj = self.tbl_remover.db_obj
        self.db_name = self.db_obj.dbName()
        self.canvas_prepper = self.tbl_remover.canvas_prepper

    #-------------------------
    # testFindTablesToConsider
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testFindTablesToConsider(self):
        
        all_tbls     = ['Terms']
        to_consider  = ['AssignmentSubmission']
        self.assertEqual(self.tbl_remover.find_tables_to_consider(all_tbls, to_consider),
                         [])
        to_consider  = ['Terms']
        self.assertEqual(self.tbl_remover.find_tables_to_consider(all_tbls, to_consider),
                         ['Terms'])
        
        all_tbls     = ['Terms', 'Terms_2019_01_10_14_14_40_123456']
        to_consider  = ['Terms']
        self.assertEqual(self.tbl_remover.find_tables_to_consider(all_tbls, to_consider),
                         ['Terms', 'Terms_2019_01_10_14_14_40_123456'])
        
        all_tbls     = ['Terms', 
                        'Terms_2018_01_10_14_14_40_123456', 
                        'Terms_2019_01_10_14_14_40_123456',
                        'Terms_2020_01_10_14_14_40_123456',
                        ]
        to_consider  = ['Terms']
        self.assertEqual(self.tbl_remover.find_tables_to_consider(all_tbls, to_consider),
                        ['Terms', 
                        'Terms_2018_01_10_14_14_40_123456', 
                        'Terms_2019_01_10_14_14_40_123456',
                        'Terms_2020_01_10_14_14_40_123456',
                        ])        
        
        
        
    #-------------------------
    # testBadNumToKeepInput 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testBadNumToKeepInput(self):
        try:
            self.tbl_remover.num_to_keep = 5 # More than there are names
            all_tbls     = ['Terms', 
                            'Terms_2018_01_10_14_14_40_123456', 
                            'Terms_2019_01_10_14_14_40_123456',
                            'Terms_2020_01_10_14_14_40_123456',
                            ]
            to_consider  = ['Terms']
            self.assertEqual(self.tbl_remover.find_tables_to_consider(all_tbls, to_consider),
                            ['Terms', 
                            'Terms_2018_01_10_14_14_40_123456', 
                            'Terms_2019_01_10_14_14_40_123456',
                            'Terms_2020_01_10_14_14_40_123456',
                            ])

        finally:
            # Restore the original num_to_keep
            self.tbl_remover.num_to_keep = ClearBackupTablesTester.num_to_keep

    #-------------------------
    # testRemoval 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testRemoval(self):
        self.removeAllUnittestTables(self.db_obj)
        self.createTestDb()
        
        # All there?
        table_names = self.canvas_prepper.getTblNamesInSchema(self.db_obj, self.db_name)
        self.assertEqual(len(table_names), 4)

# ------------------------- Utilities --------------------

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
        tbl_names = self.getTblNamesInSchema(db, self.db_name)
        for tbl_name in tbl_names:
            db.dropTable(tbl_name)

    #------------------------------------
    # getTblNamesInSchema 
    #-------------------    
    
    def getTblNamesInSchema(self, db, db_schema_name):
        '''
        Given a db schema ('database name' in MySQL parlance),
        return a list of all tables in that db.
        
        @param db: pymysql_utils database object
        @type db: MySQLDB
        @param db_schema_name: name of MySQL db in which to find tables
        @type db_schema_name: str
        '''
        tables_res = db.query(f'''
                              SELECT TABLE_NAME 
                                FROM information_schema.tables 
                               WHERE table_schema = '{db_schema_name}';
                              ''')
        table_names = [table_name for table_name in tables_res]
        return table_names        

    #-------------------------
    # createTestDb 
    #--------------
        
    def createTestDb(self):
        db = self.db_obj
        db.createTable('Terms', {'col1' : 'int'})
        db.createTable('Terms_2018_01_10_14_14_40_123456', {'col1' : 'int'})
        db.createTable('Terms_2019_01_10_14_14_40_123456', {'col1' : 'int'})
        db.createTable('Terms_2020_01_10_14_14_40_123456', {'col1' : 'int'})
        
# ------------------------- Main --------------------    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()