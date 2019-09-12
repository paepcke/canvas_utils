'''
Created on Sep 12, 2019

@author: paepcke
'''
import os
import tempfile
import unittest

from final_sanity_check import SanityChecker
from canvas_utils_exceptions import TableExportError

TEST_ALL = True
#TEST_ALL = False

class SanityCheckTester(unittest.TestCase):

    #-------------------------
    # setUp
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.sanity_checker = SanityChecker(unittest=True)
        # Modify the table export directory so as not
        # to interfere with the real one:
        self.test_tmpdir      = tempfile.TemporaryDirectory(prefix='sanity_check_unittests')
        self.test_tmpdir_path = self.test_tmpdir.name
        # Have sanity checker assume that copied tables
        # are to be in our temp dir:
        self.sanity_checker.table_export_dir_path = self.test_tmpdir_path
        self.sanity_checker.init_table_vars()
        self.all_table_names = self.sanity_checker.all_tables
        
    #-------------------------
    # tearDown 
    #--------------

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.test_tmpdir.cleanup()

    #-------------------------
    # testCopiedFilesPresent 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testCopiedFilesPresent(self):
        # Create a fake copied file in the 
        # temp dir for each aux table:
        for table_name in self.sanity_checker.all_tables:
            self.create_copied_file(table_name)
        # Update checker's notion of copied tables:
        self.sanity_checker.init_table_vars()
        self.assertTrue(self.sanity_checker.check_num_files())
                        
    #-------------------------
    # testCopiedFilesMissing
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')        
    def testCopiedFilesMissing(self):
        # Seed the temp dir with nothing. All
        # tables should be missing:
        
        try:
            self.sanity_checker.check_num_files()
            self.fail('Should have seen a copy files missing error')
        except TableExportError as e:
            self.assertEqual(len(e.table_list), len(self.all_table_names))
        
    #-------------------------
    # testZeroLengthCopies 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testZeroLengthCopies(self):
        
        # Create a fake, empty copied file in the 
        # temp dir for each aux table:
        for table_name in self.sanity_checker.all_tables:
            self.create_copied_file(table_name)
        # Update copied-file notion in sanity_checker:
        self.sanity_checker.init_table_vars()
        self.assertTrue(self.sanity_checker.check_num_files())
        
        with self.assertRaises(TableExportError) as e:
            self.sanity_checker.check_for_zero_len_exports()
            self.assertEqual(len(e.table_list), len(self.all_table_names))
            
        # Same with just one zero-len:
        for table_name in self.sanity_checker.all_tables:
            self.create_copied_file(table_name, be_empty=False)
        # Update copied-file notion in sanity_checker:
        self.sanity_checker.init_table_vars()
        
        # Should give no error of zero length file:
        self.assertTrue(self.sanity_checker.check_for_zero_len_exports())
        
        # Truncate just one:
        zero_len_file_path = os.path.join(self.test_tmpdir_path, self.all_table_names[-1] + '.csv')
        # Truncate the file:
        with open(zero_len_file_path, 'wb'):
            pass
            
        try:
            self.sanity_checker.check_for_zero_len_exports()
            self.fail(f'Should have seen a one-table zero-len error: {os.path.basename(zero_len_file_path)}')
        except TableExportError as e:
            self.assertEqual(e.table_list, [self.all_table_names[-1]])
            
            
    # --------------------------- Utilities ---------------        
    
    #-------------------------
    #  create_copied_file
    #--------------
    
    def create_copied_file(self, table_name, be_empty=True, ext='.csv'):
        '''
        Creates one file in the temporary dir created
        in setUp() method. Caller controls whether the file
        should be empty, or have some (arbitrary) content in it. 
        
        @param table_name: name of table to which file should
            correspond. Ex. 'Terms'
        @type table_name: str
        @param be_empty: whether or not file should be left empty
        @type be_empty: bool
        @param ext: extension for the file.
        @type ext: str
        '''
        file_path = os.path.join(self.test_tmpdir_path, table_name + ext)
        with open(file_path, 'wb') as fd:
            if not be_empty:
                fd.write(b"some content.")
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCopiedFilesPresent']
    unittest.main()