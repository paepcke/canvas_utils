'''
Created on Sep 12, 2019

@author: paepcke
'''
import os
from pathlib import Path
import socket
import tempfile
import unittest

from canvas_utils_exceptions import TableExportError
from final_sanity_check import SanityChecker

TEST_ALL = True
#TEST_ALL = False

TEST_EMAIL = True

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
        
        # Make sanity checker believe that cronlog files 
        # are in test_tmp_dir/cronlogs:
        SanityChecker.CRONLOG_DIR = Path(os.path.join(self.test_tmpdir_path, 'cronlogs'))
        
        # Create the cronlogs file there:
        Path(self.sanity_checker.CRONLOG_DIR).mkdir()
        
        # Content to put into tables to pretend they were
        # filled properly:
        self.tbl_test_content = b"some content."
        
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
            self.sanity_checker.check_exported_file_lengths()
            self.assertEqual(len(e.table_list), len(self.all_table_names))
            
        # Same with just one zero-len:
        for table_name in self.sanity_checker.all_tables:
            self.create_copied_file(table_name, be_empty=False)
        # Update copied-file notion in sanity_checker:
        self.sanity_checker.init_table_vars()
        
        # But pretend that all we expect for file sizes is
        # the length of our test content:
        
        reasonable_sizes_dict = self.sanity_checker.putative_file_sizes_dict
        self.fill_pretend_reasonable_file_lengths(reasonable_sizes_dict) 
        
        # Should give no error of zero length file:
        self.assertTrue(self.sanity_checker.check_exported_file_lengths())
        
        # Truncate just one:
        zero_len_file_path = os.path.join(self.test_tmpdir_path, self.all_table_names[-1] + '.csv')
        # Truncate the file:
        with open(zero_len_file_path, 'wb'):
            pass
            
        try:
            self.sanity_checker.check_exported_file_lengths()
            self.fail(f'Should have seen a one-table zero-len error: {os.path.basename(zero_len_file_path)}')
        except TableExportError as e:
            self.assertEqual(e.table_list, [self.all_table_names[-1]])


    #-------------------------
    # test_error_log_analysis
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def test_error_log_analysis(self):
        
        self.create_log_file(include_errors=False)

        # Have method being tested find this error-free
        # log file and report no error found:
         
        has_error = self.sanity_checker.check_cronlog_errors()
        self.assertIsNone(has_error)
        
        # Check a log file with errors:
        error_lines = self.create_log_file(include_errors=True)
        has_error = self.sanity_checker.check_cronlog_errors()
        self.assertEqual(has_error, error_lines)
        
        # To check that only the latest log is looked at:
        
        self.create_log_file(include_errors=False)

        # Should get error-free even though now one older
        # log message does have an error:
         
        has_error = self.sanity_checker.check_cronlog_errors()
        self.assertIsNone(has_error)
            
    #-------------------------
    # test_error_emails 
    #--------------

    @unittest.skipIf(not TEST_EMAIL, 'Skipping email test')
    def test_error_emails(self):
        
        e1 = TableExportError("Testing deficiency 1", ['Tbl1', 'Tbl2'])
        e2 = TableExportError("Testing deficiency 2", ['Tbl1', 'Tbl3'])
        
        this_host = socket.gethostname()
        if this_host == self.sanity_checker.DEVMACHINE_HOSTNAME:
            self.assertTrue(self.sanity_checker.maybe_send_mail([e1, e2]))
        else:
            self.assertFalse(self.sanity_checker.maybe_send_mail([e1, e2]))

            
    # --------------------------- Utilities ---------------        
    
    #-------------------------
    # fill_pretend_reasonable_file_lengths 
    #--------------
    
    def fill_pretend_reasonable_file_lengths(self, file_length_dict):
        '''
        The sanity checker uses a json file to remember
        the expected file lengths of csv files. Make those
        lengths be the length of our test copied-file content.
        
        @param file_length_dict: the SanityCheck instance's reasonable size dict
        @type file_length_dict: {str : int}
        '''
        for tbl_name in file_length_dict.keys():
            file_length_dict[tbl_name] = len(self.tbl_test_content)
        
    
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
                fd.write(self.tbl_test_content)
        
    #-------------------------
    # create_log_file
    #--------------
    
    def create_log_file(self, include_errors=True):
        '''
        Create a log file with or without lines that
        include the string 'ERROR'. Return None if the
        requested file wasn't to have had error lines.
        Else return a list of error lines usable in an
        assert statement.
        
        @param include_errors: if True, some lines with the word
            'ERROR' are included.  
        @type include_errors: bool
        @return: None if no error lines were to be included,
            else return list of error lines that mixed into the log file
        @rtype: {None | [str]}
        '''
        log_file_obj = Path(self.sanity_checker.CRONLOG_DIR, 'log_with_errors.log')
        
        if include_errors:
            error_lines = ['line 2 is an ERROR.\n',
                           'ERROR in line 4\n'
                           ]
        
        with open(log_file_obj, 'w') as fd:
            fd.write('line 1 is without error.\n')
            if include_errors:
                fd.write(error_lines[0])
            fd.write('line 3 is fine\n')
            if include_errors:            
                fd.write(error_lines[1])
            
        return None if not include_errors else error_lines

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCopiedFilesPresent']
    unittest.main()