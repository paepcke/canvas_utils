'''
Created on Aug 14, 2019

@author: paepcke
'''
import filecmp
import os
import shutil
import tempfile
import unittest

from convert_queries import QueryConverter


class QueryConverterTester(unittest.TestCase):

    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.generateTestData()
        
        # Can't use TemporaryDirectory(), b/c open(..., 'w') only writes
        # after closing, and TemporaryDirectory dirs disappear with closing.
        self.test_dest_dir_name = tempfile.mkdtemp(prefix="QueryReplacementTest")
        # Path to the test files:
        self.test_files_dir = os.path.join(os.path.dirname(__file__), 'TestData')
        
    #-------------------------
    # tearDown 
    #--------------
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.tmp_file_all.close()
        self.tmp_file_aux.close()
        shutil.rmtree(self.test_dest_dir_name)
        
    #-------------------------
    # testReplacement 
    #--------------

    def testReplacement(self):

        # Replacements for <canvas_db>, <canvas_aux>,
        # and <data_dir> respectively:

        # Convert file that has '<canvas_aux>' in it:
        aux_repl_file = os.path.join(self.test_files_dir, 'query_conv_aux.sql')
        aux_dest_file = os.path.join(self.test_dest_dir_name, os.path.basename(aux_repl_file))
        _converter = QueryConverter('canvasdata_prd', 
                                    'canvasdata_aux', 
                                    '/tmp',
                                    files_to_replace=aux_repl_file,
                                    dest_dir=self.test_dest_dir_name)
        # Get the true value
        dst_truth_file = os.path.join(self.test_files_dir, 'query_conv_aux_truth.sql')
        self.assertTrue(filecmp.cmp(dst_truth_file, aux_dest_file))
        
        # Convert file that has '<canvas_prd>' in it:
        aux_repl_file = os.path.join(self.test_files_dir, 'query_conv_prd.sql')
        aux_dest_file = os.path.join(self.test_dest_dir_name, os.path.basename(aux_repl_file))
        _converter = QueryConverter('canvasdata_prd', 
                                    'canvasdata_aux', 
                                    '/tmp',
                                    files_to_replace=aux_repl_file,
                                    dest_dir=self.test_dest_dir_name)
        # Get the true value
        dst_truth_file = os.path.join(self.test_files_dir, 'query_conv_prd_truth.sql')
        self.assertTrue(filecmp.cmp(dst_truth_file, aux_dest_file))

        # Convert file that has '<canvas_prd>' in it:
        aux_repl_file = os.path.join(self.test_files_dir, 'query_conv_data_dir.sql')
        aux_dest_file = os.path.join(self.test_dest_dir_name, os.path.basename(aux_repl_file))
        _converter = QueryConverter('canvasdata_prd', 
                                    'canvasdata_aux', 
                                    '/tmp',
                                    files_to_replace=aux_repl_file,
                                    dest_dir=self.test_dest_dir_name)
        # Get the true value
        dst_truth_file = os.path.join(self.test_files_dir, 'query_conv_data_dir_truth.sql')
        self.assertTrue(filecmp.cmp(dst_truth_file, aux_dest_file))

        # Convert file that has '<canvas_prd>' in it:
        aux_repl_file = os.path.join(self.test_files_dir, 'query_conv_all.sql')
        aux_dest_file = os.path.join(self.test_dest_dir_name, os.path.basename(aux_repl_file))
        _converter = QueryConverter('canvasdata_prd', 
                                    'canvasdata_aux', 
                                    '/tmp',
                                    files_to_replace=aux_repl_file,
                                    dest_dir=self.test_dest_dir_name)
        # Get the true value
        dst_truth_file = os.path.join(self.test_files_dir, 'query_conv_all_truth.sql')
        self.assertTrue(filecmp.cmp(dst_truth_file, aux_dest_file))

        

# --------------------- Utilities -----------------

    #-------------------------
    # generateTestData 
    #--------------
    
    def generateTestData(self):
        '''
        Generate instance vars:
        
            all_truth
            aux_truth
            prd_truth
            data_truth
            
        and:
            all_challenge
            aux_challenge
            prd_challenge
            data_challenge
        
        The first batch are text in which placeholders were
        manually replaced. The second are corresponding texts
        with placeholders.
        '''
        # Temp files where tests above can write their 
        # filled-in strings:
        
        self.tmp_file_all  = tempfile.NamedTemporaryFile(suffix='.txt', prefix='query_conv_all_test.txt', dir='/tmp')
        self.tmp_file_prd  = tempfile.NamedTemporaryFile(suffix='.txt', prefix='query_conv_prd_test.txt', dir='/tmp')
        self.tmp_file_aux  = tempfile.NamedTemporaryFile(suffix='.txt', prefix='query_conv_aux_test.txt', dir='/tmp')
        self.tmp_file_data = tempfile.NamedTemporaryFile(suffix='.txt', prefix='query_conv_data_test.txt', dir='/tmp')
        
        # Paths to manually converted test files:
        data_dir = os.path.join(os.path.dirname(__file__), 'TestData')
        sql_cmd_all_truth_path  	= os.path.join(data_dir, 'query_conv_all_truth.sql')
        sql_cmd_aux_truth_path  	= os.path.join(data_dir, 'query_conv_aux_truth.sql')
        sql_cmd_prd_truth_path  	= os.path.join(data_dir, 'query_conv_prd_truth.sql')
        sql_cmd_data_dir_truth_path = os.path.join(data_dir, 'query_conv_data_dir_truth.sql')        
        
        # Paths to texts with placeholders to test conversion on: 
        sql_cmd_all_challenge_path  	  = os.path.join(data_dir, 'query_conv_all.sql')
        sql_cmd_aux_challenge_path  	  = os.path.join(data_dir, 'query_conv_aux.sql')
        sql_cmd_prd_challenge_path  	  = os.path.join(data_dir, 'query_conv_prd.sql')
        sql_cmd_data_dir_challenge_path   = os.path.join(data_dir, 'query_conv_data_dir.sql')        
        
        # Retrieve ground truth of converted data:
        with open(sql_cmd_all_truth_path, 'r') as fd:
            self.all_truth = fd.read()
            
        with open(sql_cmd_aux_truth_path, 'r') as fd:
            self.aux_truth = fd.read()
            
        with open(sql_cmd_prd_truth_path, 'r') as fd:
            self.prd_truth = fd.read()
        
        with open(sql_cmd_data_dir_truth_path, 'r') as fd:
            self.data_dir_truth = fd.read()

        # Retrieve the text with placeholders:
        with open(sql_cmd_all_challenge_path, 'r') as fd:
            self.all_challenge = fd.read()
            
        with open(sql_cmd_aux_challenge_path, 'r') as fd:
            self.aux_challenge = fd.read()
            
        with open(sql_cmd_prd_challenge_path, 'r') as fd:
            self.prd_challenge = fd.read()
        
        with open(sql_cmd_data_dir_challenge_path, 'r') as fd:
            self.data_dir_challenge = fd.read()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()