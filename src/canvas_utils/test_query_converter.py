'''
Created on Aug 14, 2019

@author: paepcke
'''
import os
import tempfile
import unittest

from convert_queries import QueryConverter, data_dir


class QueryConverterTester(unittest.TestCase):

    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.generateTestData()
        
    #-------------------------
    # tearDown 
    #--------------
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.tmp_file_all.close()
        self.tmp_file_aux.close()
        self.tmp_data_dir.close()
        
    #-------------------------
    # testReplacement 
    #--------------

    def testReplacement(self):

        # Replacements for <canvas_db>, <canvas_aux>,
        # and <data_dir> respectively:
        converter = QueryConverter('canvasdata_prd', 'canvasdata_aux', '/tmp')
        

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
        
        self.tmp_file_all  = tempfile.NamedTemporaryFile(mode='rw', suffix='.txt', prefix='query_conv_all_test.txt', '/tmp')
        self.tmp_file_prd  = tempfile.NamedTemporaryFile(mode='rw', suffix='.txt', prefix='query_conv_prd_test.txt', '/tmp')
        self.tmp_file_aux  = tempfile.NamedTemporaryFile(mode='rw', suffix='.txt', prefix='query_conv_aux_test.txt', '/tmp')
        self.tmp_file_data = tempfile.NamedTemporaryFile(mode='rw', suffix='.txt', prefix='query_conv_data_test.txt', '/tmp')
        
        # Paths to manually converted test files:
        data_dir = os.path.join(os.path.dirname(__file__), 'TestData')
        sql_cmd_all_truth_path  	= os.path.join(data_dir, 'query_conv_all_truth.txt')
        sql_cmd_aux_truth_path  	= os.path.join(data_dir, 'query_conv_aux_truth.txt')
        sql_cmd_prd_truth_path  	= os.path.join(data_dir, 'query_conv_prd_truth.txt')
        sql_cmd_data_dir_truth_path = os.path.join(data_dir, 'query_conv_data_dir_truth.txt')        
        
        # Paths to texts with placeholders to test conversion on: 
        sql_cmd_all_challenge_path  	  = os.path.join(data_dir, 'query_conv_all.txt')
        sql_cmd_aux_challenge_path  	  = os.path.join(data_dir, 'query_conv_aux.txt')
        sql_cmd_prd_challenge_path  	  = os.path.join(data_dir, 'query_conv_prd.txt')
        sql_cmd_data_dir_challenge_path   = os.path.join(data_dir, 'query_conv_data_dir.txt')        
        
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