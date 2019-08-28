'''
Created on Aug 27, 2019

@author: paepcke
'''
from _datetime import timezone
from datetime import datetime
import unittest
from io import StringIO

from refresh_history import LoadHistoryLister


TEST_ALL = True
#TEST_ALL = False

class Test(unittest.TestCase):

    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.buildEventDicts()
        
    #-------------------------
    # testTwoTablesSameName
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testShowAll(self):
        out_fd = StringIO()
        self.refresh_lister = LoadHistoryLister(latest_only=True, unittests=True)

        self.refresh_lister.print_latest_refresh(latest_only=False, 
                                                 load_log_content=[self.tbl1_load_record1,
                                                                   self.tbl1_load_record2],
                                                 out_fd=out_fd
                                                 )
        res = out_fd.getvalue()
        #print(res)
        self.assertEqual(res, '''
Aux tables in Unittest:

                    Table Name      Last Refreshed       Num Rows
                          tbl1    2010-09-30 13:10:04 PDT   10  
                          tbl1    2011-09-30 13:10:04 PDT   20  
''')
        
    #-------------------------
    # testOnlyOneOfSameTable
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skipped')
    def testOnlyOneOfSameTable(self):
        out_fd = StringIO()
        self.refresh_lister = LoadHistoryLister(unittests=True)

        self.refresh_lister.print_latest_refresh(latest_only=True, 
                                                 load_log_content=[self.tbl1_load_record1,
                                                                   self.tbl1_load_record2],
                                                 out_fd=out_fd
                                                 )
        res = out_fd.getvalue()
        #print(res)
        self.assertEqual(res, '''
Aux tables in Unittest:

                    Table Name      Last Refreshed       Num Rows
                          tbl1    2011-09-30 13:10:04 PDT   20  
''')
        
    #-------------------------
    # testOnlyOneThreeTblsPreserveOrder 
    #--------------
      
    #******@unittest.skipIf(not TEST_ALL, 'Temporarily skipped')  
    def testOnlyOneOfThreeTbls(self):

        out_fd = StringIO()
        self.refresh_lister = LoadHistoryLister(unittests=True)

        self.refresh_lister.print_latest_refresh(latest_only=True, 
                                                 load_log_content=[self.tbl2_load_record1,
                                                                   self.tbl1_load_record1,
                                                                   self.tbl1_load_record2],
                                                 out_fd=out_fd
                                                 )
        res = out_fd.getvalue()
        #print(res)
        truth_str = '''
Aux tables in Unittest:

                    Table Name      Last Refreshed       Num Rows
                          tbl1    2011-09-30 13:10:04 PDT   20  
                          tbl2    2012-09-30 13:10:04 PDT   20  
''' 
        self.assertEqual(res, truth_str)
        
        
        
# ----------------------- Utilities ---------------

    #-------------------------
    # buildEventDicts 
    #--------------

    def buildEventDicts(self):

        self.tbl1_load_record1 = {
            'tbl_name' : 'tbl1',
            'num_rows' : 10,
            'time_refreshed' : datetime(2010, 9, 30, 20, 10, 4, tzinfo=timezone.utc)
            }
        
        # Same table as above, but later refresh time:
        self.tbl1_load_record2 = {
            'tbl_name' : 'tbl1',
            'num_rows' : 20,
            'time_refreshed' : datetime(2011, 9, 30, 20, 10, 4, tzinfo=timezone.utc)
            }

        self.tbl2_load_record1 = {
            'tbl_name' : 'tbl2',
            'num_rows' : 20,
            'time_refreshed' : datetime(2012, 9, 30, 20, 10, 4, tzinfo=timezone.utc)
            }
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()