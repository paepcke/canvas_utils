'''
Created on May 1, 2019

@author: paepcke
'''
import os
from tempfile import mkstemp
import tempfile
import unittest

from copy_aux_tables import AuxTableCopier


class CanvasUtilsTests(unittest.TestCase):


    def testGetPreexistingTables(self):
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"tmpdirname is '[tmpdirname]'")
            
            copier = AuxTableCopier(dest_dir=tmpdirname,
                                    tables={'table1.csv',
                                            'table2.csv'},
                                    copy_format='sql'
                                    )
            _tbl1_fd = open(os.path.join(tmpdirname, 'table1.sql'), 'w')
            _tbl3_fd = open(os.path.join(tmpdirname, 'table3.sql'), 'w')
            
            # Only table1.csv is in the tables list and also in
            # the directory as a .csv file:
            
            existing_tbls = copier.get_existing_tables()
            self.assertEqual(existing_tbls, 'table1')
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetPreexistingTables']
    unittest.main()