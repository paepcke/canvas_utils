'''
Created on Aug 15, 2019

@author: paepcke
'''
import unittest
from query_sorter import QuerySorter
from pkg_resources._vendor.pyparsing import operatorPrecedence
from pip._vendor.six import assertCountEqual

TEST_ALL = True
#TEST_ALL = False

class QuerySorterTester(unittest.TestCase):

    #-------------------------
    # setUp
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.sorter = QuerySorter(unittests=True)
        
    #-------------------------
    # testBuildPrecedenceList 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testBuildPrecedenceList(self):
        
        txt1 = "Create table Terms()\n"
        txt2 = "Create table Terms()\n and also CourseEnrollment"
        txt_dict = {'Terms' : txt1,
                    'CourseEnrollment' : txt2}
        precedence_dict = self.sorter.build_precedence_dict(txt_dict)
        self.assertDictEqual(precedence_dict, {'Terms': [], 'CourseEnrollment': ['Terms']}) 

    #-------------------------
    # testResolveDependencies 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')    
    def testResolveDependencies(self):

        # Test table without dependencies:
        precedence_dict = {'Terms': [], 'CourseEnrollment': ['Terms']}

        free_list = self.sorter.resolve_dependencies(precedence_dict, 'Terms', [])
        self.assertCountEqual(free_list, ['Terms'])

        free_list = self.sorter.resolve_dependencies(precedence_dict, 'CourseEnrollment', [])
        self.assertCountEqual(free_list, ['Terms', 'CourseEnrollment'])

    #-------------------------
    # testSort 
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testSort(self):
        
        # Test table Terms with no dependency, and table CourseEnrollment with 
        # only table Terms as dependency:
        precedence_dict = {'Terms': [], 'CourseEnrollment': ['Terms']}
        ordered_table_list = self.sorter.sort(precedence_dict)
        self.assertEqual(ordered_table_list, ['Terms', 'CourseEnrollment'])
        
        # Test loop detection:
        precedence_dict = {'Terms': ['CourseEnrollment'], 'CourseEnrollment': ['Terms']}
        with self.assertRaises(ValueError):
            ordered_table_list = self.sorter.sort(precedence_dict)
        
        # Test unresolved requirement-detection:
        precedence_dict = {'Terms': [], 'CourseEnrollment': ['NoTable']}
        with self.assertRaises(ValueError):
            ordered_table_list = self.sorter.sort(precedence_dict)
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()