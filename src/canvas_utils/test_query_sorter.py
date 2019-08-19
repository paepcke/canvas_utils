'''
Created on Aug 15, 2019

@author: paepcke
'''
import unittest
from query_sorter import QuerySorter, TableError

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

        ordered_list = self.sorter.detect_mutual_table_dependencies(precedence_dict)
        self.assertCountEqual(ordered_list, ['Terms', 'CourseEnrollment'])
        
        # Dependency on non-existing table:
        precedence_dict = {'Terms': ['Student'], 'CourseEnrollment': ['Terms']}

        try:
            ordered_list = self.sorter.detect_mutual_table_dependencies(precedence_dict)
        except TableError as e:
            #print(e.message())
            self.assertEqual(e.message, "('CourseEnrollment', 'Terms', 'CourseEnrollment'): Missing table file Student.sql in Queries directory")


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
        with self.assertRaises(TableError):
            ordered_table_list = self.sorter.sort(precedence_dict)
        
        # Test unresolved requirement-detection:
        precedence_dict = {'Terms': [], 'CourseEnrollment': ['NoTable']}
        with self.assertRaises(TableError):
            ordered_table_list = self.sorter.sort(precedence_dict)
            
    #-------------------------
    # testDependencyLoopDetection
    #--------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testDependencyLoopDetection(self):
        # No loop:
        precedence_dict = {'Terms': [], 'CourseEnrollment': ['Terms']}
        self.assertTrue(self.sorter.detect_mutual_table_dependencies(precedence_dict))
         
        # Simple loop, immediately visible:
        precedence_dict = {'Terms': ['CourseEnrollment'], 'CourseEnrollment': ['Terms']}
        try:
            self.sorter.detect_mutual_table_dependencies(precedence_dict)
            self.fail("Expected ValueError, which was not raised.")
        except TableError as e:
            # Ensure there is an explanatory error text:
            self.assertEqual(e.table_tuple, ('CourseEnrollment', 'Terms', 'CourseEnrollment'))
                
        # Indirect dependency: A dependsOn B, which dependsOn C, which dependsOn A:
        precedence_dict = {'Terms': ['CourseEnrollment'], 'CourseEnrollment': ['Student'], 'Student' : ['Terms']}
        try:
            self.sorter.detect_mutual_table_dependencies(precedence_dict)
            self.fail("Expected ValueError, which was not raised.")
        except TableError as e:
            # Ensure there is an explanatory error text:
            self.assertCountEqual(e.table_tuple, ('Student', 'Terms', 'CourseEnrollment', 'Student'))
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()