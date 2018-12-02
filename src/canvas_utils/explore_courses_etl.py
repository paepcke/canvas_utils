#!/usr/bin/env python
'''
Created on Dec 1, 2018

@author: paepcke
'''

import argparse
import os
import sys

from lxml import etree


class ECXMLExtractor(object):
    '''
    Given an Explore Courses XML file, output CSV to stdout.
    One row per course in the catalog:
        'course_code', 'subject', 'unitsMin', 'unitsMax', 'acad_year',\
             'course_id', 'acad_group', 'department', 'acad_career'
             
    '''

    #--------------------------
    # Constructor 
    #----------------

    def __init__(self, xml_file):
        '''
        Constructor
        '''
        sys.stderr.write('Building xml tree in memory...\n')
        self.root = etree.parse(xml_file)
        sys.stderr.write('Done building xml tree in memory.\n')
        self.print_explore_courses_table(self.root)
        
    #--------------------------
    # print_explore_courses_table 
    #----------------
        
    def print_explore_courses_table(self, xml_root):
        '''
        Workhorse. Takes the document root of the internalized XML.
        The expected format is:
        
          xml
		    courses
		        course
		           year 2018-2019
		           subject
		           code (a.k.a. catalog_nbr)
		           title
		           description
		           unitsMin
		           unitsMax
		           administrativeInformation
		               courseId
		               academicGroup
		               academicOrganization
		               academicCareer
		         /course           
		   ...
		/xml      
        
        @param xml_root:
        @type xml_root:
        '''
        header = ['course_code', 'subject', 'unitsMin', 'unitsMax', 'acad_year', 
                  'course_id', 'acad_group', 'department', 'acad_career']
        print(','.join(header))
        # Grab one <course>...</course> element at a time,
        # pull out what we need, and add the tag text to 
        # db_tuple:
        for course_el in xml_root.getiterator('course'):
            db_tuple = []
            subject = course_el.find('subject').text
            db_tuple.append(subject + course_el.find('code').text)
            db_tuple.append(subject),
            db_tuple.append(course_el.find('unitsMin').text)
            db_tuple.append(course_el.find('unitsMax').text)
            admin_info_el = course_el.find('administrativeInformation')
            db_tuple.append(admin_info_el.find('courseId').text)
            db_tuple.append(admin_info_el.find('academicGroup').text)
            db_tuple.append(admin_info_el.find('academicOrganization').text)
            db_tuple.append(admin_info_el.find('academicCareer').text)
            
            # One row (i.e. course entry in the course catalog is done):
            print(','.join(db_tuple))

        #*********
        #print('done')
        #*********

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), 
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description='Turn Explore Courses XML to .csv on stdout.')
    parser.add_argument('input',
                        help='fully qualified file name of the Explore Courses XML file.'
                        )

    args = parser.parse_args();

    ECXMLExtractor(args.input)
    