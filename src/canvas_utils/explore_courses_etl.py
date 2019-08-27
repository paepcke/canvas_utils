#!/usr/bin/env python
'''
Created on Dec 1, 2018

@author: paepcke
'''

import argparse
import csv
import os
import sys

from lxml import etree
import lxml
from canvas_utils_exceptions import ExploreCoursesError


class ECXMLExtractor(object):
    '''
    Given an Explore Courses XML file, output CSV to stdout.
    One row per course in the catalog:
        'course_code', 'subject', 'course_name', 'unitsMin', 'unitsMax', 'acad_year',\
             'course_id', 'acad_group', 'department', 'acad_career', 'ger_fulfillment',\ 
             'quarter_name', 'instructors'
             
    course_code    : like AA101
    subject        : like AA
    course_name    : like "I Fly Away'
    units_min      : like 1
    units_max      : like 3
    acad_year      : like 2018-2019
    course_id      : like 1405834     University code for each offering
    acad_group     : like ENGR
    department     : like CS or AEROASTRO
    acad_career    : like UG          For 'Undergrad': for whom the crs is intended
    ger_fulfillment: THINK,WAY-ER,Writing 1
    quarter_name   : like Fall or Spring, but can also be 'Fall,Winter,Spring',
                          or 'not given this year'
    instructors : like 'Bill Goph' or 'Bill Gogh,Ann Marie Goddard,...'
    
             
    '''

    #--------------------------
    # Constructor 
    #----------------

    def __init__(self, xml_file):
        '''
        The input XML file of Explore Courses information 
        is read, and spit back out as csv.
        
        @param xml_file: file name or file object
        @type xml_file: {str | file-like}
        '''
        sys.stderr.write('Building xml tree in memory...\n')
        try:
            self.root = etree.parse(xml_file)
        except lxml.etree.XMLSyntaxError as e:
            # Could not parse the file as XML. Is
            # it an HTML formatted message from the
            # site, such as 'Site unavailable?'
            site_msg = self.try_parse_html_msg(xml_file)
            if site_msg is not None:
                raise ExploreCoursesError(f"Problem with the ExploreCourses site: {site_msg}\n")
            else:
                raise ExploreCoursesError(f"Failed to parse ExploreCourses XML file: {repr(e)}\n")
                return
            
        sys.stderr.write('Done building xml tree in memory.\n')
        self.print_explore_courses_table(self.root)

    #-------------------------
    # try_parse_html_msg 
    #--------------
    
    def try_parse_html_msg(self, file_path):
        '''
        Sometimes the ExploreCourses service is down, and
        responds with an HTML msg instead of XML content.
        Try whether this is true by parsing the 'xml' file
        as HTML, and returning the HEAD/TITLE. An example
        title is: '503 Service Unavailable'.
        Return None if no such message can be found, else
        the string.
        
        @param file_path: full path to file
        @type file_path: str
        @return: message string, or None
        @rtype {None | str}
        '''
        try:
            root = etree.parse(file_path, etree.HTMLParser())
            msg_arr = root.xpath("/html/head/title/text()")
            if len(msg_arr) > 0:
                return msg_arr[0]
            return None 
        except Exception as e:
            sys.stderr.write(f"Attempted parsing site message, but: {repr(e)}\n")
            return None
        
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
		           gers
		           unitsMin
		           unitsMax
		           administrativeInformation
		               courseId
		               academicGroup
		               academicOrganization
		               academicCareer
				   <sections>         
				       <section>
				           <instructors>
				               <instructor>
				                   <firstName>
				                   <middleName>
				                   <lastName>
                    ...		               
		         /course           
		   ...
		/xml      
        
        @param xml_root:
        @type xml_root:
        '''
        # The unix dialect quotes all values, and
        # uses \n as line terminator:
        csv_writer = csv.writer(sys.stdout, dialect='unix')
        header = ['course_code', 'subject', 'course_name', 'units_min', 'units_max', 'acad_year', 
                  'course_id', 'acad_group', 'department', 'acad_career', 'ger_fulfillment', 
                  'quarter_name', 'instructors']
        csv_writer.writerow(header)
        # Grab one <course>...</course> element at a time,
        # pull out what we need, and add the tag text to 
        # db_tuple:
        for course_el in xml_root.getiterator('course'):
            db_tuple = []
            subject = course_el.find('subject').text
            db_tuple.append(subject + course_el.find('code').text)
            db_tuple.append(subject),
            db_tuple.append(course_el.find('title').text)
            db_tuple.append(course_el.find('unitsMin').text)
            db_tuple.append(course_el.find('unitsMax').text)
            db_tuple.append(course_el.find('year').text)
            
            admin_info_el = course_el.find('administrativeInformation')
            db_tuple.append(admin_info_el.find('courseId').text)
            db_tuple.append(admin_info_el.find('academicGroup').text)
            db_tuple.append(admin_info_el.find('academicOrganization').text)
            db_tuple.append(admin_info_el.find('academicCareer').text)
            
            gen_ed_reqs = course_el.find('gers')
            db_tuple.append(gen_ed_reqs.text if gen_ed_reqs is not None else '')
            
            # Find a list of 'attribute' elements for this course.
            # That's where the 'not given this year', or 'Autumn'
            # is stored:
            #     <attribute>
            #             <name>NQTR</name>
            #             <value>AUT</value>
            #             <description>Autumn</description>
            #             <catalogPrint>true</catalogPrint>
            #             <schedulePrint>false</schedulePrint>
            #     </attribute>
            #
            # We replace 'Autumn' with 'Fall' b/c Canvas uses that.
            #
            # But: a course can have multiple of these entries when
            #      a course is given in multiple quarters. In that
            #      case: create a comma-separated string: 'Fall,Winter,Spring' 

            attr_els = course_el.xpath('attributes/attribute')
            quarter_names = ''
            for attr_el in attr_els:
                if attr_el.xpath('name/text()') == ['NQTR']:
                    quarter_name = attr_el.xpath('description/text()')[0]
                    if quarter_name == 'Autumn':
                        quarter_name = 'Fall'
                    if len(quarter_names) == 0:
                        quarter_names = quarter_name
                    else:
                        quarter_names += ',' + quarter_name

            if len(quarter_names) == 0:
                quarter_names = '""'                    
            db_tuple.append(quarter_names)
            
            instructors_list = []
            instructors_els =  course_el.xpath('sections/section/schedules/schedule/instructors/instructor')
            
            # MySQL seems to have trouble when last column
            # (i.e. instructors) are missing. We would (correctly)
            # put a trailing comma at the end of the CSV line. But
            # in this case MySQL slurs the line together with the next
            # line (maybe a /n vs. /r/n issue). So put something there.
              
            if len(instructors_els) == 0:
                instructors_list.append('None listed')
            for instructor_el in instructors_els:
                first_name_list  = instructor_el.xpath('firstName/text()')
                first_name  = first_name_list[0] if len(first_name_list) > 0 else '' 
                middle_name_list = instructor_el.xpath('middleName/text()')
                middle_name = middle_name_list[0] if len(middle_name_list) > 0 else '' 
                last_name_list  = instructor_el.xpath('lastName/text()')
                last_name       = last_name_list[0] if len(last_name_list) > 0 else '' 
                full_name = '{0}{1} {2}'.format(first_name,
                                                 ' ' + middle_name if len(middle_name) > 0 else '',
                                                 last_name
                                                 )
                instructors_list.append(full_name)
                
            # Some courses list huge numbers of instructors, all of
            # which are usable for the course, but only one at a time.
            # Do ellipses after some number of names:
            if len(instructors_list) > 10:
                instructors_flattened = ', '.join(instructors_list[0:10]) + '...'
            else:
                instructors_flattened = ', '.join(instructors_list)
            db_tuple.append(instructors_flattened)
            # One row (i.e. course entry in the course catalog is done):
            csv_writer.writerow(db_tuple)

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
    