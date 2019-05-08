'''
Created on Mar 7, 2019

@author: paepcke
'''

from explorecourses import CourseConnection
from html.parser import HTMLParser

from explorecourses.classes import Course
from typing import List


class ExploreCoursesHtmlGetter(CourseConnection):
    '''
    classdocs
    '''

    #------------------------------------
    # Constructor 
    #-------------------    

    def __init__(self):
        '''
        Constructor
        '''
        CourseConnection.__init__(self)
        self.html_parser = ECHtmlParser()
        
    #------------------------------------
    # get_courses_by_query
    #-------------------    
    
    def get_courses_by_query(self, query: str, *filters: str, year=None) -> List[Course]:

        """
        Gets all courses matched by a search query.

        Args:
            query (str): The search query.
            *filters (str): Search query filters.
            year (Optional[str]): The academic year within which to retrieve 
                courses (e.g., "2017-2018"). Defaults to None.

        Returns:
            List[Course]: The courses matching the search query.

        """
        #*******************
#         query = 'AFRICAAM'
#         filters = ['filter-departmentcode-AFRICAAM']
#         year = '20182019'
        
        query = 'CS 106A'
        filters = ['filter-departmentcode-CS']
        year = '20182019'
        #*******************
        url = self._URL + "search"

        payload = {
            "view": "catalog",
            "filter-coursestatus-Active": "on",
            "q": query,
        }
        payload.update({f: "on" for f in filters})
        if year: payload.update({"academicYear": year})
        
        #****************
        # payload.update({'page': 0})
        #****************

        res = self._session.get(url, params=payload)

        root = self.html_parser.feed(str(res.content))
        courses = root.findall(".//course")

        return [Course(course) for course in courses]
        
# ----------------------------- HTML Parser ---------------

class ECHtmlParser(HTMLParser):
    
    def handle_data(self, data):
        print("Data: '%s'" % data)
        
    def handle_starttag(self, tag, attrs):
        print("Encountered a start tag:", tag)

    def handle_endtag(self, tag):
        print("Encountered an end tag :", tag)             
        
# ----------------------------- Main -------------------------

if __name__ == '__main__':
    
    getter = ExploreCoursesHtmlGetter()
    getter.get_courses_by_query('fake', 'fake')
    