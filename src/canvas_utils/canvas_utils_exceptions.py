'''
Created on Aug 24, 2019

@author: paepcke
'''

# --------------------- Exception Classes --------------------

#-------------------------
# ConfigurationError 
#--------------


class ConfigurationError(Exception):
    '''
    Exception raised when the setup.cfg file cannot
    be read, or holds faulty content.

    Attributes:
        message -- explanation of the error
    '''

    def __init__(self, message):
        self._message_fragment = message
        super().__init__(self.message)
        
    
    @property
    def message(self):
        return f"{self._message_fragment}"

#-------------------------
# TableError
#--------------

class TableError(Exception):
    '''
    Exception raised for tables missing in Queries subdir,
    or mutually required tables. 

    Attributes:
        tuple of two table names involved in an error
        message -- explanation of the error
    '''

    def __init__(self, table_tuple, message):
        self.table_tuple = table_tuple
        self._message_fragment = message
        super().__init__(self.message)
        
    
    @property
    def message(self):
        return f"{self.table_tuple}: {self._message_fragment}"

#-------------------------
# TableExportError 
#--------------

class TableExportError(Exception):
    '''
    Exception in the context of exporting the aux
    tables to csv files in an output directory.
    
    Attributes:
       table_list: list of table names that were implicated in the error.
       message: error message
    '''
    
    def __init__(self, message, table_list=[]):
        self._table_list = table_list
        self._message    = message
        
    @property
    def message(self):
        return self._message
    
    @property
    def table_list(self):
        return self._table_list
    

#-------------------------
# DatabaseError  
#--------------

class DatabaseError(Exception):
    '''
    Error during interaction with database.
    One property: message
    '''
    def __init__(self, message):
        self._message_fragment = message
        super().__init__(self.message)
        
    
    @property
    def message(self):
        return f"{self._message_fragment}"
    
#-------------------------
# ExploreCoursesError
#--------------
    
class ExploreCoursesError(Exception):
    '''
    Error during pull from ExploreCourses site
    One property: message
    '''
    def __init__(self, message):
        self._message_fragment = message
        super().__init__(self.message)
        
    
    @property
    def message(self):
        return f"{self._message_fragment}"
    
    
#-------------------------
# InternalTableError 
#--------------
    
class InternalTableError(Exception):

    def __init__(self, table_name, message, implicated_tables=[]):
        self.table_name = table_name
        self._message_fragment = message
        self.implicated_tables = implicated_tables
        super().__init__(self.message)

    @property
    def message(self):
        return self._message_fragment
