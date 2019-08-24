#!/usr/bin/env python
'''
Created on Aug 14, 2019

@author: paepcke
'''
from _io import StringIO
import copy
import os
import re
import sys
      
class QuerySorter(object):
    '''
    Runs through all queries in the Queries subdirectory,
    and looks for query text that refers to other tables.
    We assume that the files in Queries all have the names
    of tables, with extension '.sql' appended.
    
    Will order the list of tables such that all dependencies
    of MySQL running the table X sql on other tables having
    been created first are satisfied.
    
    Raises ValueError if circular dependency is detected.
    '''

    #-------------------------
    # Constructor 
    #--------------

    def __init__(self, unittests=False):
        '''
        Constructor
        '''
        
        self.curr_dir    = os.path.dirname(__file__)
        self.query_dir   = os.path.join(self.curr_dir, 'Queries')
        
        # Get basenames of query files, e.g. Terms.sql:
        self.query_file_names = os.listdir(self.query_dir)
        self.table_names = [file_name.split('.')[0] 
                            for file_name in self.query_file_names
                            if file_name.split('.')[1] == 'sql']
                
        if unittests:
            # Allow unittests to call the various methods
            # in isolation:
            return
        
        self.query_texts = self.get_query_texts(self.query_file_names)
        
        self.precedence_dict = self.build_precedence_dict(self.query_texts) 
        self._sorted_table_names = self.sort(self.precedence_dict)

    #-------------------------
    # property sorted_table_names 
    #--------------

    @property
    def sorted_table_names(self):
        return self._sorted_table_names
        
    #-------------------------
    # get_query_texts 
    #--------------
    
    def get_query_texts(self, file_basenames):
        '''
        Read all queries in files within Query.
        Return a dict {table_name : "the query text"}
        Leave out lines with sharp char (comment) at
        the start
        
        @param file_basenames: names of query file names in Queries
            (not full paths)
        @type file_basenames: [str]
        @return: dictionary mapping table names to the SQL text
            that creates them
        @rtype: {str : str}
        '''
        
        full_query_paths = [os.path.join(self.query_dir, file_name)
                            for file_name in file_basenames
                              if file_name.endswith('.sql')]            
        text_dict = {}
        
        for query_path in full_query_paths:
            # Table name is name of file without extension:
            table_name = os.path.splitext(os.path.basename(query_path))[0]
            with open(query_path, 'r') as fd:
                in_buf = StringIO(fd.read())
            # Discard comments with hash char at start of line:
            out_buf = StringIO()
            for line in in_buf:
                if line[0] == '#':
                    continue
                out_buf.write(line)
                
            # Store the entire query file content
            # in the value of the table dict:
            text_dict[table_name] = out_buf.getvalue()

        return text_dict  

    #-------------------------
    # build_precedence_dict 
    #--------------

    def build_precedence_dict(self, text_dict):
        '''
        Given a dict: {<table_name> : <query_text_StringIO_buf},
        construct a dict:
        
           {<table_name> : [table_name, table_name, ...]}
           
        where the array contains names of tables that must
        be processed before the table_name in the key.
        
        Strategy:
        
        1. Build a regular expression that will find every
           table name in a string, and makes a regegx group
           out of it. Ex: r"(tbl1_name)|(tbl2_name)|(tbl3_name)"
        
        2. For each query string that creates a table, apply
           the search pattern to get tuples of groups. For 
           string "I am tbl1_name\nAnd you are tbl3_name"
           We get:
                [('tbl2_name','',''),('','','tbl3_name')]
                
        3. Pick the non-empty table names out from each tuple
           to get a list of mentioned table names.  
        
        @param text_dict: dict table_name to query text StringIO buffer
        @type text_dict: {str : str}
        @return: dict mapping a table name to an array of
            table names that need to be processed earlier.
        @rtype: {str : [str]}
        '''
        
        precedence_dict = {}
        
        # Build a regular expression that makes a group out
        # of every occurrence of a table name in a multi-line
        # string: r"(<tablename1>)|(<tablename2>)|..."
        
        table_names = text_dict.keys()
        # Convert ['table1', 'table2', ...] to ['(table1)', '(table2)', ...]:
        table_name_grps = [f"({table_name})" for table_name in table_names]
        # Put the regex OR operator between each group:
        search_pattern = re.compile('|'.join(table_name_grps))
        
        for (table_name, query_str) in text_dict.items():
            precedence_set = set()
            
            # Get a list of groups of table names:
            # [('','Table3', ''), ('','', 'Table10'),
            # where each table name is one found in the
            # query string:
            group_tuples = search_pattern.findall(query_str)
            
            # Go through each tuple and find the group that is
            # not empty, and is not the name of the table whose
            # dependencies we are trying to find:
            
            for group_tuple in group_tuples: 
                found_tbl_names = [found_table_name for found_table_name in group_tuple 
                                   if len(found_table_name) > 0 and found_table_name != table_name] 
                precedence_set = precedence_set.union(set(found_tbl_names))

            precedence_dict[table_name] = list(precedence_set)

        return precedence_dict
    
    #-------------------------
    # sort 
    #--------------
        
    def sort(self, precedence_dict):
        '''
        Given a dict:
           {table_name3 : [table_name1, table_name2],
            table_name1 : [],
            table_name2 : [table_name1]
            }
        returns an ordered list of table names such that
        there will not be a "unknown table" error when 
        loading the corresponding files in order.
        
        @param precedence_dict: map from table names to lists of
            tables that must be loaded ahead of time.
        @type precedence_dict: { str : [str]}
        @return: ordered list of table names
        @rtyp: [str]
        @raise TableError: if there two tables have a mutual dependency,
            or if any table in the queries has not corresponding 
            .sql file in Queries.
        '''
        # Check for cycles: tables that mutually require the
        # other one to be loaded first: Throws a value
        # errors with informative msg if dicovers a conflict.
        # Else returns True:
        ordered_tables = self.detect_mutual_table_dependencies(precedence_dict)
        return ordered_tables
        
    #-------------------------
    # detect_mutual_table_dependencies 
    #--------------
    
    def detect_mutual_table_dependencies(self, precedence_dict, table_list_todo=None, tables_done=[], tables_being_considered=[]):
        '''
        Given a precedence dict: {table : [table1, table2, ...]} of
        tables and their dependencies, return True if there are
        no mutual dependencies. Else raise ValueError with informative
        message.
        
        A mutual dependency occurs when:
             {table1 : [table2],
              table2 : [table1]
              }
              
        or with more complexity: 
         
             {table1 : [table2],
              table2 : [table3],
              table3 : [table1]
              }
              
        The method is recursive, diving depth-first into the
        dependencies.              
        
        From the top level, only the precedence_dict is typically
        provided. The remaining args are for recursive calls.
        
        @param precedence_dict: dict of table interdependencies
        @type precedence_dict: {str : [str]}
        @param table_list_todo: list of table names that are to
            be examined for conflicts.
        @type table_list_todo: [str]
        @param tables_done: list of tables that are already processed
        @type tables_done: [str]
        @param tables_being_considered: list of tables that are being
            processed in the current layers of recursion
        @type tables_being_considered: [str]
        @return: list of tables in the order in which they can be loaded.
        @rtype: [str]
        @raise: TableError if mutual dependency is found, or a table name
            appears in the queries that does not have a corresponding .sql
            file in Queries.
        '''

        if table_list_todo is None:
            # First time in (i.e top level) 
            top_level = True
            # Copy the passed-in precedence_dict, b/c belo we pop 
            # values off some of its entries.            
            precedence_dict = copy.deepcopy(precedence_dict)
            
            # Total number of tables to examine:
            table_list_todo = list(precedence_dict.keys())
            
            # Right off the bat: declare all tables without
            # dependencies winners: Transfer them to tables_done:
            no_precedence_table_it = filter(lambda tname: len(precedence_dict[tname]) == 0, table_list_todo)
            tables_done = [tname for tname in no_precedence_table_it]
            # Remove the done ones from the todos:
            table_list_todo = [tbl_name for tbl_name in table_list_todo if tbl_name not in tables_done]
            
            # Sort tables todo by decreasing number of 
            # dependencies that each table needs. The
            # array pop() in the while loop will therefore 
            # do as many low dependency tables as possible:
            table_list_todo = sorted(table_list_todo, 
                                    key=lambda tbl_name: len(precedence_dict[tbl_name]), 
                                    reverse=True)
            # No dependency chain yet:
            tables_being_considered = []
            # No error encountered yet:
            original_error = None
            
        else:
            # This is a recursive call:
            top_level = False
            
        while True:
            try:
                curr_table = table_list_todo.pop()
            except IndexError:
                # All done:
                return tables_done
            
            if curr_table in tables_done:
                # Satisfied this table's dependencies
                # earlier, or has none:
                continue
            
            if curr_table in tables_being_considered:
                raise InternalTableError(curr_table, "Mutual load order dependency")
            
            try: 
                curr_dependencies = precedence_dict[curr_table]
            except KeyError as e:
                raise InternalTableError(curr_table, f"Missing table file {curr_table}.sql in Queries directory")
                
            satisfied = [dep for dep in curr_dependencies if dep in tables_done]
            curr_dependencies = [unfilled for unfilled in curr_dependencies if unfilled not in satisfied] 
            
            if len(curr_dependencies) > 0:
                try:
                    tables_being_considered.append(curr_table)
                    tables_done = self.detect_mutual_table_dependencies(precedence_dict, 
                                                                        table_list_todo=curr_dependencies, 
                                                                        tables_done=tables_done, 
                                                                        tables_being_considered=tables_being_considered
                                                                        )
                    tables_being_considered.pop()
                except InternalTableError as e:
                    original_error = e
                    # Unwind the recursion:
                    if not top_level:
                        raise e
                    
                # Recursion unwound, check for error:
                if original_error:
                    # Add the current table to the end of
                    # the dependency chain. That will be 
                    # the same as the list head. Ex:
                    #   [CourseEnrollment, Terms, Student, CourseEnrollment]
                    tables_being_considered.append(curr_table)
                    # Build nice error message
                    raise TableError(tuple(tables_being_considered), original_error.message)

            tables_done.append(curr_table)

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
            
# -------------------- Main --------------------

if __name__ == '__main__':

    if len(sys.argv) > 1:
        print("Usage: no command line options. When run, prints list of aux tables\n" +
              "whose .sql in subdirectory Queries must be run in order to avoid\n" +
              "MySQL table-not-found errors. Used internally."
              )
        sys.exit()
    
    sorter = QuerySorter()
    print (f"Table order:\n{sorter.sorted_table_names}")