'''
Created on Aug 14, 2019

@author: paepcke
'''
from _symtable import FREE
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
        query_file_names = os.listdir(self.query_dir)
        self.table_names = [file_name.split('.')[0] for file_name in query_file_names]
                
        if unittests:
            # Allow unittests to call the various methods
            # in isolation:
            return
        
        self.query_texts = self.get_query_texts(query_file_names)
        
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
        
        @param file_basenames: names of query file names in Queries
            (not full paths)
        @type file_basenames: [str]
        @return: dictionary mapping table names to the SQL text
            that creates them
        @rtype: {str : str}
        '''
        
        full_query_paths = [os.path.join(self.query_dir, file_name) for file_name in file_basenames]
        text_dict = {}
        
        for query_path in full_query_paths:
            table_name = os.path.splitext(os.path.basename(query_path))[0]
            with open(query_path, 'r') as fd:
                text_dict[table_name] = fd.read()

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
            precedence_arr = []
            
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
                precedence_arr.extend(found_tbl_names)

            precedence_dict[table_name] = precedence_arr

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
        @raise ValueError: if there two tables have a mutual dependency. 
        '''
        
        working_prec_dict = copy.deepcopy(precedence_dict)
        
        # Before doing any work, ensure that all tables
        # listed in dependency arrays have an implementation:
        dependency_set = set()
        for prec_list in precedence_dict.values():
            dependency_set.update(prec_list)
        existing_tables = set(precedence_dict.keys())
        if len(dependency_set - existing_tables) > 0:
            raise ValueError(f"Some table(s) in queries don't have corresponding query files: " +
                                f"{str(dependency_set - existing_tables)}")
        
        free_list = []
        tbls_to_examine = len(working_prec_dict)
        while len(free_list) < tbls_to_examine:
            free_list_len_at_start   = len(free_list)   
            for tbl_nm in working_prec_dict.keys():
                free_list = self.resolve_dependencies(working_prec_dict, tbl_nm, free_list)
            # Stopped making progress?
            if free_list_len_at_start == len(free_list):
                # We have a loop in the dependency:
                raise ValueError(f"Dependency loop. Remaining tables: {str(working_prec_dict)}")
        return free_list
            
    #-------------------------
    # resolve_dependencies 
    #--------------
         
    def resolve_dependencies(self, precedence_dict, table_to_examine, free_list):
        '''
        Given an ordered list of table names that won't
        incur an error when MySQL loads the corresponding file
        in the given order. Return a new list to which 
        more tables are appended. The new tabales are found
        by running though the dependencies of table_to_examine,
        and finding chains of tables that can be loaded before
        table_to_examine without errors. 
        
        @param precedence_dict: map from table names to lists of
            tables that must be loaded ahead of time.
        @type precedence_dict: { str : [str] }
        @param table_to_examine: name of one table whose dependencies
            are to be resolved if possible
        @type table_to_examine: str
        @param free_list: ordered list of table names that can be
            loaded into MySQL in order without incurring a table not
            found error.
        @type free_list: [str]
        @return: a new, augmented free_list
        @rtype: [str]
        '''
        dependency_list = precedence_dict[table_to_examine].copy()
        if len(dependency_list) == 0:
            free_list.append(table_to_examine)
            return free_list
        
        # Go through each table in the given table's precedence list,
        # and see whether it depends on anyone:
        dependency_list_copy = dependency_list.copy()
        # Go through the table_to_examine's dependencies:
        for tbl_nm in dependency_list_copy:
            # If table already free of dependencies, nothing to do:
            if tbl_nm in free_list:
                dependency_list.remove(tbl_nm)
                continue
            # Or if it does not have any dependencies, it is now free:
            if len(precedence_dict[tbl_nm]) == 0:
                # Remove tbl_nm from table_to_examine's dependency list:
                dependency_list.remove(tbl_nm)
                free_list.append(tbl_nm)
        # If we cleared table_to_examine's dependency
        # list, push that table itself onto the freelist:
        if len(dependency_list) == 0 and\
            table_to_examine not in free_list:
                free_list.append(table_to_examine)
        return free_list

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