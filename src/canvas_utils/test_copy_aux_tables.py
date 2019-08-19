'''
Created on Aug 6, 2019

@author: paepcke
TODO: 
  o Add test for index on 'text'

'''
import configparser
import getpass
import os
import shutil
import unittest

from pymysql_utils.pymysql_utils import MySQLDB

from copy_aux_tables import AuxTableCopier
from copy_aux_tables import Schema


#from copy_aux_tables import SchemaColumn
#from copy_aux_tables import SchemaIndex
TEST_ALL = True
#TEST_ALL = False


class AuxTableCopyTester(unittest.TestCase):

    #test_host  = 'canvasdata-prd-db1.ci6ilhrc8rxe.us-west-1.rds.amazonaws.com'
    #test_host  = 'localhost'
    #mysql_user = 'canvasdata_aux'
    #db         = None
    
    #------------------------------------
    # setUpClass 
    #-------------------    

    @classmethod
    def setUpClass(cls):
        super(AuxTableCopyTester, cls).setUpClass()
        
        # Read config file to see which MySQL server test_host we should
        # run the tests on. If setup.py does not exist, copy
        # setupSample.py to setup.py:
        
        conf_file_dir  = os.path.join(os.path.dirname(__file__), '../../')
        conf_file_path = os.path.join(conf_file_dir, 'setup.cfg')
        if not os.path.exists(conf_file_path):
            shutil.copyfile(os.path.join(conf_file_dir, 'setupSample.cfg'),
                            os.path.join(conf_file_dir, 'setup.cfg'))        
        
        config = configparser.ConfigParser()
        setup_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../setup.cfg'))
        config.read(setup_file_name)
        test_host       = cls.test_host = config['TESTMACHINE']['mysql_host']
        user            = cls.user = config['TESTMACHINE']['mysql_user']

        cls.test_host = test_host
        cls.user      = user
        
        if test_host == 'localhost':
            # No password for user unittest:
            mysql_pwd = cls.mysql_pwd = ''
        else:
            mysql_pwd = cls.mysql_pwd = cls.get_db_pwd()

        cls.mysql_pwd = mysql_pwd

        
        db = AuxTableCopyTester.db = MySQLDB(user=user, 
                                             passwd=mysql_pwd, 
                                             db='information_schema', 
                                             host=test_host)
        # If not working on localhost, where we expect a db
        # 'Unittest" Ensure there is a unittest db for us to work in.
        # We'll delete it later:
        
        if test_host == 'localhost':
            cls.db_name = 'Unittest'
            cls.mysql_pwd = ''
        else:
            unittest_db_nm = 'unittests_'
            nm_indx = 0
            try:
                print("Looking for unused database name for unittest activity...")
                while True:
                    nm_indx += 1
                    db_name = unittest_db_nm + str(nm_indx)
                    db_exists_cmd = f'''
                                     SELECT COUNT(*) AS num_dbs 
                                       FROM information_schema.schemata
                                      WHERE schema_name = '{db_name}';
                                     '''
                    try:
                        num_existing = db.query(db_exists_cmd).next()
                        if num_existing == 0:
                            # Found a db name that doesn't exist:
                            break
                    except Exception as e:
                        db.close()
                        raise RuntimeError(f"Cannot probe for existing db '{db_name}': {repr(e)}")
            
                print(f"Creating database {db_name} for unittest activity...")
                # Create the db to play in:
                try:
                    db.execute(f"CREATE DATABASE {db_name};")
                except Exception as e:
                    raise RuntimeError(f"Cannot create temporary db '{db_name}': {repr(e)}")
            finally:
                cls.db_name = db_name
        db.close()
        
    #-------------------------
    # tearDownClass 
    #--------------

    @classmethod
    def tearDownClass(cls):
        super(AuxTableCopyTester, cls).tearDownClass()
        try:
            #AuxTableCopyTester.copier_obj.close()
            pass
        except Exception:
            pass

    #-------------------------
    # setUp 
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        # Get a new copier, after closing the current one:
        try:
            self.copier.close()
        except Exception as _e:
            pass
        
        mysql_pwd = AuxTableCopyTester.mysql_pwd
        test_host = AuxTableCopyTester.test_host
        user      = AuxTableCopyTester.user
        self.db_name   = AuxTableCopyTester.db_name
        
        self.copier = AuxTableCopier(user=user, 
                                     host=test_host, 
                                     pwd=mysql_pwd,
                                     unittests=True,
                                     unittest_db_name=self.db_name)
        
        self.db        = self.copier.db

        # Make a fresh version of the unittest tables.
        self.removeAllUnittestTables(self.db)        
        self.create_test_db()

    #-------------------------
    # tearDown  
    #--------------

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.copier.close()
        
        
    # ----------------------------- Test Methods -------------

    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testGetSchemaCreation(self):
        schema_obj = Schema('unittest')
        schema_obj.push('id', 'int')

        # Get this new column name back:
        col_names = schema_obj.col_names(quoted=False)
        self.assertEqual(col_names, ['id'])
        
        create_stmt = schema_obj.construct_create_table()
        self.assertEqual(create_stmt, 
                         '''CREATE TABLE unittest (
	id  int);''')

        # Test for quoted col names:
        col_names = schema_obj.col_names(quoted=True)
        self.assertEqual(col_names, ['"id"'])
        
        # Second col:
        schema_obj.push('col1', 'varchar(20)')
        create_stmt = schema_obj.construct_create_table()        
        self.assertEqual(create_stmt, 
                         '''CREATE TABLE unittest (
	id  int,
	col1  varchar(20));''')
        
        # Third col:
        schema_obj.push('col2', 'int', col_default=0)
        create_stmt = schema_obj.construct_create_table()        
        self.assertEqual(create_stmt, 
                         '''CREATE TABLE unittest (
	id  int,
	col1  varchar(20),
	col2  int DEFAULT 0);''')
        
        # Forth col, but put into second place, right after 'id':
        schema_obj.push('col3', 'int', col_position=2)
        create_stmt = schema_obj.construct_create_table()        
        self.assertEqual(create_stmt, 
                         '''CREATE TABLE unittest (
	id  int,
	col3  int,
	col1  varchar(20),
	col2  int DEFAULT 0);''')
        
    #-------------------------
    # testIndexesWithoutLength 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testIndexesWithoutLength(self):
        '''
        Make the test schema, which looks like this:
            CREATE TABLE Unittest (
              id int(11),
              var1 int(11),
              var2 int(11) DEFAULT 10,
              var3 varchar(40) DEFAULT NULL
              )
        
        look like this:
            CREATE TABLE Unittest (
              id int(11) NOT NULL AUTO_INCREMENT,
              var1 int(11) DEFAULT NULL,
              var2 int(11) DEFAULT NULL,
              var3 varchar(40) DEFAULT NULL,
              PRIMARY KEY (id),
              KEY var3_idx (var3),
              KEY var1_2_idx (var1,var2);        

        '''
            
        # Make the id a primary key:
        schema_obj = self.create_test_schema()
        schema_obj.add_index('Primary', 'id')
 
        create_stmt = schema_obj.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int,
	var1  int,
	var2  int DEFAULT 10,
	var3  varchar(40),
	PRIMARY KEY (id));''')
        
        # Put index named var3_idx onto col var3:
        schema_obj.add_index('var3_idx', 'var3')
        create_stmt = schema_obj.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int,
	var1  int,
	var2  int DEFAULT 10,
	var3  varchar(40),
	PRIMARY KEY (id),
	KEY var3_idx (var3));''')
        
        # Finally: the composite index:
        schema_obj.add_index('var1_2_idx', 'var1')
        schema_obj.add_index('var1_2_idx', 'var2')
        create_stmt = schema_obj.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int,
	var1  int,
	var2  int DEFAULT 10,
	var3  varchar(40),
	PRIMARY KEY (id),
	KEY var1_2_idx (var1,var2),
	KEY var3_idx (var3));''')

    #-------------------------
    # testIndexesWithLength 
    #--------------
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testIndexesWithLength(self):
        '''
        Use table Unittest1, which has a 'text'-typed
        column:
				CREATE TABLE `Unittest1` (
				  `var1` text,
				  `var2` varchar(30) DEFAULT NULL,
				  `var3` double DEFAULT NULL,
				  KEY `var1_idx` (`var1`(20)),
				  KEY `var2_idx` (`var2`),
				  KEY `var3_idx` (`var3`))       
				  
        We create an index on col 'var1' to ensure the
        length spec shows up:
        '''
        # Create schema with just one index: on a 'text' col
        # with index length of 20: 
        schema_obj = self.create_test_schema(tbl='Unittest1')
 
        create_stmt = schema_obj.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest1 (
	id  int,
	var1  text,
	var2  varchar(30),
	var3  double,
	KEY var1_txt_indx (var1(20)));''')

        
    #-------------------------
    # testAutoIncrement 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testAutoIncrement(self):
        '''
        Create schema_prim_key_auto:
           CREATE TABLE Foo (
               id PRIMARY KEY AUTO_INCREMENT,
               var int
               )
               
        and check that the 'id' column obj is marked
        as auto_increment, and that the generated CREATE TABLE
        statement reflects that. 
        
        Then same with schema_no_key_auto:
           CREATE TABLE Foo (
               var int auto_increment
               )
        '''
        # A primary key with auto increment:
        schema_prim_key_auto = Schema('Unittest')
        schema_prim_key_auto.push('id', 'int', col_is_auto_increment=True)
        schema_prim_key_auto.add_index('Primary', 'id')
        
        id_col = schema_prim_key_auto['id']
        self.assertEqual(id_col.col_is_auto_increment, True)
        
        create_stmt = schema_prim_key_auto.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int AUTO_INCREMENT,
	PRIMARY KEY (id));''')
        
        # Regular key with auto increment:
        schema_auto = Schema('Unittest')
        schema_auto.push('id', 'int', col_is_auto_increment=True)
        schema_auto.add_index('key', 'id')
        
        id_col = schema_auto['id']
        self.assertEqual(id_col.col_is_auto_increment, True)
        
        create_stmt = schema_auto.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int AUTO_INCREMENT,
	KEY key (id));''')

    #-------------------------
    # testPopulateMetadata 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testPopulateMetadata(self):
        
        # Have copier read the table from the DB,
        # and create a Schema object from it:
        self.copier.populate_table_schema('Unittest')
        
        # Get a CREATE TABLE statement from this 
        # inspection of the table in the db:
        create_stmt = self.copier.schema.construct_create_table()
        self.assertEqual(create_stmt,
                         '''CREATE TABLE Unittest (
	id  int AUTO_INCREMENT,
	var1  int,
	var2  int,
	var3  varchar(40),
	PRIMARY KEY (id),
	KEY var1_2_idx (var1,var2),
	KEY var3_idx (var3));''')
            
    #-------------------------
    # testCopyOneTable 
    #--------------
    
    #*****@unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testCopyOneTable(self):
        
        # Add some rows to the test table:
        self.db.bulkInsert('Unittest', 
                           ('var1', 'var2', 'var3'),
                           [(10,20,'ten,twenty'),
                            (30,40,'thirty/forty'),
                            (50,60,'fifty'),
                            ]
                           )
        
        # Have copier read the table from the DB,
        # and create a Schema object from it:
        self.copier.populate_table_schema('Unittest')
        
        # Check automatically created destination directory:
        dest_file = os.path.join(self.copier.dest_dir, 'Unittest.csv')
        self.assertEqual(dest_file, '/tmp/Unittest.csv')
        
        # Do the copying:
        self.copier.copy_one_table_to_csv()
        
        with open(dest_file, 'r') as fd:
            file_content = fd.read()
            self.assertEqual(file_content,
                             '''"id","var1","var2","var3"
"1","10","20","ten,twenty"
"2","30","40","thirty/forty"
"3","50","60","fifty"
'''
)
    
    #-------------------------
    # testCopyMultipleTables 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, 'Temporarily skip this test.')
    def testCopyMultipleTables(self):
        
        # Add some rows to the test tables:
        self.db.bulkInsert('Unittest', 
                           ('var1', 'var2', 'var3'),
                           [(10,20,'ten,twenty'),
                            (30,40,'thirty/forty'),
                            (50,60,'fifty'),
                            ]
                           )

        self.db.bulkInsert('Unittest1', 
                           ('var1', 'var2', 'var3'),
                           [("This is text.","One varchar", 10.5),
                            ("More, text",'Another varchar', 20.5),
                            ('"Text" galore',"Lots of varchar",30.5),
                            ]
                           )
        
        self.copier.copy_to_csv_files(['Unittest', 'Unittest1'])
        with open('/tmp/Unittest1.csv', 'r') as fd:
            content = fd.read()
        self.assertEqual(content,
                        '''"var1","var2","var3"
"This is text.","One varchar","10.5"
"More, text","Another varchar","20.5"
"""Text"" galore","Lots of varchar","30.5"
''')
            
# ----------------------------------- Utilities -------------

    #-------------------------
    # log_into_mysql 
    #--------------
        
    @classmethod
    def log_into_mysql(cls, user, pwd, db=None):
        
        host = AuxTableCopyTester.test_host
        try:
            # Try logging in, specifying the database in which all the tables
            # will be created: 
            db = MySQLDB(user=user, passwd=pwd, db=db, host=host)
        except ValueError as e:
            # Does unittest not exist yet?
            if str(e).find("OperationalError(1049,") > -1:
                # Log in without specifying a db to 'use':
                db =  MySQLDB(user=user, passwd=pwd, host=host)
                # Create the db:
                db.execute('CREATE DATABASE %s;' % 'unittest')
            else:
                raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        except Exception as e:
            raise RuntimeError("Cannot open Canvas database: %s" % repr(e))
        
        return db

    #-------------------------
    # create_test_schema 
    #--------------

    def create_test_schema(self, tbl='Unittest'):
        '''
        Create a Schema object for a table that would 
        look like this:
            CREATE TABLE Unittest (
              id int,
              var1 int,
              var2 int DEFAULT 10,
              var3 varchar(40) DEFAULT NULL
              )
        '''

        if tbl == 'Unittest':
            schema = Schema('Unittest')
            schema.push('id', 'int')
            schema.push('var1', 'int')
            schema.push('var2', 'int', col_default=10)
            schema.push('var3', 'varchar(40)')
            return schema
        else:
            schema = Schema('Unittest1')
            schema.push('id', 'int')
            schema.push('var1', 'text')
            schema.push('var2', 'varchar(30)'),
            schema.push('var3', 'double')
            schema.add_index('var1_txt_indx', 'var1', index_length=20)
            return schema

    #-------------------------
    # create_test_db
    #--------------
    
    def create_test_db(self):
        '''
        Create a table like this:
            CREATE TABLE Unittest (
              id int(11) NOT NULL AUTO_INCREMENT,
              var1 int(11) DEFAULT NULL,
              var2 int(11) DEFAULT NULL,
              var3 varchar(40) DEFAULT NULL,
              PRIMARY KEY (id),
              KEY var3_idx (var3),
              KEY var1_2_idx (var1,var2);
              
        and another one like:
			CREATE TABLE Unittest1 (
			  id int,
			  var1 text,
			  var2 varchar(30) DEFAULT NULL,
			  var3 double DEFAULT NULL,
			  KEY var1_idx (var1(20)),
			  KEY var2_idx (var2),
			  KEY var3_idx (var3)        
        '''
        self.db.execute(f"DROP TABLE IF EXISTS Unittest")
        self.db.execute(f'''
                        CREATE TABLE Unittest (
                          id int NOT NULL AUTO_INCREMENT,
                          var1 int DEFAULT NULL,
                          var2 int DEFAULT NULL,
                          var3 varchar(40) DEFAULT NULL,
                          PRIMARY KEY (id),
                          KEY var3_idx (var3),
                          KEY var1_2_idx (var1,var2)
                          );        
                        ''')
        
        self.db.execute(f"DROP TABLE IF EXISTS Unittest1")
        self.db.execute(f'''
						CREATE TABLE Unittest1 (
						  var1 text,
						  var2 varchar(30) DEFAULT NULL,
						  var3 double DEFAULT NULL,
						  KEY var1_idx (var1(20)),
						  KEY var2_idx (var2),
						  KEY var3_idx (var3))       
                        ''')
        
    #-------------------------
    # removeAllUnittestTables 
    #--------------
    
    def removeAllUnittestTables(self, db):
        '''
        Find all tables in the Unittest db,
        and remove them.
        
        @param db: db object
        @type db: pymysql_utils
        '''
        tbl_names = self.get_tbl_names_in_schema(db, self.db_name)
        for tbl_name in tbl_names:
            db.dropTable(tbl_name)
        
    #------------------------------------
    # get_tbl_names_in_schema 
    #-------------------    
    
    def get_tbl_names_in_schema(self, db, db_schema_name):
        '''
        Given a db schema ('database name' in MySQL parlance),
        return a list of all tables in that db.
        
        @param db: pymysql_utils database object
        @type db: MySQLDB
        @param db_schema_name: name of MySQL db in which to find tables
        @type db_schema_name: str
        '''
        tables_res = db.query(f'''
                              SELECT TABLE_NAME 
                                FROM information_schema.tables 
                               WHERE table_schema = '{db_schema_name}';
                              ''')
        table_names = [table_name for table_name in tables_res]
        return table_names        
    
    #-------------------------
    # get_db_pwd 
    #--------------
    
    @classmethod
    def get_db_pwd(cls):
        
        if cls.test_host == 'localhost':
            return ''
        
        HOME = os.getenv('HOME')
        if HOME is not None:
            default_pwd_file = os.path.join(HOME, '.ssh', AuxTableCopier.canvas_pwd_file)
            if os.path.exists(default_pwd_file):
                with open(default_pwd_file, 'r') as fd:
                    pwd = fd.readline().strip()
                    return pwd
            
        # Ask on console:
        pwd = getpass.getpass("Password for Canvas database: ")
        return pwd
    
    
# --------------------------------------- Main -----------------        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()