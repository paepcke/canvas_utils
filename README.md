#<p align="center">Canvas Analysis Data Extraction</p>


Utilities for mining Canvas data. Creates a set of 'auxiliary' tables from exports of Canvas data. The generated tables pull from the exports data that are likely to be useful for analysis. New auxiliary tables can easily be added. The package can perform the following functions: 

- Create the auxiliary tables
- Backup/restore the auxiliary tables
- Export the auxiliary tables' schemas as .sql files, and their contents
  to .csv files in a specified directory

In the following we call the MySQL database holding the new auxiliary tables `Auxiliaries.` The following functions are performed when the generation of the `Auxiliaries` tables is requested:

- Backup the existing `Auxiliaries` tables
- Create the tables in `Auxiliaries`
- Ensure that only up to two `Auxiliaries` backups exist to avoid
  excessive disk usage.

The program responsible for table creation is `canvas_prep.py`. It can be called with a `-h` or `--help` option.

The backup copies are managed by `restore_tables.py`, which also answers to the `-h/--help` switch.

Example for creating the tables in `Auxiliaries`:
```
# Create all `Auxiliaries`, backing up the existing tables first:
src/canva_utils/canvas_prep.py

# Create only the CourseEnrollments and AssignmentSubmissions tables:
src/canva_utils/canvas_prep.py --table CourseEnrollments --table AssignmentSubmissions
```

Example for exporting the tables in `Auxiliaries` to .csv with
contents, and .sql for the schema:
```
# Generate one .csv file and one .sql file for every auxiliary table
# in the default directory /tmp:
src/canvas_utils/copy_aux_tables.py

# Export only the CourseEnrollments and AssignmentSubmissions tables
# into /my/own/directory:
src/canvas_utils/copy_aux_tables.py --table CourseEnrollments --table AssignmentSubmissions --destdir /my/own/directory
```

The schema files contain SQL `CREATE TABLE` statements. The .csv files will contain a header line with with column names, followed by the data. All values will be double-quoted.

## Installation

Known unpleasantness: Ensure that mysql_config is installed. It is included in the MySQL server installation. Right now this dependency forces the server installation, even though the server is not used. 
Then:

`pip install canvas_utils`

or clone the repo:

`git clone https://github.com/paepcke/canvas_utils.git`

Switch to the project root directory, create a virtual environment if desired, and run:

`python setup.py install`

to install the needed Python packages.

Check file `setupSample.cfg` in the root directory. If any changes are needed, copy the file to `setup.cfg`, and modify options there.

### Known Unpleasantnesses

1. The pip installation requires `mysqlclient`, even if this package is not used later. There should be an install choice to avoid this requirement when the pure Python option is used later on. The `mysqlclient` will insist on the presence of `mysql_config`. To obtain it (Centos directions below are uncertain):

```
Centos: sudo yum mysql-community-devel
Ubuntu: sudo apt-get install libmysqlclient-dev
```

Normally canvas_utils uses `mysqlclient` to access the MySQL server. On some installations there is a known incompatibility between `mysqlclient` and the `openssl` installation. If this should be the case, follow this procedure:

- Locate the installation of package pymysql_utils. If using Anaconda,
  this package will be in the site-packages of your environment
- Locate pymysql_utils_SAMPLE.cnf in this package  
- Copy that file to pymysql_utils.cfg in the same directory, and
  follow directions in this file.

## Customization

Customization opportunities are a configuration file, command line options to commands, and the addition of new `Auxiliaries` tables.

Customization options comprise:

- The host where the MySQL database resides
- Name of the database (a.k.a. schema) on the MySQL server in which the full
  Canvas export resides.
- Name of the database in which the `Auxiliaries` tables are to be
  created

In addition, the three commands `canvas_prep.py`, `copy_aux_tables.py`, `restore_tables.py` provide a number of command line options. Use the --help switch for details.

The file `setup_Sample.cfg` in the project root directory enables settings for a number of defaults, including login names, database names, and host names.

To make changes in this file, copy it to `setup.cfg` in the root directory. Then follow instructions in the file. This modified copy will be preserved during code updates.

## Addition of new auxiliary tables

Every auxiliary table is defined in a separate .sql file in subdirectory `Queries`. Each file name (without the extension) is the name of one auxiliary table.

To add an additional table, create a file in the `Queries` directory. Make the file name the name of the table, with extension .sql. The `canvas_prep.py` command will do the rest.

Similarly, the existing files may be modified to taste.

### Implementation Note
The table .sql files naturally contain table creation statements. Those statements reference the database (a.k.a. MySQL schema) where the `Auxiliaries` are to be constructed. They also reference the database where the full Canvas exports reside. These names are hard coded into the .sql files.

This decision means that customizations need to replace those hardcoded names with those compatible at the installation site. In an effort to avoid this installation inconvenience, the initial implementation used placeholder names in the .sql files. Those placeholders were automatically replaced by the `canvas_prep.py` code.

User feedback directed the change to hard coding the names. The addition of new tables, or users modifying the existing queries is a more frequent event than globally changing the database names in the sql statements once during the installation.

However, if the database with Canvas full exports is not called canvasdata_prd, or if the desired destination database of the auxiliary tables is not to be called canvasdata_aux, then the aux table creation queries can be changed wholesale using the included utility `convert_queries.py`. The program accepts replacement database names, and replaces the hardcoded database names in the queries.

## Localization

Almost all of this package should work at other universities, with respective customization (see above). There is, however, one Stanford facility called `ExploreCourses`, which holds data about courses. This information is kept in table ExploreCourses. Two options:

Option 1: Construct the course information for your university in the required format: Create in the Data subdirectory a file called explore_courses.csv. The expected schema should match the following MySQL create statement:

```
+-----------------+--------------+
| Field           | Type         |
+-----------------+--------------+
| course_code     | varchar(15)  |
| subject         | varchar(15)  |
| course_name     | varchar(255) |
| units_min       | int(11)      |
| units_max       | int(11)      |
| acad_year       | varchar(25)  |
| course_id       | int(11)      |
| acad_group      | varchar(25)  |
| department      | varchar(40)  |
| acad_career     | varchar(10)  |
| ger_fulfillment | varchar(255) |
| quarter_name    | varchar(40)  |
| instructors     | varchar(255) |
+-----------------+--------------+
```
An example entry database is:

```
    course_code: AMSTUD25Q
        subject: AMSTUD
    course_name: The Origins of the Modern American City, 1865-1920 (HISTORY 55Q, URBANST 25Q)
      units_min: 3
      units_max: 3
      acad_year: 2018-2019
      course_id: 215582
     acad_group: H&S
     department: AMSTU
    acad_career: UG
ger_fulfillment: WAY-ED, WAY-SI
   quarter_name: not given this year
    instructors: None listed
```
An example column name header plus single record .csv entry would be:

```
"course_code","subject","course_name","units_min","units_max","acad_year","course_id","acad_group","department","acad_career",
"ger_fulfillment","quarter_name","instructors"
"ATHLETIC25","ATHLETIC","VARSITY - Gymnastics (Men)","1","2","2019-2020","201035","MED","MEDDPT","UG","","Spring,Winter,Fall",
"Austin Douglas Lee, Thomas G Glielmi, Austin Douglas Lee, Thomas G Glielmi, Austin Douglas Lee, Thomas G Glielmi"
"BIO25Q","BIO","Cystic fibrosis: from medical conundrum to precision medicine success story","3","3","2019-2020","111833","H&S
","BIO","UG","GER: DB-NatSci, WAY-SMA","Spring","Ron R Kopito"
```

Option 2: Only the Courses table depends on this information. One could modify the Courses.sql file in the Queries subdirectory to not attempt a join with ExploreCourses.

## Passwords
 No passwords are contained in the code. However, each module that needs a MySQL password knows to look for the file `$HOME/.ssh/canvas_pwd`. Placing a password into this file will cause smooth operation.

As is usual for this directory, make sure that both $HOME/.ssh, and the password file are only readable by owner.
 
All commands also support the `-p` and `-u` options that prompt for the password at runtime. 


