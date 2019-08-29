# <p align="center">Canvas Analysis Data Extraction</p>


Utilities for mining Canvas data. Creates a set of 'auxiliary' tables from exports of Canvas data. The generated tables data likely to be useful for analysis. New auxiliary tables can easily be added. The package can perform the following functions: 

- Create the auxiliary tables
- Backup/restore the auxiliary tables
- Export the auxiliary tables' schemas as .csv files

In the following we call the MySQL database holding the new auxiliary tables `Auxiliaries.` The following functions are performed when the generation of the `Auxiliaries` tables is requested by running `canvas_prep.py`:

- Backup the existing `Auxiliaries` tables
- Create the tables in `Auxiliaries`
- Ensure that only up to two `Auxiliaries` backups exist to avoid
  excessive disk usage.

The program responsible for table creation is `canvas_prep.py`. Like all other commands, `canvas_prep.py` can be called with a `-h` or `--help` option.

A summary of the available commands. Only the first three are typically in general use:

- **refresh_history.py**: list auxiliary tables that have already been created, and the tables that are still missing.
- **canvas_prep.py**: request that some, or all aux tables be built.
- **copy_aux_tables.py**: export the aux tables to csv.
- **refresh_history.py**: list the available auxiliary tables, and the still missing tables.
- restore_tables.py: replace an aux table with the latest of its backups.
- clear_old_backups.py: remove all but a specified number of backups. Called automatically. But if errors interrupt runs, this script may be called with the number of maximum backup tables as command line parameter.


Example for creating the tables in `Auxiliaries`:
```
# Create all `Auxiliaries`, backing up the existing tables first:
src/canva_utils/canvas_prep.py

# Create only the CourseEnrollments and AssignmentSubmissions tables:
src/canva_utils/canvas_prep.py --table CourseEnrollments AssignmentSubmissions
```

Example for exporting the tables in `Auxiliaries` to .csv with
contents, and .sql for the schema:
```
# Generate one .csv file and one .sql file for every auxiliary table
# in the default directory /tmp. The .sql file holds the table's
# sql schema:
src/canvas_utils/copy_aux_tables.py

# Export only the CourseEnrollments and AssignmentSubmissions tables
# into /my/own/directory:
src/canvas_utils/copy_aux_tables.py --table CourseEnrollments AssignmentSubmissions --destdir /my/own/directory
```

The schema files contain SQL `CREATE TABLE` statements. The .csv files will contain a header line with with column names, followed by the data. All values will be double-quoted.

## Installation

Known unpleasantness: Ensure that mysql_config is installed. It is included in the MySQL server installation. Right now this dependency forces the server installation, even though the server is not used. 

It is generally recommended to use a Python virtual environment, such as [Anaconda](https://www.anaconda.com/distribution/), [Virtualenv](https://virtualenv.pypa.io/en/latest/), or Python 3's [venv](https://docs.python.org/3/library/venv.html). These ensure that no root access is needed during installation:

`pip install canvas_utils`

or clone the repo:

`git clone https://github.com/paepcke/canvas_utils.git`

Switch to the project root directory, create a virtual environment if desired, and run:

`python setup.py install`

to install the needed Python packages.

Check file `setupSample.cfg` in the root directory. If any changes are needed, copy the file to `setup.cfg`, and modify options there. Note the following entries:

- default_user--ensure that this entry's value is a known user in you MySQL installation.
- canvas_auxiliary_db_name--create this database (a.k.a. MySQL schema). Grant at least CREATE, DROP, INSERT, SELECT on this database to default_user.
- raw_data_db--ensure this is where your Canvas raw downloads reside. Grant at least SELECT on this database for default_user.
- canvas_pwd_file--[see Password section](#passwords)

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

The file `setupSample.cfg` in the project root directory enables settings for a number of defaults, including login names, database names, and host names.

To make changes in this file, copy it to `setup.cfg` in the root directory. Then follow instructions in the file. This modified copy will be preserved during code updates.

## Addition of new auxiliary tables

Every auxiliary table is defined in a separate .sql file in subdirectory `Queries`. Each file name (without the extension) is the name of one auxiliary table.

To add an additional table, create a file in the `Queries` directory. Make the file name the name of the table, with extension .sql. The `canvas_prep.py` command will do the rest.

Similarly, the existing files may be modified to taste.

### Implementation Note
The table .sql files contain table creation statements. Those statements reference the database (a.k.a. MySQL schema) where the `Auxiliaries` are to be constructed. They also reference the database where the full Canvas exports reside. These names are hard coded into the .sql files.

The names are automatically replaced in memory at runtime if `setup.cfg` alters these names via the

- canvas_auxiliary_db_name and
- raw_data_db

entries. One can use the script convert_queries.py for modifying the .sql files in the `Queries` directory to reflect the local names, instead of the hardcoded `canvasdata_aux` and `canvasdata_prd`. But the .sql files will be overwritten with updates. Best practice: use those two names. Second-best practice: indicate the different names in `setup.cfg`, and rely on `canvas_prep.py` to do the substitutions on the fly.

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

As is usual for this directory, make sure that both $HOME/.ssh, and the password file are only readable by owner. The filename may be changed in `setup.cfg`.
 
All commands also support the `-p` and `-u` options that prompt for the password at runtime. 


