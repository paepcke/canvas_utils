#<p align="center">Canvas Analysis Data Extraction</p>


Utilities for mining Canvas data. Creates a set of 'auxiliary' tables from exports of Canvas data. The generated tables pull from the exports data that are likely to be useful for analysis. New auxiliary tables can easily be added. The package can perform the following functions: 

- Create the auxiliary tables
- Backup/restore the auxiliary tables
- Export the auxiliary tables' schemas as .sql files, and their contents
  to .csv files in a specified directory

In the following we call the MySQL database holding the full export `Export`, and the database holding the new auxiliary tables `Auxiliaries.` The following functions are performed when the generation of the `Auxiliaries` tables is requested:

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

Check file `setupSample.cfg` in the root directory. If any changes are needed, copy the cile to `setup.cfg`, and modify options there.

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

## Passwords
 No passwords are contained in the code. However, each module that needs a MySQL password knows to look for the file `$HOME/.ssh/canvas_pwd`.
 
All commands also support the `-p` and `-u` options that prompt for the password at runtime. 


