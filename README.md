Utilities for mining Canvas data. The main entry point is
canvas_prep.py. The -h or --help switch provides argument info.

The main function is to log into the AWS RDS database that houses
Canvas data. Once logged in, new tables are computed from the base
tables that are provided by the Canvas company. This Python module can
run anywhere, but is assumed to run in an AWS compute server.

The raw production tables are assumed to be in db canvasdata_prd.
The newly computed tables will be placed in canvasdata_aux.

Login credentials for the MySQL db may be provided on the command
line. Else the user name will be 'canvasdata_prd' and the password is
assumed to be in the invoking user's $HOME/.ssh/canvas_pwd. Permission
precautions typical for the .ssh directories are assumed.

The main commandline options allow for:

1 creating either just a small number of individual tables, or all of
  them.

2.and controlling whether tables should be computed even if they exist
  in canvasdata_aux. Or only to compute missing tables.

Item 2 allows for picking up computations if a table creation fails
midstream.

The architecture makes is easy to add new computational tables to be
computed from base tables. Additions are particularly easy if
computations only involve SQL. In that case:

1. Place a file with the sql queries into src/canva_utils/Queries
   Name the file <tableName>.sql
2. Enter the table name into the list near the top of canvas_prep.py.

A loop will pick the table name up from the list, and execute the sql
in the file.

The by far longest-running computations are for DiscussionMessages and
AssignmentSubmissions.


