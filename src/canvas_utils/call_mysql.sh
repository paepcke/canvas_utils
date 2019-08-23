#!/usr/bin/env bash

# Used only internally by copy_aux_tables.py.
# Invoke a command line mysql command, given
# user, pwd, the database name, the full
# path to the mysql executable, and the
# statement.
#
# Needed b/c subprocess() had trouble calling
# MySQL directly. Should be possible, though, with
# more time investment.
#
# Returns the exit code of the mysql call.


HOST=$1
USER=$2
PWD=$3
SRC_DB=$4
MYSQL_PATH=$5
OUTFILE=$6
SELECT_STATEMENT=$7

# echo "User: $USER"
# echo "Pwd:  $PWD"
# echo "Db:   $SRC_DB"
# echo "MySQL Path: ${MYSQL_PATH}"
# echo "Mysql:$SELECT_STATEMENT"

if [[ -z $PWD ]]
then   
    ${MYSQL_PATH} -h $HOST -u $USER $SRC_DB -e " ${SELECT_STATEMENT}" > $OUTFILE
    exit $?
else
    ${MYSQL_PATH} -h $HOST -u $USER -p$PWD $SRC_DB -e " ${SELECT_STATEMENT}" > $OUTFILE
    exit $?
fi

