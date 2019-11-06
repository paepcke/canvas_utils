#!/usr/bin/env bash

# Used only internally by copy_aux_tables.py.  Invoke a command line
# mysql command, given user, pointer to file where pwd is stored, the
# database name, the full path to the mysql executable, and the
# statement.
#
# **** The following maybe obsolete, subject to testing:
# The TSV output is filtered as follows to make the Informatica
# tool happy:
#
#   o Escaped TAB char in columns with free text are replaced
#     by eight spaces.
#   o Line feed characters (\n) embedded in columns of text are
#     replaced with one space.
#
#
# Needed b/c subprocess() had trouble calling MySQL directly. Should
# be possible, though, with more time investment.
#
# Returns the exit code of the mysql call.


HOST=$1
USER=$2
PWD_FILE_POINTER=$3
SRC_DB=$4
MYSQL_PATH=$5
OUTFILE=$6
SELECT_STATEMENT=$7

# Is pwd file pointer absolute?
if [[ ! $PWD_FILE_POINTER == /* ]]
then
    # Nope, make it so:
    PWD_FILE_POINTER=${HOME}/.ssh/${PWD_FILE_POINTER}
fi

PWD=$(<${PWD_FILE_POINTER})

 # echo "Host: $HOST"
# echo "User: $USER"
# echo "Pwd file ptr: $PWD_FILE_POINTER"
# echo "Pwd:  $PWD"
# echo "Db:   $SRC_DB"
# echo "MySQL Path: ${MYSQL_PATH}"
# echo "Mysql:$SELECT_STATEMENT"

# The funky --defaults-extra-file creates an on-the-fly
# MySQL option 'file' where MySQL goes to look for the
# host/user/pwd. In a real option file MySQL you would see:

#    [client]
#    host = myhost
#    user = myuser
#    password = mypassword


if [[ -z $PWD ]]
then   
    ${MYSQL_PATH} -h $HOST -u $USER $SRC_DB -e " ${SELECT_STATEMENT}" > $OUTFILE
    exit $?
else
    ${MYSQL_PATH} --defaults-extra-file=<(printf "[client]\nhost = %s\nuser = %s\npassword = %s" "$HOST" "$USER" "$PWD")\
                  $SRC_DB --batch -e " ${SELECT_STATEMENT}" >> $OUTFILE
                  # $SRC_DB --batch -e " ${SELECT_STATEMENT}" > $OUTFILE | sed  "s/\\\t/        /g" | sed "s/\\\n/ /g"
    exit $?
fi

