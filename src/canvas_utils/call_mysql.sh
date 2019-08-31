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
ENCRYPTED_PWD=$3
SRC_DB=$4
MYSQL_PATH=$5
OUTFILE=$6
SELECT_STATEMENT=$7

# This cipher_key is shared with copy_aux_tables.py. It
# is only used to avoid having the MySQL pwd appear in
# 'ps -ef', not to otherwise keep it secret:

cipher_key="b'0pWdfacGWmnlZqNCGfMF2gD6vmI4UmhZDVBAcIkr9mU='"
PWD=$(python -c "import sys; from cryptography.fernet import Fernet; cipher=Fernet($cipher_key); print(cipher.decrypt(b'$ENCRYPTED_PWD'))")

 # echo "Host: $HOST"
 # echo "User: $USER"
 # echo "Pwd:  $PWD"
 # echo "Db:   $SRC_DB"
 # echo "MySQL Path: ${MYSQL_PATH}"
 # echo "Mysql:$SELECT_STATEMENT"

# The funky --defaults-extra-file creates an on-the-fly
# MySQL option 'file' where MySQL goes to look for the
# host/user/pwd. In a real option file MySQL would see:

#    [client]
#    host = myhost
#    user = myuser
#    password = mypasswor

if [[ -z $PWD ]]
then   
    ${MYSQL_PATH} -h $HOST -u $USER $SRC_DB -e " ${SELECT_STATEMENT}" > $OUTFILE
    exit $?
else
    ${MYSQL_PATH} --defaults-extra-file=<(printf "[client]\nhost = %s\nuser = %s\npassword = %s" "$HOST" "$USER" "$PWD")\
                  $SRC_DB -e " ${SELECT_STATEMENT}" > $OUTFILE
    exit $?
fi

