#!/usr/bin/env bash

# One-time script to copy the contents of some canvasdata_prd
# from one db to another. Receiving db is assumed to have
# empty tables created beforehand. This can be done using
# script prep_canvasdata_prd.sh
#
# The db canvasdata_prd is expected to exist at the destination.
# Likely only needed for creating test setups.

declare -a table_names

table_names=(
               account_dim \
               assignment_dim \
               assignment_fact \
               assignment_group_dim \
               assignment_group_fact \
               assignment_group_score_fact \
               course_dim \
               courseassignments \
               discussion_entry_dim \
               discussion_entry_fact \
               discussion_topic_dim \
               discussion_topic_fact \
               enrollment_dim \
               enrollment_term_dim \
               quiz_dim \
               quiz_fact \
               role_dim \
               submission_dim \
               submission_fact \
               user_dim
)

for tbl_nm in "${table_names[@]}"
do
    # Create a local csv file from the table:
    echo "Starting on table ${tbl_nm}..."
    
    csv_file=/private/tmp/${tbl_nm}.csv
    if [[ -f ${csv_file} ]]
    then
        echo "    Removing existing file ${csv_file} ..."
        rm ${csv_file}
    fi
    
    echo "    Exporting to file ${tbl_nm}.csv ..."
    mysql --login-path=paepcke canvasdata_prd -e \
          "SELECT * INTO OUTFILE '${csv_file}' \
              FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n'\
              FROM ${tbl_nm};"
    if [[ $? != 0 ]]
    then
        echo "    ***** Could not export table ${tbl_nm}"
        exit 1
    fi

    echo "    Done exporting to file ${tbl_nm}.csv"
    
    echo "    Ingesting to ${tbl_nm}.csv at remote db..."
    # Read the file into the remote db:
    mysql --login-path=aux canvasdata_prd -e \
          "LOAD DATA LOCAL INFILE '${csv_file}' \
            INTO TABLE ${tbl_nm} \
           FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n';"
    if [[ $? != 0 ]]
    then
        echo "    ***** Could not ingest table ${tbl_nm}"
        exit -1
    fi
    echo "Done ingesting to ${tbl_nm}.csv at remote db."
done
