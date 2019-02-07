#!/usr/bin/env bash

# One-time script to create the CREATE TABLE statements
# for all tables we need from canvasdata_prd. Likely
# only needed for creating test setups.

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
    sql_str=$(mysql --login-path=paepcke canvasdata_prd --silent --skip-column-names -e "SHOW CREATE TABLE $tbl_nm;\G")
    # Remove the leading table name:
    create_str=$(echo ${sql_str} | sed -n 's/\([^ ]*[ ]*\)\(.*\)/\2\;/p')
    echo ${create_str}
done
