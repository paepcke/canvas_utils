#!/usr/bin/env bash

# Usually called from cron. 

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# Go to project root dir:
cd $SCRIPT_DIR/..

# Create the ~/cronlogs dir if needed:
if [[ ! -e $HOME/cronlogs ]]
then
    mkdir $HOME/cronlogs
fi

# Activate the proper anaconda environment; if that
# succeeds, run canvas_prep.py, creating a new log
# file for its output:

$HOME/anaconda3/bin/activate canvas_utils && \
$HOME/anaconda3/envs/canvas_utils/bin/python src/canvas_utils/canvas_prep.py > \
$HOME/cronlogs/cron_aux_refresh_$(/bin/date +%d-%m-%Y).log 2>&1
