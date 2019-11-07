#!/usr/bin/env bash

# Usually called from cron. 

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PICKUP_DIR=/dmr_shared/vptl/data

# Create the ~/cronlogs dir if needed:
if [[ ! -e $HOME/cronlogs ]]
then
    mkdir $HOME/cronlogs
fi

LOG_PATH=$HOME/cronlogs/cron_aux_refresh_$(/bin/date +%d-%m-%Y).log

# Run the canvas raw data refresh into canvasdata_prd.  If that
# succeeds, activate the proper anaconda environment; if that
# succeeds, run canvas_prep.py, creating a new log file for its
# output:


# <Karen's invocation of raw data refresh goes here> && \
# To the canvas_utils project root:
$HOME/Code/canvasdata/run/import_canvas_data.sh && \
cd $SCRIPT_DIR/.. && \
$HOME/anaconda3/bin/activate canvas_utils && \
    $HOME/anaconda3/envs/canvas_utils/bin/python src/canvas_utils/canvas_prep.py > $LOG_PATH 2>&1 && \
    $HOME/anaconda3/envs/canvas_utils/bin/python src/canvas_utils/copy_aux_tables.py \
                                                 --destdir ${PICKUP_DIR} >> $LOG_PATH 2>&1 && \
    $HOME/anaconda3/envs/canvas_utils/bin/python src/canvas_utils/final_sanity_check.py >> $LOG_PATH 2>&1


        

    
