#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate usgs_env

usgs $CHECK_VERSION search-create $DATASET search.json \
      --noninteractive --lat-min $LAT_MIN --lat-max $LAT_MAX \
      --lon-min $LON_MIN --lon-max $LON_MAX \
      --start-date $START_DATE --end-date $END_DATE \
      --max-cloud-cover $MAX_CLOUD_COVER_PCT $ROW_FILTER $PATH_FILTER $MONTH_FILTER $NIGHT_FILTER

usgs search-run search.json > $SCENES_CSV_PATH

