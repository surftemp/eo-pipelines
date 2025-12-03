#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate usgs_env

usgs_download -f $SCENES_CSV_PATH \
  -o $OUTPUT_FOLDER \
  -d $DOWNLOAD_FOLDER \
  -s $SUFFIXES \
  $EXCLUDE_SUFFIXES \
  $FILE_CACHE_INDEX \
  $LIMIT \
  $NO_DOWNLOAD \
  -e $DOWNLOAD_SUMMARY_PATH $CHECK_VERSION
