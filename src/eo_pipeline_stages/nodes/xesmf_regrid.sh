#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate xesmf_env

regrid $INPUT_PATH $GRID_FILE $OUTPUT_FOLDER --cache $CACHE_FOLDER --max-distance $MAX_DISTANCE