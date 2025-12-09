#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate fmask

cd $FMASK_HOME/main

python fmask.py --imagepath $SCENEPATH --model $MODEL
