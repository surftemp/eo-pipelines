#!/bin/bash

# copy relevant files to JASMIN

rootfolder=`dirname $0`/..
username=$1
destfolder=$2

if [ -z ${username} ] || [ -z ${destfolder} ];
then
  echo provide the username and destination folder on JASMIN as arguments
else
  rsync -avr --delete --exclude "*/__pycache__" $rootfolder/src $username@login2.jasmin.ac.uk:$destfolder/eo-pipelines
  rsync -avr --delete $rootfolder/example_pipelines $username@login2.jasmin.ac.uk:$destfolder/eo-pipelines
  rsync -avr $rootfolder/pyproject.toml $username@login2.jasmin.ac.uk:$destfolder/eo-pipelines
  rsync -avr $rootfolder/setup.cfg $username@login2.jasmin.ac.uk:$destfolder/eo-pipelines
  rsync -avr $rootfolder/MANIFEST.in $username@login2.jasmin.ac.uk:$destfolder/eo-pipelines

  for environment in cartopy pyproj xesmf rioxarray
  do
      echo syncing $environment
      rsync -avr --delete --exclude "*/__pycache__" $rootfolder/environments/$environment $username@login2.jasmin.ac.uk:$destfolder
  done
fi




