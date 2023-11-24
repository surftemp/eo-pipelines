#!/bin/bash

# copy relevant environments to JASMIN

rootfolder=`dirname $0`
username=$1
destfolder=$2

if [ -z ${username} ] || [ -z ${destfolder} ];
then
  echo provide the username and destination folder on JASMIN as arguments
else
  for environment in cartopy pyproj rioxarray xesmf
  do
      echo syncing $environment
      rsync -avr --delete --exclude "*/__pycache__" $rootfolder/$environment $username@login2.jasmin.ac.uk:$destfolder
  done
fi