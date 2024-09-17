#!/bin/bash

# copy relevant files to a remote server

rootfolder=`dirname $0`/..
hostname=$1
username=$2
destfolder=$3

if [ -z ${hostname} ] || [ -z ${username} ] || [ -z ${destfolder} ];
then
  echo provide the hostname, username and destination folder as arguments
else
  rsync -avr $rootfolder/src $username@$hostname:$destfolder/eo-pipelines
  rsync -avr $rootfolder/example_pipelines $username@$hostname:$destfolder/eo-pipelines
  rsync -avr $rootfolder/pyproject.toml $username@$hostname:$destfolder/eo-pipelines
  rsync -avr $rootfolder/setup.cfg $username@$hostname:$destfolder/eo-pipelines
  rsync -avr $rootfolder/MANIFEST.in $username@$hostname:$destfolder/eo-pipelines

  for environment in cartopy pyproj xesmf rioxarray
  do
      echo syncing $environment
      rsync -avr $rootfolder/environments/$environment $username@$hostname:$destfolder
  done
fi




