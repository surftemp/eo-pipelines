#!/bin/bash

# copy relevant files to JASMIN

rootfolder=`dirname $0`/..
username=$1
destfolder=$2

if [ -z ${username} ] || [ -z ${destfolder} ];
then
  echo provide the username and destination folder on JASMIN as arguments
else
  rsync -avr $rootfolder/src $username@login2.jasmin.ac.uk:$destfolder
  rsync -avr $rootfolder/pyproject.toml $username@login2.jasmin.ac.uk:$destfolder
  rsync -avr $rootfolder/setup.cfg $username@login2.jasmin.ac.uk:$destfolder
  rsync -avr $rootfolder/cartopy_env.yml $username@login2.jasmin.ac.uk:$destfolder
fi