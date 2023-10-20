#!/bin/bash

rootfolder=`dirname $0`/..

rsync -avr $rootfolder/src niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/eo-pipeline-stages
rsync -avr $rootfolder/example_pipelines niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/eo-pipeline-stages
rsync -avr $rootfolder/pyproject.toml niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/eo-pipeline-stages
rsync -avr $rootfolder/setup.cfg niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/eo-pipeline-stages

hyrrokkinfolder=$rootfolder/../hyrrokkin

rsync -avr $hyrrokkinfolder/src niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/hyrrokkin
rsync -avr $hyrrokkinfolder/pyproject.toml niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/hyrrokkin
rsync -avr $hyrrokkinfolder/setup.cfg niallmcc@login2.jasmin.ac.uk:/home/users/niallmcc/github/hyrrokkin



