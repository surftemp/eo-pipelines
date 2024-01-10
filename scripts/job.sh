#!/bin/bash

#SBATCH --partition=par-single
#SBATCH --job-name=pipeline0
#SBATCH -o pipeline0.out
#SBATCH -e pipeline0.err
#SBATCH -t 48:00:00
#SBATCH --mem=32G
#SBATCH --ntasks=4

conda activate eo_pipelines_env

# add these if using the fetch stage in the pipeline
export USGS_USERNAME=
export USGS_PASSWORD=

run_pipeline pipeline.yaml
