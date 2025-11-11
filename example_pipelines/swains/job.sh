#!/bin/bash
#SBATCH --job-name=swains
#SBATCH -o pipeline.out
#SBATCH -e pipeline.err
#SBATCH -t 48:00:00
#SBATCH --mem=128G
#SBATCH --account=lake_icesheet
#SBATCH --partition=standard
#SBATCH --qos=long

# this script can be used to submit a pipeline group as a SLURM job

conda activate eo_pipelines_env

export USGS_USERNAME=
export USGS_TOKEN=

run_pipeline swains.yaml 
