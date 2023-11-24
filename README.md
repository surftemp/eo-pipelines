# eo-pipeline-stages

Stage implementation using the eo-pipelines API

## Installation

create a conda environment called `eo_pipelines_env` using:

```
conda env create -f eo_pipelines_env.yml
```

## Dependencies

Requires the hyrrokkin and pyjob libraries

- https://github.com/surftemp/pyjob
- https://github.com/visualtopology/hyrrokkin

## available stages

Pipleline stages currently supported:

stage_name | purpose                                                                                   | required conda environment
-----------|-------------------------------------------------------------------------------------------| --------------------------
**usgs_search** | find landsat scenes using the usgs tool                                                   | usgs_env
**usgs_fetch** | download landsat scenes using the usgs_download tool                                      | usgs_env
**usgs_import** | convert landsat scenes to netcdf4                                                         | rioxarray_env
**usgs_group** | combine/stack groups of landsat scenes from the same orbit (also across different datasets) | pyproj_env
**xesmf_regrid** | regrid scenes onto a target grid using XESMF                                              | pyproj_env

## Required conda environments

Each of the above stages requires a conda environment and specific tools installed into it.  For details on how to create these see:

### usgs_env 

See https://github.com/surftemp/usgs

### rioxarray_env

See https://github.com/surftemp/eo-pipelines/tree/main/environments/rioxarray

### cartopy_env

See https://github.com/surftemp/eo-pipelines/tree/main/environments/cartopy

### pyproj_env

https://github.com/surftemp/eo-pipelines/tree/main/environments/pyproj_env

### xesmf_env

https://github.com/surftemp/eo-pipelines/tree/main/environments/xesmf

## Running pipelines

See the `example_pipelines` folder for examples of how to define pipelines

To run a single pipeline:

```
conda activate eo_pipelines_env
nohup run_pipeline <path-to-pipeline-yaml-file> &
```

To run a set of pipelines (pipelines are executed in series):

```
nohup run_pipelines "**/*.yaml" &
```