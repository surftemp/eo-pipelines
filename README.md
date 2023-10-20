# eo-pipeline-stages

Stage implementation using the eo-pipelines API

## Dependencies

Requires the hyrrokkin library - https://github.com/visualtopology/hyrrokkin

## available stages

Pipleline stages currently supported:

stage_name | purpose | required conda environment
-----------|-------- | --------------------------
**eo_pipelines:usgs_search** | find scenes using the usgs tool | usgs_env
**eo_pipelines:usgs_fetch** | download scenes using the usgs_download tool | usgs_env
**eo_pipelines:usgs_import** | convert scenes to netcdf4 | rioxarray_env
**eo_pipelines:usgs_group** | combine/stack groups of regridded scenes from the same orbit (also across different datasets) | landsat2nc_env

## Required conda environments

Each of the above stages requires a conda environment and specific tools installed into it.

### usgs_env 

See https://github.com/surftemp/usgs

### rioxarray_env

See https://github.com/surftemp/landsat_importer

### landsat2nc_env

See https://github.com/surftemp/landsat2nc

