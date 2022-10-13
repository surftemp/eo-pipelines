# eo-pipeline-stages

Stage implementation using the eo-pipelines API

## Dependencies

Requires the eo_pipelines library - https://github.com/surftemp/eo-pipelines

## available stages

Pipleline stages currently supported:

stage_name | purpose
-----------|--------
**eo_pipeline_stages.fetch.Fetch** | find and download scenes using the usgs tool
**eo_pipeline_stages.regrid.Regrid** | regrid scenes onto a regular latlon grid and export to netcdf4 
**eo_pipeline_stages.group.Group** | combine/stack groups of regridded scenes from the same orbit (also across different datasets)
**eo_pipeline_stages.time_stacker.TimeStacker** | Stack scenes along the time dimension
**eo_pipeline_stages.slider_tool.SliderTool** | plot scenes to html using slider tool

* for **fetch_scenes**, you'll need usgs installed in a conda environment called usgs_env.  See https://github.com/surftemp/usgs

* for **regrid_scenes** and **group_scenes**, you'll need landsat2nc installed in a conda environment called landsat2nc_env.  See https://github.com/surftemp/landsat2nc

* for **slider_tool**, you'll need slider_tool installed in a conda environment called slider_tool_env.  See https://github.com/surftemp/slider_tool

## supported combinations of stages

Currently this is quite restricted to sequences:

`fetch [ -> regrid [ -> group [ -> slider_tool ] ] ]`
