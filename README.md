# eo-pipelines

Define and run pipelines to process Earth Observation (EO) data.

## List of CLI tools

| tool                    | description                                                                    |
|-------------------------|--------------------------------------------------------------------------------|
| create_pipelines        | create multiple pipelines given a template and csv containing parameters       |
| run_pipeline            | run a single pipeline                                                          |
| run_pipelines           | run multiple pipelines sequentially                                            |
| manage_pipelines        | monitor and launch sets of pipelines                                           |

## Installation

Installation into a miniforge enviromnent is suggested.  See [https://github.com/conda-forge/miniforge](https://github.com/conda-forge/miniforge) for installing miniforge.

Create a miniforge environment called `eo_pipelines_env` using:

```
mamba create -n eo_pipelines_env python=3.11 Mako
mamba activate eo_pipelines_env
mamba install xarray netcdf4 pyproj requests pandas pillow shapely scipy scikit-learn matplotlib rioxarray
pip install hyrrokkin[YAML]
```

## Dependencies

The following dependencies need to be installed into the eo_pipelines_env environment, see their READMEs for more information: 

* hyrrokkin - https://codeberg.org/visualtopology/hyrrokkin

## Defining Pipelines

Pipelines are defined using a YAML file, for example `test_pipeline.yaml`, that starts with a configuration section:

```yaml
configurations:
  eo_pipelines:
    # the spec describes global parameters
    properties:
      spec:
        # define the region of interest
        lat_min: "72.35.56N"
        lat_max: "64.44.52N"
        lon_min: "54.46.05W"
        lon_max: "53.49.56W"
        # and the time range
        start_date: "2021-06-01"
        end_date: "2021-08-31"
        # filter out scenes thought to have more than 10% cloud cover
        max_cloud_cover_fraction: 0.1
        # specify the datasets of interest
        datasets:
          LANDSAT_OT_C2_L1:
            bands: [ B1,B2,B3,B4,B5,B6,B7,B8,B9,B10,B11,QA_PIXEL ]
      # the environment describes how 
      environment:
        working_directory: /tmp/pipeline_test
        conda_path: /home/users/myuser/miniconda3/bin/conda
        shell: "/bin/bash"
        echo_stdout: True
        tracking_database_path: /tmp/tracking.db # optional - path to a database that is used to record task execution statistics
```

Following the configuration, each stage in the pipeline is described

```yaml
nodes:
  n0:
    # searches the region and time of interest for matching scenes
    type: eo_pipelines:usgs_search
  n1:
    # fetch the scenes from the USGS 
    type: eo_pipelines:usgs_fetch
    properties:
      configuration:
        output_path: fetched
  n2:
    # decode and import the scenes to individual netcdf4 files
    type: eo_pipelines:landsat_import
    properties:
      configuration:
        output_path: imported
      executor_settings:
          executor: local
          local:
            nr_threads: 4
```

Finally, the links between the nodes are specified to complete the pipeline's definition

```yaml
links:
  - n0 => n1
  - n1 => n2
  - n2 => n3
```

On execution the directory structure will be created:

```
/tmp/
    pipeline_test/
        n0/
            LANDSAT_OT_C2_L1_scenes.csv
        n1/
            fetched/
                LANDSAT_OT_C2_L1/
                    LC08_L1TP_005003_20140605_20200911_02_T1_B2.TIF   
                    LC08_L1TP_005003_20160610_20200906_02_T1_B4.TIF   
                    LC08_L1TP_005003_20190603_20200828_02_T1_B6.TIF   
                    ...
        n2/
            imported/
                LANDSAT_OT_C2_L1/
                    20140605143221-NCEO-L1C-Landsat8-v2.0-fv01.0.nc
                    20150608143155-NCEO-L1C-Landsat8-v2.0-fv01.0.nc 
                    ...
```

## Configurations and Executor settings

Each node is defined by a `properties` section which is divided into `configuration` and `executor_settings`

* `properties` -> `configuration`

In this section, parameters that are passed to the node are defined.  In the example above, nodes `n1` and `n2` are both configured with an `output_path`
parameter which controls where the node's output files are written.

* `properties` -> `executor_settings`

Most nodes execute themselves by launching one or more tasks, which can be run on the local machine, or as SLURM jobs.  The `executor_settings` configuration determines how tasks are executed

| executor_setting                                  | purpose                                                                                                                                                       |
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **executor**                                      | currently, should be set to "local", meaning that all tasks run as sub-processes on the local machine                                                         |
| **can_skip**                                      | set to "true" or "false" (the default).  Allows the executor to skip execution of the node if a results.json file has been generated by a previous execution. |
| **concurrent_execution_limit -> limit**           | limit the number of executions that can happen, systemwide                                                                                                    |
| **concurrent_execution_limit -> tracking_folder** | the shared folder used to implement the concurrent execution limit                                                                                            |
| **retry_count**                                   | the number of times a failed task should be retried                                                                                                           |
|  **local -> nr_threads**                          | the number of threads/sub-processes to launch in parallel, when executing tasks locally                                                                       | 


## Available stages

Pipleline stages currently supported:

| stage_name           | purpose                                                                                     | required conda environment |
|----------------------|---------------------------------------------------------------------------------------------|----------------------------|
| **usgs_search**      | find landsat scenes using the usgs tool                                                     | usgs_env                   |
| **usgs_fetch**       | download landsat scenes using the usgs_download tool                                        | usgs_env                   |
| **landsat_import**   | convert landsat scenes to netcdf4                                                           | landsat_importer_env       |
| **xesmf_regrid**     | regrid scenes onto a target grid using XESMF                                                | xarray_regridder_env       |
| **regrid**           | regrid scenes onto a target grid using xarray_regridder                                     | xarray_regridder_env       |  
| **group**            | combine/stack scenes from the same orbit (also across different datasets) after regridding  | eo_pipelines_env           | 
| **metadata_tweaker** | output a netcdf4 with adjusted metadata for each input file                                 | eo_pipelines_env           |
| **time_stacker**     | combine a number of separate netcdf4 files into one file by appending along the time access | eo_pipelines_env           | 
| **add_masks**        | add mask layers defined by geojson files                                                    | eo_pipelines_env           |
| **add_spatial**      | add spatial layers defined by netcdf4 files                                                 | eo_pipelines_env           |
| **generate_html**    | create an interactive static HTML website for exploring netcdf files                        | netcdf_explorer_env        |
| **rubber_stamp**     | write the pipeline YAML definition and path into the data files                             | eo_pipelines_env           | 
| **apply_fmask**      | annotate raw landsat scenes with the fmask 5.0.1 cloud mask                                 | fmask                      |
| **create_grid**      | create a spatial grid - currently lat-lon only - that can be input to the **regrid** stage  | eo_pipelines_env           | 

## Required conda environments

Each of the above stages requires a conda environment and specific tools installed into it.  If the required conda environment is `eo_pipelines_env` these tools are already installed, otherwise for details on how to create these see:

### usgs_env 

See https://github.com/surftemp/usgs

Environment variables USGS_USERNAME and USGS_PASSWORD should be defined

### landsat_importer_env 

See https://github.com/surftemp/landsat_importer

### xarray_regridder_env

https://github.com/surftemp/xarray_regridder

### netcdf_explorer_env

https://github.com/surftemp/netcdf_explorer

### fmask

See https://github.com/GERSL/Fmask

## Running pipelines

See the `example_pipelines` folder for examples of how to define pipelines

To run a single pipeline:

```
conda activate eo_pipelines_env
nohup run_pipeline <path-to-pipeline-yaml-file> &
```

## Running multiple pipelines

To run a set of pipelines (pipelines are executed in series):

```
nohup run_pipelines "**/*.yaml" &
```

## Creating a set of pipelines from a template

A `create_pipelines` tool is provided for preparing a set of pipelines from a template and a CSV file containing parameter values that will be substituted into the template

```
create_pipelines pipeline_template.yaml parameters.csv /tmp/pipelines
```

where pipeline_template.yaml is a pipeline with template parameters surrounded by dollar+curly brackets

```
nodes:
...
  n0:
    type: eo_pipelines:usgs_search
    properties:
      executor_settings:
        can_skip: true
      configuration:
        row: ${row}
        path: ${path}
        months: [6]
...
```

and parameters.csv contains values to substitute for each parameter:

```csv
row,path
23,202
22,207
26,201
18,209
```

The templating engine used is Mako (for more information see https://docs.makotemplates.org/en/latest/ and https://devhints.io/mako)

running the tool will create the following directory structure

```filesystem
/tmp/
    pipelines/
        pipeline0/
            pipeline.yaml
        pipeline1/
            pipeline.yaml
```

## Reproducibility

Ideally, it would be possible to know which software was used to run a pipeline.  eo_pipelines adopts the following strategy:

* strict versioning of the eo_pipelines package, individual pipeline stages and external tools used to run stages
* tag source code in git with the versions
* embed a required eo_pipelines version string in the pipeline
* cross-check version numbers between pipeline stages and external tools
* use the rubber_stamp stage (see above) to embed the pipeline string into data products

### eo_pipelines versioning

The eo_pipelines version string is specified in `eo_pipelines.VERSION` and this defines the version number in the eo_pipelines python package.
 
Non-trivial updates to the eo_pipelines package require an update to the version number

### version format

A `<major>.<minor>.<release>` format is suggested for all version strings used

### embedding the eo_pipelines version in a pipeline YAML

A pipeline can specify that a particular version of the eo_pipelines package is used to run the pipeline.

Add `require_version` to the eo_pipelines configuration in the pipeline YAML file:

```yaml
configurations:
  eo_pipelines:
    # the spec describes global parameters
    properties:
      require_version: "0.0.5"
```

### per-stage version numbers

Each pipeline stage is assigned its own version string.  Whenever one of these is increased, the eo_pipelines version should also be increased.

For an example of the pipeline stage versioning:

[src/eo_pipelines/stages/nodes/usgs_fetch.py](src/eo_pipelines/stages/nodes/usgs_fetch.py)

```python

...
class USGS_Fetch(PipelineStage):

    VERSION = "0.4.0"

    ...
```

Non-trivial updates to a pipeline stage should trigger version incremements for both the stage and the eo_pipelines package.

### external tool version numbers

Some pipeline stages call out to external tools rather than executing code within the package.  The version of the tool is by default checked against the version of the stage.

To suppress this version checking, add `check_tool_version: false` to the stage configuration.

The following stages use external tools:

* usgs_search
* usgs_fetch
* landsat_import
* regrid
* xesmf_regrid

The external tools support a `--check-version <VERSION>` command line option

Versions of the stages and tools should be kept in sync

### tagging source code in git

For code that is stored in a git repo, for each version number update to a tool or the eo_pipelines package, the code should be tagged with that version

```
git tag -a X.Y.Z
git show X.Y.Z
git push origin X.Y.Z
```

