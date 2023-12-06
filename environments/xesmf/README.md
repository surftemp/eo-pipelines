# xesmf_pipeline_tools

various tools for manipulating/reprojecting EO data

## Installation

```
git clone git@github.com:surftemp/eo-pipelines.git
cd eo-pipelines/environments/xesmf
conda env create -f xesmf_env.yml
conda activate xesmf_env
pip install -e .
```


## aster_dem: create aster dem grid at 30m for a given lat/lon bounding box

```
aster_dem --skip-download --lat-min 47.089294 --lat-max 61.132767 --lon-min -15.373539 --lon-max 4.749778 --download-folder /gws/nopw/j04/eocis_chuk/CHUK_boxes/DEM/dem_2d /gws/nopw/j04/eocis_chuk/CHUK_boxes/DEM/dem30m.nc
```





