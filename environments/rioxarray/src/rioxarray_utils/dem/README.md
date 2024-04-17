
## a collection of tools for working with ASTER and Copernicus 30m DEM

Note - Copernicus DEM 30m is known to correct issues in the ASTER DEM regarding impossibly high mountians in Greenland

### example usage: Copernicus DEM for the UK

#### download tiles

```
conda activate rioxarray_env
python ~/tools/rioxarray/src/rioxarray_utils/dem/download_copernicus_dem.py --lat-min 48 --lat-max 63 --lon-min -12 --lon-max 4 --download-folder=dem_tiles
```

#### create a target grid

```
conda activate pyproj_env
python ~/tools/pyproj/src/pyproj_utils/create_latlon_grid.py dem_grid.nc --lat-min 48 --lat-max 63 --lon-min -12 --lon-max 4 --lat-step 0.0005 --lon-step 0.0005 --one-d
```

#### regrid tiles onto target grid

```
conda activate rioxarray_env
python ~/tools/xesmf/src/xesmf_utils/regrid_latlon.py dem_tiles dem_grid.nc --output-aggregation-path=dem.nc --output-aggregation-function=max --output-aggregation-chunksize=1000 
```

### example usage: Copernicus DEM for greenland

```
conda activate rioxarray_env

python ~/tools/rioxarray/src/rioxarray_utils/download_copernicus_dem.py --lat-min=58 --lat-max=83 --lon-min=-74 --lon-max=-10 --download-folder dem_tiles
```

```
conda activate pyproj_env

python ~/tools/pyproj/src/pyproj_utils/create_latlon_grid.py dem_grid.nc --lat-min=58 --lat-max=83 --lon-min=-74 --lon-max=-10 --lat-step 0.001 --lon-step 0.001 --one-d
```

```
conda activate rioxarray_env

python ~/tools/xesmf/src/xesmf_utils/regrid_latlon.py dem_tiles dem_grid.nc --output-aggregation-path=dem.nc --output-aggregation-function=max --output-aggregation-chunksize=1000 
```




