# landsat_importer

import landsat 8/9 scenes to netcdf4 

## Installation

Set up an anaconda environment

```
conda env create -f environment.yml
conda activate rioxarray_env
```

Clone this repo and install it

```
git clone git@github.com:surftemp/landsat_importer.git
cd landsat_importer
pip install .
```

## Usage

Convert scene, all bands

```
run_landsat_importer <path_to_landsat_scene> <output_netcdf4_path>
```



