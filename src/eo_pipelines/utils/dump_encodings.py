import netCDF4 as nc4
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_path", type=str, help="path to netcdf4 file")

args = parser.parse_args()

import xarray as xr

ds = xr.open_dataset(args.input_path)
for v in ds.variables:
    print(v, ds[v].encoding)
