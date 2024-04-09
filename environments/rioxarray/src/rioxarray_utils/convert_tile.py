#     convert TIF files to NetCDF
#
#     Copyright (C) 2023  EOCIS and National Centre for Earth Observation (NCEO)
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse

import rioxarray
import xarray as xr

parser = argparse.ArgumentParser()
parser.add_argument("--input-path",help="Path to input TIFF file",
                    default="/home/dev/Downloads/Copernicus_DSM_COG_10_N76_00_W055_00_DEM.tif")
parser.add_argument("--output-path",help="Path to output NetCDF file",
                    default="/home/dev/Downloads/Copernicus_DSM_COG_10_N76_00_W055_00_DEM.nc")
parser.add_argument("--variable-name",default="Copernicus_DEM",help="Name of the variable to export")
parser.add_argument("--x-dim-name",default="lon",help="Name of the x-dimension in exported file")
parser.add_argument("--y-dim-name",default="lat",help="Name of the y-dimension in exported file")

args = parser.parse_args()

raster = rioxarray.open_rasterio(args.input_path).rename({"x":args.x_dim_name,"y":args.y_dim_name})
ds = xr.Dataset()
ds[args.variable_name] = raster
ds.to_netcdf(args.output_path)
