#!/usr/bin/env python
# -*- coding: utf-8 -*-

#     download Copernicus DEM tiles
#
#     Copyright (C) 2024 National Centre for Earth Observation (NCEO)
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
import os
import math
import xarray as xr
import rioxarray

script_path = os.path.join(os.path.split(__file__)[0],"download_copernicus_dem.sh")

def tif_to_netcdf(input_path, output_path):
    raster = rioxarray.open_rasterio(input_path).rename({"x": "lon", "y": "lat"})
    ds = xr.Dataset()
    ds["Copernicus_DEM"] = raster.squeeze()
    del ds["band"]
    ds.to_netcdf(output_path,
        encoding={"Copernicus_DEM": {"dtype": "short", "zlib": True, "complevel": 5, "_FillValue": "-9999"}})

def fetch_dem(min_lat, max_lat, min_lon, max_lon, folder):

    def get_tile_template(lat, lon):
        lats = "N%02d" % int(lat) if lat > 0 else "S%02d" % int(-lat)
        lons = "W%03d" % int(-lon) if lon < 0 else "E%03d" % int(lon)
        return (lats,lons)

    latmin = min_lat
    latmax = max_lat
    lonmin = min_lon
    lonmax = max_lon

    latimin = int(latmin)
    latimax = math.ceil(latmax)
    lonimin = int(lonmin) - 1
    lonimax = math.ceil(lonmax)

    for y in range(latimin, latimax + 1):
        for x in range(lonimin, lonimax + 1):
            lats,lons = get_tile_template(y, x)
            expected_filename = f"Copernicus_DSM_10_{lats}_00_{lons}_00_DEM.tif"
            expected_path = os.path.join(folder, expected_filename)
            if os.path.exists(expected_path):
                print(f"{expected_filename} already downloaded, skipping")
            else:
                print(f"attempted to download {expected_filename}")
                os.system(script_path+" "+lats+" "+lons+" "+folder)

            output_filename = os.path.splitext(expected_filename)[0] + ".nc"
            output_path = os.path.join(folder, output_filename)
            if os.path.exists(expected_path) and not os.path.exists(output_path):
                print(f"converting {expected_filename} to netcdf")
                output_filename = os.path.splitext(expected_filename)[0]+".nc"
                output_path = os.path.join(folder,output_filename)
                tif_to_netcdf(expected_path, output_path)

            if not os.path.exists(output_path):
                print(f"warning: {output_path} was not created")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", required=True)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", required=True)
    parser.add_argument("--download-folder", help="folder for storing downloaded dem tiles", required=True)

    args = parser.parse_args()
    os.makedirs(args.download_folder, exist_ok=True)
    fetch_dem(args.lat_min,args.lat_max,args.lon_min,args.lon_max,args.download_folder)

if __name__ == '__main__':
    main()

