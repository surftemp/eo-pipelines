#!/usr/bin/env python

#     assemble DEM tiles
#
#     Copyright (C) 2023-2024 National Centre for Earth Observation (NCEO)
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
import xarray as xr

spatial_ref = None
crs = None

def glue_dem(variable_name, min_lat, max_lat, min_lon, max_lon, input_folder, output_path):

    lat_cache = {}
    lon_cache = {}

    def preprocessor(ds):
        # DEM tiles seem to overlap by 1 pixel in both dimensions
        # remove last latitude and longitude pixels from each tile to ensure they don't overlap
        # because this causes a problem in the merged DEM dataset
        nlat = len(ds.lat)
        nlon = len(ds.lon)
        ds2 = ds.isel(lat=range(0, nlat - 1), lon=range(0, nlon - 1))

        # get the min lat/lon of the tile
        lat_min = int(ds2.lat.min())
        lon_min = int(ds2.lon.min())

        # standardise lats/lons as there are occasionally small differences at about e-14
        # that will cause problems with dask

        if lat_min in lat_cache:
            ds2["lat"] = lat_cache[lat_min]
        else:
            lat_cache[lat_min] = ds2["lat"]

        if lon_min in lon_cache:
            ds2["lon"] = lon_cache[lon_min]
        else:
            lon_cache[lon_min] = ds2["lon"]

        global spatial_ref
        if spatial_ref is None:
            spatial_ref = ds2["spatial_ref"]
        ds2 = ds2.drop_vars("spatial_ref")

        global crs
        if crs is None:
            crs = ds2["crs"]
        ds2 = ds2.drop_vars("crs")

        ds2.set_coords(["lat","lon"])

        return ds2

    dem_path = input_folder + "/*.nc"

    ds = xr.open_mfdataset(dem_path, preprocess=preprocessor)
    if min_lat is not None and max_lat is not None and min_lon is not None and max_lon is not None:
        ds = ds.sel(lat=slice(min_lat,max_lat),lon=slice(min_lon,max_lon))
    global spatial_ref
    if spatial_ref is not None:
        ds["spatial_ref"] = spatial_ref

    global crs
    if crs is not None:
        ds["crs"] = crs

    ds.to_netcdf(output_path, encoding={
        variable_name: {
            "zlib": True, "complevel": 5, "chunksizes": [1800, 1800], "dtype": "int16"
        }
    })

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output_path", help="path to write output file")
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", default=None)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", default=None)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", default=None)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", default=None)
    parser.add_argument("--input-folder", help="folder containing dem tiles", required=True)
    parser.add_argument("--variable-name", help="the variable name", default="Copernicus_DEM")

    args = parser.parse_args()

    glue_dem(args.variable_name,args.lat_min,args.lat_max,args.lon_min,args.lon_max,args.input_folder,args.output_path)

if __name__ == '__main__':
    main()

