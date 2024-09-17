
import xarray as xr
import numpy as np
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("output_path",help="path to write output file")
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", required=True)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", required=True)
    parser.add_argument("--lat-step", type=float, help="latitude step, decimal degrees", required=True)
    parser.add_argument("--lon-step", type=float, help="longitude step, decimal degrees", required=True)
    parser.add_argument("--lat-variable-name", help="latitude variable name", default="lat")
    parser.add_argument("--lon-variable-name", help="longitude variable name", default="lon")
    parser.add_argument("--y-dim", help="y dimension name", default="lat")
    parser.add_argument("--x-dim", help="x dimension name", default="lon")
    parser.add_argument("--one-d", action="store_true", help="write 1d lat lon arrays")

    args = parser.parse_args()

    global_attributes = {
        "geospatial_lat_min": f"{args.lat_min}",
        "geospatial_lat_max": f"{args.lat_max}",
        "geospatial_lon_min": f"{args.lon_min}",
        "geospatial_lon_max": f"{args.lon_max}",
        "geospatial_lat_units": "degrees_north",
        "geospatial_lat_resolution": f"{args.lat_step}",
        "geospatial_lon_units": "degrees_east",
        "geospatial_lon_resolution": f"{args.lon_step}"
    }

    target_ds = xr.Dataset(attrs=global_attributes)
    lats = np.flip(np.arange(args.lat_min, args.lat_max, args.lat_step))
    lons = np.arange(args.lon_min, args.lon_max, args.lon_step)

    shape = len(lats), len(lons)



    lon_attributes = {
        "long_name":"longitude",
		"standard_name":"longitude",
		"units": "degrees_east",
		"valid_min": f"{args.lon_min}",
		"valid_max": f"{args.lon_max}",
		"axis":"X",
		"reference_datum": "geographical coordinates, WGS84 projection"
    }

    lat_attributes = {
        "long_name": "latitude",
        "standard_name": "latitude",
        "units": "degrees_north",
        "valid_min": f"{args.lat_min}",
        "valid_max": f"{args.lat_max}",
        "axis": "Y",
        "reference_datum": "geographical coordinates, WGS84 projection"
    }

    if args.one_d:
        target_ds[args.lat_variable_name] = xr.DataArray(lats, dims=(args.y_dim,),
                                                         attrs=lat_attributes)
        target_ds[args.lon_variable_name] = xr.DataArray(lons, dims=(args.x_dim,),
                                                         attrs=lon_attributes)
    else:

        lats2d = np.broadcast_to(lats[None].T, shape)
        lons2d = np.broadcast_to(lons, shape)

        target_ds[args.lat_variable_name] = xr.DataArray(lats2d, dims=(args.y_dim,args.x_dim),attrs=lat_attributes)
        target_ds[args.lon_variable_name] = xr.DataArray(lons2d, dims=(args.y_dim,args.x_dim),attrs=lon_attributes)

    target_ds.to_netcdf(args.output_path)

