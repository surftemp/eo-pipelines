
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
    parser.add_argument("--y-dim", help="y dimension name", default="nj")
    parser.add_argument("--x-dim", help="x dimension name", default="ni")
    parser.add_argument("--one-d", action="store_true", help="write 1d lat lon arrays")

    args = parser.parse_args()

    target_ds = xr.Dataset()
    lats = np.flip(np.arange(args.lat_min, args.lat_max, args.lat_step))
    lons = np.arange(args.lon_min, args.lon_max, args.lon_step)

    shape = len(lats), len(lons)

    if args.one_d:
        target_ds[args.lat_variable_name] = xr.DataArray(lats, dims=(args.y_dim,),
                                                         attrs={"units": "degrees_north", "standard_name": "latitude"})
        target_ds[args.lon_variable_name] = xr.DataArray(lons, dims=(args.x_dim,),
                                                         attrs={"units": "degrees_east", "standard_name": "longitude"})
    else:

        lats2d = np.broadcast_to(lats[None].T, shape)
        lons2d = np.broadcast_to(lons, shape)

        target_ds[args.lat_variable_name] = xr.DataArray(lats2d, dims=(args.y_dim,args.x_dim),attrs={"units":"degrees_north","standard_name":"latitude"})
        target_ds[args.lon_variable_name] = xr.DataArray(lons2d, dims=(args.y_dim,args.x_dim),attrs={"units":"degrees_east","standard_name":"longitude"})

    target_ds.to_netcdf(args.output_path)

