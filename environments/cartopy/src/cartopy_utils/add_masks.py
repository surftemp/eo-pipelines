import os
import json
import xarray as xr
import numpy as np
import argparse
from shapely.geometry import shape
from shapely import Point

class Masker:

    def __init__(self, input_path, output_path, layer_dict):
        self.input_path = input_path
        self.output_path = output_path
        self.layer_dict = layer_dict

    def run(self):
        ds = xr.open_dataset(self.input_path)
        lats = ds["lat"].data
        lons = ds["lon"].data
        if len(lats.shape) == 1:
            nlats = lats.shape[0]
            nlons = lons.shape[0]

            output_shape = (nlats,nlons)

            lats = np.broadcast_to(lats[None].T, output_shape)
            lons = np.broadcast_to(lons, output_shape)
        else:
            (nlats,nlons) = lats.shape

        for layer_name in self.layer_dict:
            print(f"Adding mask {layer_name}")
            mask = np.zeros((nlats, nlons), dtype=np.int8)

            with open(self.layer_dict[layer_name]) as f:
                geojson = json.loads(f.read())

                shapes = []

                for feature in geojson["features"]:
                    shapes.append(shape(feature["geometry"]).buffer(0))

                for s in shapes:
                    for y in range(0,nlats):
                        for x in range(0,nlons):
                            p = Point(lons[y,x],lats[y,x])
                            if s.contains(p):
                                mask[y,x] = 1

            ds[layer_name] = xr.DataArray(data=mask,dims=("nj","ni"))

        ds.to_netcdf(self.output_path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", help="path to input netcdf4 file", required=True)
    parser.add_argument("--output-path", help="path to output netcdf4 file", required=True)
    parser.add_argument("--add-layers", nargs="+", help="1 or more layer specifications of the form layer_name=path_to_geojson")

    args = parser.parse_args()
    layers = {}
    for add_layer in args.add_layers:
        kv = add_layer.split("=")
        layers[kv[0]] = kv[1]

    masker = Masker(args.input_path, args.output_path, layers)
    masker.run()
