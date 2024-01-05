import os

import geopandas
import math
import csv
import numpy as np
import xarray as xr

target_resolution_m = 50
m_per_degree = 111000  # approx m per degree of latitude

max_box_height = 500
max_box_width = 500

path = ""

class Box:

    def __init__(self, lake_ids, min_lat, min_lon, max_lat, max_lon):
        self.lake_ids = lake_ids
        self.min_lat = min_lat
        self.min_lon = min_lon
        self.max_lat = max_lat
        self.max_lon = max_lon

    def expand_box(self, boundary_m):
        mid_lat = (self.min_lat + self.max_lat) / 2
        lat_boundary = boundary_m / 111000
        lon_boundary = math.cos(math.radians(mid_lat)) * lat_boundary
        self.max_lat += lat_boundary
        self.min_lat -= lat_boundary
        self.max_lon += lon_boundary
        self.min_lon -= lon_boundary

    def merge(self, other):
        min_lat = min(self.min_lat, other.min_lat)
        max_lat = max(self.max_lat, other.max_lat)

        min_lon = min(self.min_lon, other.min_lon)
        max_lon = max(self.max_lon, other.max_lon)
        return Box(self.lake_ids+other.lake_ids,min_lat,min_lon,max_lat,max_lon)

    def get_resolution(self):
        mid_lat = (self.min_lat + self.max_lat) / 2
        resolution_degrees_y = target_resolution_m / m_per_degree  # height of each target pixel in degrees
        # work out width of each target pixel in degrees for the mid-latitude
        # width will increase at higher latitudes
        resolution_degrees_x = resolution_degrees_y / math.cos(math.radians(mid_lat))
        return (resolution_degrees_y, resolution_degrees_x)

    def get_size_pixels(self):
        (resolution_degrees_y, resolution_degrees_x) = self.get_resolution()
        height = int((self.max_lat - self.min_lat) / resolution_degrees_y)
        width = int((self.max_lon - self.min_lon) / resolution_degrees_x)
        return (height, width)

    def export_netcdf(self, to_path):
        (resolution_degrees_y, resolution_degrees_x) = self.get_resolution()
        lats = np.flip(np.arange(self.min_lat, self.max_lat, resolution_degrees_y))
        lons = np.arange(self.min_lon, self.max_lon, resolution_degrees_y)
        shape = len(lats), len(lons)

        lats2d = np.broadcast_to(lats[None].T, shape)
        lons2d = np.broadcast_to(lons, shape)

        target_ds = xr.Dataset(attrs={"lakeids":";".join(self.lake_ids)})
        target_ds["lat"] = xr.DataArray(lats2d, dims=("nj", "ni"),
                                                         attrs={"units": "degrees_north", "standard_name": "latitude"})
        target_ds["lon"] = xr.DataArray(lons2d, dims=("nj", "ni"),
                                                         attrs={"units": "degrees_east", "standard_name": "longitude"})

        target_ds.to_netcdf(to_path)

    def __str__(self):
        return f"Box({self.lake_ids},({self.min_lat},{self.min_lon}),({self.max_lat},{self.max_lon}))"


class BoxMerger:

    def __init__(self):
        self.merged_boxes = []
        self.current_box = None

    def add_box(self, box):
        if self.current_box is None:
            self.current_box = box
        else:
            merged = self.current_box.merge(box)
            (h,w) = merged.get_size_pixels()
            if h > max_box_height or w > max_box_width:
                self.merged_boxes.append(self.current_box)
                self.current_box = box
            else:
                self.current_box = merged

    def get_merged_boxes(self):
        return self.merged_boxes + [self.current_box] if self.current_box else []


def export_to_csv(export_list, path, for_sector):
    with open(path,"w") as f:
        out = csv.writer(f)
        out.writerow(["target_grid_path","sector","min_lat","min_lon","max_lat","max_lon","lat_resolution","lon_resolution","height","width","lake_ids"])
        for (filename,box) in export_list:
            (lat_resolution,lon_resolution) = box.get_resolution()
            (height,width) = box.get_size_pixels()
            out.writerow([filename,for_sector,box.min_lat,box.min_lon,box.max_lat,box.max_lon,lat_resolution,lon_resolution,height,width,";".join(box.lake_ids)])


def extract_boxes(df, output_folder, sector):
    boxlist = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        bounds = df.bounds.iloc[idx]
        boxlist.append(Box([str(row.LakeID)], bounds.miny, bounds.minx, bounds.maxy, bounds.maxx))

    for box in boxlist:
        box.expand_box(1000)

    # sort by ascending minimum lat
    boxlist = sorted(boxlist, key=lambda v: v.min_lat)

    merger = BoxMerger()
    for box in boxlist:
        merger.add_box(box)

    boxlist = merger.get_merged_boxes()

    print("\n".join(map(str, boxlist[:10])))

    os.makedirs(output_folder, exist_ok=True)
    idx = 0
    export_list = []
    for box in boxlist:
        filename = f"box{idx}.nc"
        path = os.path.join(output_folder, filename)
        export_list.append((os.path.abspath(filename), box))
        idx += 1
        box.export_netcdf(path)

    export_to_csv(export_list, os.path.join(output_folder, "summary.csv"), sector)

def extract_shapes(df, output_folder):
    os.makedirs(output_folder,exist_ok=True)
    for idx in range(len(df)):
        row = df.iloc[idx]
        poly = row.geometry
        j = geopandas.GeoSeries([poly]).to_json()
        filename = f"lake{str(row.LakeID)}.geojson"
        path = os.path.join(output_folder,filename)
        with open(path,"w") as f:
            f.write(j)

def extract_kml(df, output_path):
    import simplekml
    import json

    with open("myfile.geojson") as f:
        data = json.load(f)
    kml = simplekml.Kml()
    for feature in data['features']:
        geom = feature['geometry']
        geom_type = geom['type']
        if geom_type == 'Polygon':
            kml.newpolygon(name='test',
                           description='test',
                           outerboundaryis=geom['coordinates'][0])
        elif geom_type == 'LineString':
            kml.newlinestring(name='test',
                              description='test',
                              coords=geom['coordinates'])
        elif geom_type == 'Point':
            kml.newpoint(name='test',
                         description='test',
                         coords=[geom['coordinates']])
        else:
            print("ERROR: unknown type:", geom_type)
    kml.save('kml_file.kml')

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-path",help="path to geojson file containing inventory",default="/home/dev/Projects/greenland_lakes/lakes.geojson")
    parser.add_argument("--output-folder",help="path to output folder to export details of target grids",default="extract")
    parser.add_argument("--sector",help="name of sector to process",default="SW")
    args = parser.parse_args()

    # Leverett lake ids are 765 766 1019 1020
    # Possible Sources are S1, S2
    df = geopandas.read_file(args.inventory_path,where=f"DrainageBa='{args.sector}'")
    # df = df.cx[-50.164:-49.926, 67.076:67.02]

    df = df.drop_duplicates("LakeID")
    # work out merged boxes
    extract_boxes(df,os.path.join(args.output_folder,"boxes"),args.sector)
    # extract individual lake geojsons
    extract_shapes(df,os.path.join(args.output_folder,"lakes"))

    extract_kml(df,"test.kml")


