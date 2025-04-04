import argparse
import os
import zipfile
import logging

import shapely
import shapely.geometry as sgeom


class WRSSearch:

    def __init__(self, max_overlap_fraction):
        self.logger = logging.getLogger("usgs_filter")
        folder = os.path.split(__file__)[0]
        path = os.path.join(folder, "WRS_descending.csv.zip")
        self.areas = {}
        self.max_overlap_fraction = max_overlap_fraction
        with open(path, "rb") as f:
            zf = zipfile.ZipFile(f)
            filename = zf.namelist()[0]
            f = zf.open(filename)
            content = f.read().decode("utf-8").split("\n")

            headers = {}
            for line in content:
                items = line.split(",")
                if len(items) <= 1:
                    continue
                if not headers:
                    for idx in range(len(items)):
                        headers[items[idx]] = idx
                else:
                    row = int(items[headers["ROW"]])
                    path = int(items[headers["PATH"]])
                    coords = items[headers["COORDS"]].split(":")
                    coords = list(map(lambda c: (float(c.split(";")[0]), float(c.split(";")[1])), coords))
                    poly = sgeom.Polygon(coords)
                    self.areas[(row, path)] = poly

        self.paths = {}
        for (row, path) in self.areas:
            if path not in self.paths:
                self.paths[path] = []
            self.paths[path].append(self.areas[(row, path)])

        for path in self.paths:
            self.paths[path] = shapely.union_all(self.paths[path])

    def scene_matches(self, search_poly, min_overlap_fraction=0):
        search_area = search_poly.area
        matches = []
        for (row, path) in self.areas:
            test_area = self.areas[(row, path)]
            if (test_area.intersects(search_poly)):
                intersection = test_area.intersection(search_poly)
                overlap_fraction = intersection.area / search_area
                if overlap_fraction >= min_overlap_fraction:
                    matches.append((row, path))
        return matches

    def path_matches(self, search_poly):
        search_area = search_poly.area
        matches = []
        for path in self.paths:
            test_area = self.paths[path]
            if (test_area.intersects(search_poly)):
                intersection = test_area.intersection(search_poly)
                overlap_fraction = intersection.area / search_area
                print(overlap_fraction)
                if overlap_fraction >= self.max_overlap_fraction:
                    matches.append(path)
        return matches


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", help="Path to CSV file containing landsat scene identifiers")
    parser.add_argument("--min-overlap-fraction", type=float, help="min overlap with the bounding box", required=True)
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", required=True)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", required=True)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", required=True)
    args = parser.parse_args()

    search_area = sgeom.Polygon(
        [[args.lon_min, args.lat_min], [args.lon_max, args.lat_min], [args.lon_max, args.lat_max],
         [args.lon_min, args.lat_max], [args.lon_min, args.lat_min]])

    search = WRSSearch(args.min_overlap_fraction)

    # work out which paths have sufficient overlap with the search area
    path_matches = search.path_matches(search_area)

    # collect all the intersecting scenes
    row_paths = search.scene_matches(search_area)

    # the results are all intersecting scenes which belong to one of the paths
    row_path_matches = set((row, path) for (row, path) in row_paths if path in path_matches)

    # now filter the CSV
    filtered_lines = []
    input_count = 0
    with open(args.csv_path) as f:
        for line in f.readlines():
            input_count += 1
            items = line.split(",")
            scene_id = items[2].strip()
            # extract the ROW and PATH from the filename
            # https://gisgeography.com/landsat-file-naming-convention/
            # LC80220032014163LGN01
            path = int(scene_id[3:6])
            row = int(scene_id[6:9])
            if (row, path) in row_path_matches:
                filtered_lines.append(line)

    with open(args.csv_path, "w") as of:
        for line in filtered_lines:
            of.write(line)

    if input_count > 0:
        print(f"Filtered {len(filtered_lines) / input_count}")
    else:
        print("No input scenes were read")
