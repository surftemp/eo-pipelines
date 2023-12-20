
import argparse
import json
import csv

class WRSSearch:

    def __init__(self, wrs_geojson_path, csv_path):
        self.cells = set() # (row,path) to shapely geometry
        self.wrs_geojson_path = wrs_geojson_path
        with open(csv_path) as f:
            rdr = csv.reader(f)
            cols = {}
            for line in rdr:
                if len(cols) == 0:
                    for idx in range(len(line)):
                        cols[line[idx]] = idx
                else:
                    row = int(line[cols["row"]])
                    path = int(line[cols["path"]])
                    self.cells.add((row,path))

    def run(self):

        results = []
        with open(self.wrs_geojson_path) as f:
            o = json.loads(f.read())
            filtered_features = []
            for f in o["features"]:
                p = f["properties"]
                path = int(p["PATH"])
                row = int(p["ROW"])

                if (row,path) in self.cells:
                    filtered_features.append(f)

            o["features"] = filtered_features
        return o

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--wrs-path", help="Path to geojson file containing WRS definitions",
                        default="wrs2.geojson")
    parser.add_argument("--csv-path", help="Path read to export row/path values",
                        default="row_paths.csv")
    parser.add_argument("--export-geojson-path", help="Path to csv to export row/path values",
                        default="subset.geojson")

    args = parser.parse_args()
    search = WRSSearch(args.wrs_path,args.csv_path)

    filtered_geojson = search.run()
    with open(args.export_geojson_path,"w") as f:
        f.write(json.dumps(filtered_geojson))
