
import argparse
import json
import shapely

class WRSSearch:

    def __init__(self, wrs_geojson_path):
        self.cells = {} # (row,path) to shapely geometry
        with open(wrs_geojson_path) as f:
            o = json.loads(f.read())
            for f in o["features"]:
                p = f["properties"]
                path = p["PATH"]
                row = p["ROW"]

                g = f["geometry"]
                gt = g["type"]
                coords = g["coordinates"]
                points = []

                for l1 in coords:
                    for l2 in l1:
                        for l3 in l2:
                            lon = l3[0]
                            lat = l3[1]
                            points.append((lon,lat))

                geom = shapely.Polygon(shell=points)
                self.cells[(row,path)] = geom

    def run(self,min_lat, min_lon, max_lat, max_lon):
        results = []
        bbox = shapely.Polygon(shell=[(min_lon,min_lat),(max_lon,min_lat),(max_lon,max_lat),(min_lon,max_lat)])
        for ((row,path),cell) in self.cells.items():
            if bbox.intersects(cell):
                results.append((row,path,bbox.covered_by(cell)))
        return results

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--wrs-path", help="Path to geojson file containing WRS definitions",
                        default="wrs2.geojson")
    # defaults: Leavrett
    parser.add_argument("--min-lat", default=67.01990443)
    parser.add_argument("--max-lat", default=67.07754789)
    parser.add_argument("--min-lon", default=-50.16443218)
    parser.add_argument("--max-lon", default=-49.92716455)

    args = parser.parse_args()
    search = WRSSearch(args.wrs_path)

    matches = search.run(min_lat=args.min_lat,max_lat=args.max_lat, min_lon = args.min_lon, max_lon=args.max_lon)
    for (row,path,encloses) in matches:
        print(f"row={row},path={path},encloses={encloses}")