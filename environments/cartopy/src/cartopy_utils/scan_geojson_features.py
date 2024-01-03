import json

cells = set()
with open("subset_filtered.geojson") as f:
    o = json.loads(f.read())
    filtered_features = []
    for f in o["features"]:
        p = f["properties"]
        path = int(p["PATH"])
        row = int(p["ROW"])
        cells.add((row,path))

with open("row_path.csv","w") as of:
    of.write("row,path")
    for (row,path) in cells:
        of.write(f"\n{row},{path}")