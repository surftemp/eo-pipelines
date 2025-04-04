import json

path = "/home/dev/Downloads/WRS2_descending.geojson"
output_path = "/home/dev/Downloads/WRS2_descending.csv"

with open(output_path, "w") as of, open(path) as f:
    obj = json.loads(f.read())
    of.write("ROW,PATH,COORDS\n")
    for feature in obj["features"]:
        row = feature["properties"]["ROW"]
        path = feature["properties"]["PATH"]
        clist = []
        coordinates = feature["geometry"]["coordinates"][0][0]
        for pair in coordinates:
            clist.append(f"{pair[0]:0.2f};{pair[1]:0.3f}")
        of.write(f"{row},{path},{':'.join(clist)}\n")
