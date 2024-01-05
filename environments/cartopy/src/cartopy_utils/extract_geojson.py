import geopandas


path = "/home/dev/Projects/greenland_lakes/lakes.geojson"

# df = geopandas.read_file(path,where="LakeID=765")
# Leverett lake ids are 765 766 1019 1020
# Possible Sources are S1, S2
df = geopandas.read_file(path,where="Source='S2'")
df = df.cx[-50.164:-49.926, 67.076:67.02]

for idx in range(len(df)):
    row = df.iloc[idx]
    print(row.LakeID)
bounds_df = df.bounds
for idx in range(len(bounds_df)):
    row = df.bounds.iloc[idx]
    minx = row.minx
    miny = row.miny
    maxy = row.maxy
    maxx = row.maxx
    print((minx,maxx),(miny,maxy))
