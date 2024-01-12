import geopandas as gp
import pandas as pd
import os.path

folder = "/home/dev/Projects/greenland_lakes/lakes/"

filenames = ["lake1443.geojson","lake1630.geojson","lake1782.geojson"]

# df = geopandas.read_file(path,where="LakeID=765")
# Leverett lake ids are 765 766 1019 1020
# Sources are S1, S2
dfs = []
for filename in filenames:
    path = os.path.join(folder,filename)
    df = gp.read_file(path)
    dfs.append(df)

df = pd.concat(dfs)

# df = df.cx[-50.164:-49.926, 67.076:67.02]
df.plot()
from matplotlib import pyplot as plt
plt.show()