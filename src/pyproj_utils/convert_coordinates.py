import pyproj

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--from_crs",type=int, default=4326)
parser.add_argument("--to_crs", type=int, default=27700)
parser.add_argument("y",type=float)
parser.add_argument("x",type=float)

args = parser.parse_args()

transformer = pyproj.Transformer.from_crs(args.from_crs, args.to_crs)
to_x, to_y = transformer.transform([args.y],[args.x])
print(f"y={to_y[0]}, x={to_x[0]}")