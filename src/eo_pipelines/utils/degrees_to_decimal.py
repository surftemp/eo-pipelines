import re

lat_string_regexp = r"(\d+)\.(\d+)\.(\d+)(N|n|S|s)"
lon_string_regexp = r"(\d+)\.(\d+)\.(\d+)(W|w|E|e)"

def convert(value,is_lat):
    matches = re.match(
        lat_string_regexp if is_lat else lon_string_regexp,
        value)
    if matches:
        degrees = matches.group(1)
        minutes = matches.group(2)
        seconds = matches.group(3)
        nsew = matches.group(4)
        value = float(degrees) + float(minutes) / 60 + float(seconds) / 3600
        if is_lat and (nsew == "S" or nsew == "s"):
            value = -value
        if not is_lat and (nsew == "W" or nsew == "w"):
            value = -value
        return value
    else:
        return None

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--lat")
parser.add_argument("--lon")

args = parser.parse_args()
if args.lat:
    lat=convert(args.lat, is_lat=True)
    print(f"lat={lat}")
if args.lon:
    lon = convert(args.lon, is_lat=False)
    print(f"lon={lon}")
