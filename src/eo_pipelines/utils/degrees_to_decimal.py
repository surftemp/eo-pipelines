# MIT License
#
# Copyright (c) 2022-2025 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re

lat_string_regexp = r"(\d+)\.(\d+)\.(\d+)(N|n|S|s)"
lon_string_regexp = r"(\d+)\.(\d+)\.(\d+)(W|w|E|e)"


def convert(value, is_lat):
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
    lat = convert(args.lat, is_lat=True)
    print(f"lat={lat}")
if args.lon:
    lon = convert(args.lon, is_lat=False)
    print(f"lon={lon}")
