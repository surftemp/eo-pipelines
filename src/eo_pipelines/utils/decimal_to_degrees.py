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

import math

def convert(value, is_lat):
    degrees = math.floor(value)
    minutes = math.floor((value - degrees)*60)
    seconds = math.floor((value - degrees - (minutes/60))*3600)
    suffix = ""
    if is_lat:
        if value < 0:
            suffix = "S"
        else:
            suffix = "N"
    else:
        if value < 0:
            suffix = "W"
        else:
            suffix = "E"
    return f"{degrees}.{minutes}.{seconds}{suffix}"

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--lat", type=float)
parser.add_argument("--lon", type=float)

args = parser.parse_args()
if args.lat:
    lat = convert(args.lat, is_lat=True)
    print(f"lat={lat}")
if args.lon:
    lon = convert(args.lon, is_lat=False)
    print(f"lon={lon}")
