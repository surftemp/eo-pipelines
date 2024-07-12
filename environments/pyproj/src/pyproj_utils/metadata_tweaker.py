# -*- coding: utf-8 -*-

#    landsat2nc
#    Copyright (C) 2020  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Utility for tweaking the metadata of netcdf4 files"""

import argparse
import logging
import json
import copy

import xarray as xr

class MetadataTweaker:

    def __init__(self, input_path, output_path, config_path):
        with open(config_path) as f:
            self.config = json.loads(f.read())
        self.input_path = input_path
        self.output_path = output_path
        self.ds = None
        self.variable_attrs = {}
        self.global_attrs = {}

    def resolve(self, attrs, modifiers):
        for (k,v) in modifiers.items():
            if v is None:
                if k in attrs:
                    del attrs[k]
            else:
                if isinstance(v,str) and v.startswith("{") and v.endswith("}"):
                    # substitute a value from another attribute
                    lookup = v[1:-1].split(".")
                    if len(lookup) == 1:
                        # from a global attribute
                        new_value = self.global_attrs.get(lookup[0])
                        attrs[k] = new_value
                    elif len(lookup) == 2:
                        # from a variable attribute: variable.attr
                        new_value = self.variable_attrs.get(lookup[0],{}).get(lookup[1],None)
                        if new_value is not None:
                            attrs[k] = new_value
                else:
                    attrs[k] = v

    def process(self):
        self.ds = xr.open_dataset(self.input_path)
        self.global_attrs = copy.deepcopy(self.ds.attrs)
        self.variable_attrs = {}
        for variable in self.ds.variables:
            self.variable_attrs[variable] = copy.deepcopy(self.ds[variable].attrs)

        if "global_attributes" in self.config:
            global_modifiers = self.config["global_attributes"]
            self.resolve(self.ds.attrs,global_modifiers)

        if "variable_attributes" in self.config:
            variable_modifiers = self.config["variable_attributes"]
            for variable in variable_modifiers:
                if variable in self.ds.variables:
                    self.resolve(self.ds[variable].attrs,variable_modifiers[variable])

        self.ds.to_netcdf(self.output_path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-path", help="path to an input netcdf4 file")
    parser.add_argument(
        "--output-path", help="path to an output netcdf4 file")
    parser.add_argument(
        "--config-path", help="path to a JSON formatted config file")

    args = parser.parse_args()

    processor = MetadataTweaker(args.input_path, args.output_path, args.config_path)
    processor.process()
