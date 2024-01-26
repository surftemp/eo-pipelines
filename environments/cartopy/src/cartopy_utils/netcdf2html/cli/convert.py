# MIT License
#
# Copyright (c) 2023 Niall McCarroll
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

import os
import xarray as xr
import numpy as np
import argparse
import shutil
import json

from cartopy_utils.netcdf2html.htmlfive.html5_builder import Html5Builder

from cartopy_utils.netcdf2html.fragments.utils import anti_aliasing_style
from cartopy_utils.netcdf2html.fragments.image import save_image, save_image_falsecolour, save_image_mask, ImageFragment

js_path = os.path.join(os.path.split(__file__)[0],"index.js")
css_path = os.path.join(os.path.split(__file__)[0],"index.css")

class LayerRGB:

    def __init__(self, layer_name, layer_label, red_variable, green_variable, blue_variable):
        self.layer_name = layer_name
        self.layer_label = layer_label
        self.red_variable = red_variable
        self.green_variable = green_variable
        self.blue_variable = blue_variable

    def build(self,ds,path):
        red = ds[self.red_variable].data[0, :, :]
        green = ds[self.green_variable].data[0, :, :]
        blue = ds[self.blue_variable].data[0, :, :]

        save_image_falsecolour(red, green, blue, path)

class LayerSingleBand:

    def __init__(self, layer_name, layer_label, band_name, vmin, vmax):
        self.layer_name = layer_name
        self.layer_label = layer_label
        self.band_name = band_name
        self.vmin = vmin
        self.vmax = vmax

    def build(self,ds,path):
        da = ds[self.band_name]
        save_image(da.data[0, :, :], self.vmin, self.vmax, path)

class LayerMask:

    def __init__(self, layer_name, layer_label, band_name):
        self.layer_name = layer_name
        self.layer_label = layer_label
        self.band_name = band_name

    def build(self,ds,path):
        da = ds[self.band_name]
        save_image_mask(da.data[ :, :], path, 255, 0, 0)


class Convert:

    def __init__(self, input_path, output_folder):
        self.input_path = input_path
        self.output_folder = output_folder
        self.output_html_path = os.path.join(output_folder,"index.html")
        image_folder = os.path.join(self.output_folder,"images")
        os.makedirs(image_folder,exist_ok=True)
        shutil.copyfile(js_path,os.path.join(self.output_folder,"index.js"))
        shutil.copyfile(css_path, os.path.join(self.output_folder, "index.css"))
        self.layer_definitions = [LayerSingleBand("B10_ST","B10_ST","ST",260,300),
                                  LayerRGB("rgb","False Colour","B4","B3","B3")]

    def load_layer_masks(self, from_ds):
        for v in from_ds.variables:
            if v.startswith("lake"):
                self.layer_definitions.insert(0,(LayerMask(v,"Lake "+ v[4:],v)))

    def get_image_path(self, timestamp, key):
        filename = key+timestamp+".png"
        src = os.path.join("images", filename)
        path = os.path.join(self.output_folder, src)
        return (src,path)

    def run(self):
        time_slices = []
        image_width = None
        if os.path.isdir(self.input_path):
            for filename in os.listdir(self.input_path):
                if filename.endswith(".nc"):
                    ds = xr.open_dataset(os.path.join(self.input_path, filename))
                    timestamp = str(ds["time"].data[0])[:10]
                    time_slices.append((timestamp, ds))
                    image_width = ds.lat.shape[0]
            time_slices = sorted(time_slices, key=lambda item: item[0]);
        else:
            ds = xr.open_dataset(self.input_path)
            image_width = ds.lat.shape[0]
            tlen = len(ds["time"])
            for t in range(tlen):
                timestamp = str(ds["time"].data[t])[:10]
                time_slices.append((timestamp, ds.isel(time=slice(t, t + 1))))

            self.load_layer_masks(ds)

        builder = Html5Builder(language="en")

        builder.head().add_element("title").add_text("NetCDF to HTML")
        builder.head().add_element("style").add_text(anti_aliasing_style)
        builder.head().add_element("script",{"src":"index.js"})
        builder.head().add_element("link", {"rel": "stylesheet", "href":"index.css"})

        root = builder.body().add_element("div")

        button_div = root.add_element("div")

        slider_div = button_div.add_element("div", {"id": "slider_div", "class":"control_container"})
        slider_div.add_element("input", {"type": "checkbox", "id":"show_sliders"}).add_text("Show Layer Controls")

        initial_zoom = 768 / image_width
        if initial_zoom < 1:
            initial_zoom = 1
        elif initial_zoom > 16:
            initial_zoom = 16
        slider_div.add_element("input", {"type": "range", "id": "zoom_control", "min":1, "max":16, "value":initial_zoom}).add_text("Zoom")
        slider_container = slider_div.add_element("div",{"id":"slider_container"},{"display":"none"})
        slider_table = slider_container.add_element("table",{"id":"slider_controls"})
        slider_container.add_element("input", {"type": "button", "id": "close_all_sliders", "value": "Hide All"})
        layer_list = []
        self.layer_definitions.reverse()
        for layer_definition in self.layer_definitions:
            row = slider_table.add_element("tr")
            col1 = row.add_element("td")
            col2 = row.add_element("td")
            col1.add_text(layer_definition.layer_label)
            opacity_control_id = layer_definition.layer_name + "_opacity"
            col2.add_element("input", {"type": "range", "id": opacity_control_id})
            layer_list.append({"name": layer_definition.layer_name, "opacity_control_id": opacity_control_id})

        time_controls = button_div.add_element("div",{"class":"control_container"})
        time_controls.add_element("button", {"id": "prev_btn"}).add_text("Previous")
        time_controls.add_element("input", {"type": "range", "id": "time_index"})
        time_controls.add_element("button", {"id": "next_btn"}).add_text("Next")
        time_controls.add_element("span", {"id": "scene_label"}).add_text("?")

        month_filter = button_div.add_element("div",{"class":"control_container"})

        for month in range(0, 12):
            month_name = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][month]
            month_filter.add_element("span").add_text(month_name)
            month_filter.add_element("input", {"id":f"month{month+1}","type": "checkbox", "checked": "checked"})

        image_container = root.add_element("div")

        time_index = []

        counter = 0

        image_div = image_container.add_element("div", {"id": "image_div", "class": "image_holder"})

        first_slice = True
        layers = []

        for (timestamp, ds) in time_slices:
            counter += 1
            image_srcs = {}
            for layer_definition in self.layer_definitions:
                (src, path) = self.get_image_path(timestamp, layer_definition.layer_name)
                image_srcs[layer_definition.layer_name] = src
                layer_definition.build(ds,path)
                if first_slice:
                    image_div.add_fragment(ImageFragment(src, layer_definition.layer_name, alt_text=timestamp))
                    layers.append({"name": layer_definition.layer_name, "label": layer_definition.layer_label})

            time_index.append({"timestamp": timestamp, "image_srcs": image_srcs, "layers":layers})
            first_slice = False

        builder.head().add_element("script").add_text("let all_time_index="+json.dumps(time_index)+";")
        builder.head().add_element("script").add_text("let layer_list=" + json.dumps(layer_list) + ";")
        builder.head().add_element("script").add_text("let image_width=" + json.dumps(image_width) + ";")

        with open(self.output_html_path, "w") as f:
            f.write(builder.get_html())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", help="netcdf4 file or folder containing netcdf4 files to visualise", required=True)
    parser.add_argument("--output-path", help="folder to write html output", default="html_output")

    args = parser.parse_args()

    c = Convert(args.input_path, os.path.abspath(args.output_path))
    c.run()

if __name__ == '__main__':
    main()