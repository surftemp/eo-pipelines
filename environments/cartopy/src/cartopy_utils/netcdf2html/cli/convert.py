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
import fnmatch
import requests

from layers import LayerFactory

from cartopy_utils.netcdf2html.htmlfive.html5_builder import Html5Builder

from cartopy_utils.netcdf2html.fragments.utils import anti_aliasing_style
from cartopy_utils.netcdf2html.fragments.image import save_image, save_image_falsecolour, save_image_mask, ImageFragment

js_path = os.path.join(os.path.split(__file__)[0],"index.js")
css_path = os.path.join(os.path.split(__file__)[0],"index.css")

def get_image_dimensions(ds):
    if len(ds.lat.shape) == 2:
        image_height = ds.lat.shape[0]
        image_width = ds.lon.shape[1]
    elif len(ds.lat.shape) == 1:
        image_height = ds.lat.shape[0]
        image_width = ds.lon.shape[1]
    else:
        raise Exception("Unable to determin image dimensions from dataset")
    return (image_width, image_height)



class Convert:

    def __init__(self, input_path, output_folder, title, max_zoom=None):
        self.input_path = input_path
        self.input_filename = os.path.split(self.input_path)[1]
        self.output_folder = output_folder
        self.title = title
        self.max_zoom = max_zoom
        self.output_html_path = os.path.join(output_folder,"index.html")
        image_folder = os.path.join(self.output_folder,"images")
        os.makedirs(image_folder,exist_ok=True)
        shutil.copyfile(js_path,os.path.join(self.output_folder,"index.js"))
        shutil.copyfile(css_path, os.path.join(self.output_folder, "index.css"))
        self.layer_definitions = []

    def add_layer(self, ds, name, spec):
        self.layer_definitions += LayerFactory.create(ds,name,spec)

    def get_image_path(self, key, timestamp=""):
        filename = key+timestamp+".png"
        src = os.path.join("images", filename)
        path = os.path.join(self.output_folder, src)
        return (src,path)

    def run(self):
        time_slices = []
        image_width = image_height = None
        if os.path.isdir(self.input_path):
            for filename in os.listdir(self.input_path):
                if filename.endswith(".nc"):
                    ds = xr.open_dataset(os.path.join(self.input_path, filename))
                    timestamp = str(ds["time"].data[0])[:10]
                    time_slices.append((timestamp, ds))
                    image_width, image_height = get_image_dimensions(ds)

            time_slices = sorted(time_slices, key=lambda item: item[0])
        else:
            ds = xr.open_dataset(self.input_path)
            image_width, image_height = get_image_dimensions(ds)
            tlen = len(ds["time"])
            for t in range(tlen):
                timestamp = str(ds["time"].data[t])[:10]
                time_slices.append((timestamp, ds.isel(time=slice(t, t + 1))))


        builder = Html5Builder(language="en")

        builder.head().add_element("title").add_text(self.title)
        builder.head().add_element("style").add_text(anti_aliasing_style)
        builder.head().add_element("script",{"src":"index.js"})
        builder.head().add_element("link", {"rel": "stylesheet", "href":"index.css"})
        builder.head().add_element("link", {"rel": "stylesheet", "href": "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"})
        builder.head().add_element("script", {"src": "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"})

        initial_zoom = 768 / image_width
        if initial_zoom < 1:
            initial_zoom = 1
        if self.max_zoom and self.max_zoom > initial_zoom:
            max_zoom = self.max_zoom
        else:
            max_zoom = 2048 / image_width

        root = builder.body().add_element("div")
        header_div = root.add_element("div")

        header = header_div.add_element("h4").add_text("QUICKVIEW: "+self.title)
        header.add_element("span", {"class": "spacer"}).add_text("|")
        header.add_element("a",{"href":self.input_filename, "download":self.input_filename}).add_text("download netcdf4")

        header.add_element("span", {"class": "spacer"}).add_text("|")
        header.add_element("label",{"for":"zoom_control"}).add_text("Zoom")
        header.add_element("input", {"type": "range", "id": "zoom_control", "min": 1, "max": int(max_zoom), "step": 1,
                                         "value": initial_zoom})

        header.add_element("span",{"class":"spacer"}).add_text("|")
        header.add_element("button", {"id": "prev_btn"}).add_text("Previous")
        header.add_element("input", {"type": "range", "id": "time_index"})
        header.add_element("button", {"id": "next_btn"}).add_text("Next")
        header.add_element("span", {"id": "scene_label"}).add_text("?")

        header.add_element("span", {"class": "spacer"}).add_text("|")

        header.add_element("input", {"type": "checkbox", "id": "show_layers", "checked": "checked"}).add_text(
            "Show Layers")

        header.add_element("span", {"class": "spacer"}).add_text("|")

        header.add_element("input", {"type": "checkbox", "id": "show_filters", "checked": "checked"}).add_text(
            "Show Filters")

        controls_div = root.add_element("div",{"id": "layer_container", "class":"control_container"})
        controls_div.add_element("div", {"id": "layer_container_header", "class": "control_container_header"}).add_text("Layers")

        slider_fieldset = controls_div.add_element("fieldset", style={"display":"inline"})
        slider_fieldset.add_element("legend").add_text("Layers")
        slider_container = slider_fieldset.add_element("div")
        slider_table = slider_container.add_element("table",{"id":"slider_controls"})
        slider_container.add_element("input", {"type": "button", "id": "close_all_sliders", "value": "Hide All Layers"})
        layer_list = []

        for layer_definition in self.layer_definitions:
            row = slider_table.add_element("tr")
            col1 = row.add_element("td")
            col2 = row.add_element("td")
            col3 = row.add_element("td")
            col1.add_text(layer_definition.layer_label)
            opacity_control_id = layer_definition.layer_name + "_opacity"
            col2.add_element("input", {"type": "range", "id": opacity_control_id})
            layer_list.insert(0,{"name": layer_definition.layer_name, "opacity_control_id": opacity_control_id})
            if layer_definition.has_legend():
                legend_src,legend_path = self.get_image_path(layer_definition.layer_name+"_legend")
                layer_definition.build_legend(legend_path)
                col3.add_element("span").add_text(str(layer_definition.vmin))
                col3.add_element("img",{"src":legend_src, "class":"legend"})
                col3.add_element("span").add_text(str(layer_definition.vmax))

        filter_div = root.add_element("div", {"id": "filter_container", "class": "control_container"})
        filter_div.add_element("div",{"id": "filter_container_header", "class":"control_container_header"}).add_text("Scene Filters")

        filter_fieldset = filter_div.add_element("fieldset", style={"display": "inline"})
        filter_fieldset.add_element("legend").add_text("Filters")

        month_filter = filter_fieldset.add_element("div", {})

        for month in range(0, 12):
            month_name = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][month]
            month_filter.add_element("span").add_text(month_name)
            month_filter.add_element("input", {"id": f"month{month + 1}", "type": "checkbox", "checked": "checked"})

        self.layer_definitions.reverse()

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
                (src, path) = self.get_image_path(layer_definition.layer_name,timestamp=timestamp)
                image_srcs[layer_definition.layer_name] = src
                layer_definition.build(ds,path)
                if first_slice:
                    image_div.add_fragment(ImageFragment(src, layer_definition.layer_name, alt_text=timestamp))
                    layers.insert(0,{"name": layer_definition.layer_name, "label": layer_definition.layer_label})

            time_index.append({"timestamp": timestamp, "image_srcs": image_srcs, "layers":layers})
            first_slice = False

        builder.head().add_element("script").add_text("let all_time_index="+json.dumps(time_index)+";")
        builder.head().add_element("script").add_text("let layer_list=" + json.dumps(layer_list) + ";")
        builder.head().add_element("script").add_text("let image_width=" + json.dumps(image_width) + ";")

        with open(self.output_html_path, "w") as f:
            f.write(builder.get_html())

        shutil.copy(self.input_path, self.output_folder)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", help="set the title of the plot", required=True)
    parser.add_argument("--input-path", help="netcdf4 file or folder containing netcdf4 files to visualise", required=True)
    parser.add_argument("--output-path", help="folder to write html output", default="html_output")
    parser.add_argument("--layer-config-path", help="JSON file specifying one or more layer specifications", default="layers.json")
    parser.add_argument("--max-zoom", type=int, default=None, help="set the maximum zoom (ignored if this would result in an image size of less than 768 pixels)")

    args = parser.parse_args()

    c = Convert(args.input_path, os.path.abspath(args.output_path), args.title, max_zoom=args.max_zoom)

    variable_names = set()
    ds = None
    if os.path.isfile(args.input_path):
        ds = xr.open_dataset(args.input_path)
    elif os.path.isdir(args.input_path):
        for name in os.listdir(args.input_path):
            if name.endswith(".nc"):
                ds = xr.open_dataset(os.path.join(args.input_path,name))
                break
    if ds:
        for name in ds.variables:
            variable_names.add(name)

    with open(args.layer_config_path) as f:
        layer_specs = json.loads(f.read())
        for (layer_name,layer) in layer_specs.items():
            c.add_layer(layer_name,layer,variable_names)

    c.run()

if __name__ == '__main__':
    main()