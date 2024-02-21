

import rioxarray
import os
import logging
import xarray as xr

logger = logging.getLogger("write_crs")

def convert_file(input_path, output_path, epsg):
    ds = xr.open_dataset(input_path)
    da = ds["sec"]
    da_crs = da.rio.write_crs(f"EPSG:{epsg}")
    da_crs.to_netcdf(output_path)
    logger.info(f"Converted {input_path} => {output_path}")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder",help="path to input folder containing files to modiify")
    parser.add_argument("output_folder", help="path to output folder into which modified files will be written")
    parser.add_argument("epsg", type=int, help="EPSG number to assign")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    os.makedirs(args.output_folder, exist_ok=True)
    for filename in os.listdir(args.input_folder):
        input_path = os.path.join(args.input_folder,filename)
        output_path = os.path.join(args.output_folder,filename)
        convert_file(input_path,output_path,args.epsg)


if __name__ == '__main__':
    main()
