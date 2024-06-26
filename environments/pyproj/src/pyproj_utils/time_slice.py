import argparse
import xarray as xr

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path",help="path to input file containing time to extract")
    parser.add_argument("ref_path", help="path to reference file specifying times of interest")
    parser.add_argument("output_path", help="path to write subset of input file")

    args = parser.parse_args()
    input_ds = xr.open_dataset(args.input_path)
    ref_ds = xr.open_dataset(args.ref_path)

    filtered_ds = input_ds.sel(time=ref_ds.time)
    print(filtered_ds)

    filtered_ds.to_netcdf(args.output_path)

if __name__ == '__main__':
    main()