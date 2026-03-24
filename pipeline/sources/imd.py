import os
import sys
from datetime import datetime
import geopandas as gpd
import imdlib as imd
import numpy as np
import pandas as pd
import rasterio
import rasterstats
from rasterio.crs import CRS
from rasterio.transform import Affine, from_bounds
from rasterio.warp import reproject, Resampling
import rioxarray


path = os.getcwd()

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
print(CURRENT_FOLDER)
DATA_FOLDER = os.path.join(CURRENT_FOLDER, "data", "imd")
print(DATA_FOLDER)
TIFF_DATA_FOLDER = os.path.join(DATA_FOLDER, "rain", "tiff")
CSV_DATA_FOLDER = os.path.join(DATA_FOLDER, "rain", "csv")

os.makedirs(TIFF_DATA_FOLDER, exist_ok=True)
os.makedirs(CSV_DATA_FOLDER, exist_ok=True)

def download_data(year: int, start_date: str, end_date: str):
    """
    Download year wise data in the DATA_FOLDER
    The year wise data has datapoint for all days of all months
    """
    current_year = datetime.now().year
    if year == current_year:
        imd.get_real_data(
            var_type="rain",
            start_dy=start_date,
            end_dy=end_date,
            file_dir=DATA_FOLDER,
        )
    else:
        imd.get_data(
            var_type="rain",
            start_yr=year,
            end_yr=year,
            fn_format="yearwise",
            file_dir=DATA_FOLDER,
        )
    return None



def transform_resample_monthly_tif_filenames(tif_filename: str):
    """
    Transform and resample monthly tif files
    """

    # Define the transformation parameters
    pixel_width = 0.25
    rot_x = 0.0  # Rotation and shear parameter in X direction
    rot_y = 0.0  # Rotation and shear parameter in Y direction
    pixel_height = (
        -0.25
    )  # Pixel height (negative because of the raster's coordinate system)
    x_coordinate = 66.375  # X-coordinate of the top-left corner of the raster
    y_coordinate = 38.625  # Y-coordinate of the top-left corner of the raster

    # Create an Affine transformation object
    new_transform = Affine(
        pixel_width, rot_x, x_coordinate, rot_y, pixel_height, y_coordinate
    )

    with rasterio.open(tif_filename, "r+") as raster:
        raster.crs = CRS.from_epsg(4326)
        raster_array = raster.read(1)

        nan_mask = np.isnan(raster_array)
        raster_array[nan_mask] = -999

        raster.nodata = -999

        raster.transform = new_transform

    meta = raster.meta
    meta["transform"] = raster.transform
    reversed_data = np.flipud(raster_array)

    with rasterio.open(
        tif_filename.replace(".tif", "_flipped.tif"), "w", **meta
    ) as dst:
        dst.write(reversed_data, 1)

    # Resample from 0.25° to 0.01° using sum, then divide by 625

    flipped_tif = tif_filename.replace(".tif", "_flipped.tif")
    output_tif = tif_filename.replace(".tif", "_resampled.tif")

    with rasterio.open(flipped_tif) as src:
        new_res = 0.01
        new_width = int(round((src.bounds.right - src.bounds.left) / new_res))
        new_height = int(round((src.bounds.top - src.bounds.bottom) / new_res))
        new_transform = from_bounds(
            src.bounds.left, src.bounds.bottom,
            src.bounds.right, src.bounds.top,
            new_width, new_height,
        )

        dst_array = np.empty((new_height, new_width), dtype=np.float64)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst_array,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=new_transform,
            dst_crs=src.crs,
            resampling=Resampling.sum,
        )

        # divide by 625 to maintain overall sum_rainfall
        nodata = src.nodata if src.nodata is not None else -999
        dst_array = np.where(dst_array == nodata, nodata, dst_array / 625.0)

        out_meta = src.meta.copy()
        out_meta.update(
            width=new_width,
            height=new_height,
            transform=new_transform,
            dtype=np.float64,
            compress="deflate",
            nodata=nodata,
        )

    with rasterio.open(output_tif, "w", **out_meta) as dst:
        dst.write(dst_array, 1)


def parse_and_format_data(year: int, start_date: str, end_date: str):
    """
    Parses the year wise data in the DATA_FOLDER and formats to required type
    """
    current_year = datetime.now().year
    if year == current_year:
        data = imd.open_real_data(
            var_type="rain",
            start_dy=start_date,
            end_dy=end_date,
            file_dir=DATA_FOLDER,
        )
    else:
        data = imd.open_data(
            var_type="rain",
            start_yr=year,
            end_yr=year,
            fn_format="yearwise",
            file_dir=DATA_FOLDER,
        )

    dataset = data.get_xarray()


    dataset = dataset.where(dataset["rain"] != -999.0)
    # Group the dataset by month
    dataset = dataset.groupby("time.month")

  
    os.makedirs(TIFF_DATA_FOLDER, exist_ok=True)

    # For each month in the dataset, save the total rain in tif format
    for el in dataset:
        month = el[1]["time.month"].to_dict()["data"][0]
        if month < 10:
            month_wise_tif_filename = TIFF_DATA_FOLDER + "/{}_0{}.tif".format(
                year, month
            )
        else:
            month_wise_tif_filename = TIFF_DATA_FOLDER + "/{}_{}.tif".format(
                year, month
            )

        el[1]["rain"].sum("time").rio.to_raster(month_wise_tif_filename)

        transform_resample_monthly_tif_filenames(month_wise_tif_filename)

    # Save yearwise file as geotiff, this is used in getting crs
    data.to_geotiff("{}.tif".format(year), TIFF_DATA_FOLDER)

    return None


def retrieve_subdistrict_data(year: int, ADMIN_BDRY_GDF: str):
    """
    Retrives subdistrict data from the year wise .tif file
    """
    for month in [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]:
        month_and_year_filename = "{}_{}".format(str(year), str(month))
        try:
            raster = rasterio.open(
                os.path.join(
                    TIFF_DATA_FOLDER,
                    "{}_resampled.tif".format(month_and_year_filename),
                )
            )
            print(f"Processing for {month_and_year_filename}")
        except rasterio.errors.RasterioIOError:
            print(f"Skipping for {month_and_year_filename} - File Not Found!!")
            continue

        raster_array = raster.read(1)

        mean_dicts = rasterstats.zonal_stats(
            ADMIN_BDRY_GDF.to_crs(raster.crs),
            raster_array,
            affine=raster.transform,
            stats=["count", "mean", "sum", "max"],
            nodata=raster.nodata,
            geojson_out=True,
        )

        dfs = []

        for subdistrict in mean_dicts:
            dfs.append(pd.DataFrame([subdistrict["properties"]]))

        zonal_stats_df = pd.concat(dfs).reset_index(drop=True)

        os.makedirs(CSV_DATA_FOLDER, exist_ok=True)

        zonal_stats_df.to_csv(
            CSV_DATA_FOLDER + "/{}.csv".format(month_and_year_filename), index=False
        )

    return None





if __name__ == "__main__":

    # Takes year as an input from the cli
    year = str(sys.argv[1])
    year = int(year)

    # IF the year is current year, specify start and end date
    start_date = "2025-01-01"
    end_date = "2025-06-30"

    download_data(year, start_date=start_date, end_date=end_date)
    parse_and_format_data(year, start_date=start_date, end_date=end_date)
    retrieve_subdistrict_data(year)
