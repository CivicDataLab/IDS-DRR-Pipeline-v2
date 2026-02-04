#!/usr/bin/env python3
import rasterstats
import geopandas as gpd
import pandas as pd
import argparse
import glob
import json
import os
import shutil
import time
import urllib.request
from datetime import datetime
import multiprocessing as mp
import rasterio
import requests
from bs4 import BeautifulSoup
import numpy as np
from PIL import Image
import PIL

# Increase PIL image size limit for large stitched images
PIL.Image.MAX_IMAGE_PIXELS = 933120000

# Optional imports with graceful degradation
try:
    from netCDF4 import Dataset
    NETCDF_AVAILABLE = True
except ImportError:
    NETCDF_AVAILABLE = False
    print("Warning: netCDF4 not installed. NetCDF output will be disabled.")

try:
    import xarray as xr
    import rioxarray
    RIOXARRAY_AVAILABLE = True
except ImportError:
    RIOXARRAY_AVAILABLE = False
    print("Warning: xarray/rioxarray not installed. GeoTIFF output will be disabled.")

try:
    from joblib import Parallel, delayed
    JOBLIB_AVAILABLE = True
except ImportError:
    JOBLIB_AVAILABLE = False
    print("Warning: joblib not installed. Parallel downloading disabled.")

try:
    import natsort
    NATSORT_AVAILABLE = True
except ImportError:
    NATSORT_AVAILABLE = False
    print("Warning: natsort not installed. Using standard sorting.")

try: 
    import rasterstats
    ZONAL_STATS_AVAILABLE = True
except ImportError:
    ZONAL_STATS_AVAILABLE = False
    print("rasterstats not installed. Zonal statistics will be disabled.")



# buvan_config.json file path


CONFIG_FILE = "bhuvan_config.json"

# Default state configurations with bounding boxes and codes
# Format: state_code is used in Bhuvan URL, dropdown_value is for Selenium
DEFAULT_STATE_CONFIG = {
    "Assam": {
        "code": "as",
        "dropdown_value": "id100_0",
        "dropdown_id": "minus100",
        "bbox": {
            "lat_south": 23.994140625,
            "lat_north": 28.125,
            "lon_west": 89.6923828125,  # 90 - 7 * delta
            "lon_east": 96.0
        },
        "delta": 0.0439453125
    },
    "Bihar": {
        "code": "br",
        "dropdown_value": "id101_0",
        "dropdown_id": "minus101",
        "bbox": {
            "lat_south": 24.0,
            "lat_north": 27.6,
            "lon_west": 83.0,
            "lon_east": 88.5
        },
        "delta": 0.0439453125
    },

    "Uttar Pradesh": {
        "code": "up",
        "dropdown_value": "id103_0",
        "dropdown_id": "minus103",
        "bbox": {
            "lat_south": 23.8,
            "lat_north": 30.5,
            "lon_west": 77.0,
            "lon_east": 84.7
        },
        "delta": 0.0439453125
    },
    "Odisha": {
        "code": "od",
        "dropdown_value": "id104_0",
        "dropdown_id": "minus104",
        "bbox": {
            "lat_south": 17.8,
            "lat_north": 22.6,
            "lon_west": 81.3,
            "lon_east": 87.5
        },
        "delta": 0.0439453125
    }
}

# bhuvan_config.json handling functions
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {"states": DEFAULT_STATE_CONFIG, "extracted_dates": {}}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"Configuration saved to {CONFIG_FILE}")


def get_state_config(state_name, config):
    states = config.get("states", {})
    
    # Case-insensitive lookup
    for name, cfg in states.items():
        if name.lower() == state_name.lower():
            return name, cfg
    
    return None, None


def add_state_config(state_name, code, dropdown_value, dropdown_id, bbox, delta=0.0439453125):
    config = load_config()
    config["states"][state_name] = {
        "code": code,
        "dropdown_value": dropdown_value,
        "dropdown_id": dropdown_id,
        "bbox": bbox,
        "delta": delta
    }
    save_config(config)



# The Bhuvan disaster page loads an iframe from this URL for the flood tool.
# This iframe HTML contains all the date listings per state under div elements.
BHUVAN_FLOOD_TOOL_URL = "https://bhuvan-app1.nrsc.gov.in/disaster/usrtasks/flood/flood.php"


def parse_date_text(date_text):
# this function parses date from the html response to make it usable in WMS requests
    date_text = date_text.strip()
    if not date_text:
        return None

    # Normalise: remove 'Hr', replace all separators with '_'
    date_text = date_text.replace('Hr', '').replace('hr', '').strip()
    date_text = date_text.replace('/', '_')
    date_text = date_text.replace(' ', '_')
    date_text = date_text.replace('-', '_')

    date_parts = [p.strip() for p in date_text.split('_') if p.strip()]


    first_is_year = len(date_parts) >= 1 and len(date_parts[0]) == 4 and date_parts[0].isdigit()

    if first_is_year:
        # NEW format: YYYY_MM_DD[_HH]  →  output YYYY_DD_MM[_HH]
        if len(date_parts) == 4:
            year, month, day, hour = date_parts
            return f'{year}_{day}_{month}_{hour}'
        elif len(date_parts) == 3:
            year, month, day = date_parts
            return f'{year}_{day}_{month}'
        return None
    else:
        # OLD format: DD_MM_YYYY[_HH]  or  DD1_DD2_MM_YYYY_HH (range)
        if len(date_parts) == 5:
            day_start, day_end, month, year, hour = date_parts
            return f'{year}_{day_start}-{day_end}_{month}_{hour}'
        elif len(date_parts) == 4:
            day, month, year, hour = date_parts
            return f'{year}_{day}_{month}_{hour}'
        elif len(date_parts) == 3:
            day, month, year = date_parts
            return f'{year}_{day}_{month}'
        return None


def normalise_date_string(date_string):

    # If it already has underscores, it's in the right format
    # (any hyphens are intentional range indicators like '03-04')
    if '_' in date_string:
        return date_string

    # No underscores → all-hyphen format, re-parse it
    parsed = parse_date_text(date_string)
    if parsed:
        return parsed

    # Last resort: just replace hyphens with underscores
    return date_string.replace('-', '_')


def fetch_dates_from_bhuvan(state_name, config):

    state_actual_name, state_cfg = get_state_config(state_name, config)
    if not state_cfg:
        print(f"Error: State '{state_name}' not found in configuration.")
        print("Available states:", list(config.get("states", {}).keys()))
        return []

    print(f"Fetching dates for {state_actual_name} from Bhuvan...")

    try:
        response = requests.get(BHUVAN_FLOOD_TOOL_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching Bhuvan page: {e}")
        return []

    soup = BeautifulSoup(response.text, "lxml")

    # find container by the state's dropdown_id
    dropdown_id = state_cfg.get("dropdown_id")
    date_divs = []

    if dropdown_id:
        container = soup.find(id=dropdown_id)
        if container:
            date_divs = container.find_all("div")

    # fallback using XPath /html/body/div[1]/div
    if not date_divs:
        body = soup.find("body")
        if body:
            first_div = body.find("div")
            if first_div:
                date_divs = first_div.find_all("div", recursive=False)

    # iterate each div's text into a date string
    dates = []
    for div in date_divs:
        raw = div.get_text(strip=True)
        parsed = parse_date_text(raw)
        if parsed:
            dates.append(parsed)

    print(f"Found {len(dates)} dates for {state_actual_name}")

    # Save dates to config
    if "extracted_dates" not in config:
        config["extracted_dates"] = {}
    config["extracted_dates"][state_actual_name] = {
        "dates": dates,
        "last_updated": datetime.now().isoformat()
    }
    save_config(config)

    return dates

#downloading tiles of the state

def is_valid_image(filepath):
    """Check if a file is a valid image (not an error XML/HTML response)."""
    try:
        # PNG files start with these magic bytes
        with open(filepath, 'rb') as f:
            header = f.read(8)
        # PNG signature: \x89PNG\r\n\x1a\n
        if header[:4] == b'\x89PNG':
            return True
        # Also accept JPEG, GIF etc just in case
        if header[:2] in (b'\xff\xd8', b'GIF8', b'BM'):
            return True
        return False
    except Exception:
        return False


def download_tile(bbox, date_string, state_code, output_dir):

    # this function downloads a single WMS tile from Bhuvan.

    url = (
        f"https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms?"
        f"LAYERS=flood%3A{state_code}_{date_string}"
        f"&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap"
        f"&STYLES=&FORMAT=image%2Fpng&SRS=EPSG%3A4326"
        f"&BBOX={bbox}&WIDTH=256&HEIGHT=256"
    )
    
    output_path = os.path.join(output_dir, f"{date_string}xx{bbox}.image")
    
    for attempt in range(2):
        try:
            urllib.request.urlretrieve(url, output_path)
            if is_valid_image(output_path):
                return None
            # bbhuvan returned an error response
            if attempt == 0:
                time.sleep(3) #not sure if this helps but trying again
                continue
            else:
                # Log the first 200 bytes for debugging on final failure
                with open(output_path, 'r', errors='replace') as f:
                    snippet = f.read(200)
                print(f"Warning: Invalid image for tile {bbox} (server returned: {snippet[:100]}...)")
                os.remove(output_path)
                return bbox
        except Exception as e:
            if attempt == 0:
                time.sleep(0.5)
            else:
                print(f"Failed to download tile {bbox}: {e}")
                if os.path.exists(output_path):
                    os.remove(output_path)
                return bbox
    return bbox


def generate_bboxes(lat_south, lat_north, lon_west, lon_east, delta):
    #bound box for tiles
    bboxes = []
    
    lt_s = lat_south
    while lt_s < lat_north:
        lt_n = lt_s + delta
        ln_w = lon_west
        
        while ln_w < lon_east:
            ln_e = ln_w + delta
            bbox = f"{ln_w},{lt_s},{ln_e},{lt_n}"
            bboxes.append(bbox)
            ln_w = ln_e
        
        lt_s = lt_n
    
    return bboxes


# image processing 
def remove_watermark_from_array(image_array):

    try:
        arr = image_array.copy()
        # Set grayscale pixels (R==G==B) to white
        mask = (arr[:, :, 0] == arr[:, :, 1]) & (arr[:, :, 1] == arr[:, :, 2])
        arr[:, :, :3][mask] = 255
        return arr
    except Exception:
        return image_array


def merge_images(file1, file2, horizontal):
# merges images together horizontally or vertically
    if isinstance(file1, Image.Image):
        image1 = file1
    else:
        image1 = Image.open(file1)
        try:
            image1_ar = remove_watermark_from_array(np.asarray(image1).copy())
            image1 = Image.fromarray(image1_ar)
        except Exception:
            pass
    
    image2 = Image.open(file2)
    try:
        image2_ar = remove_watermark_from_array(np.asarray(image2).copy())
        image2 = Image.fromarray(image2_ar)
    except Exception:
        pass
    
    width1, height1 = image1.size
    width2, height2 = image2.size
    
    if horizontal:
        result_width = width1 + width2
        result_height = max(height1, height2)
        result = Image.new('L', (result_width, result_height))
        result.paste(im=image1, box=(0, 0))
        result.paste(im=image2, box=(width1, 0))
    else:
        result_height = height1 + height2
        result_width = max(width1, width2)
        result = Image.new('L', (result_width, result_height))
        result.paste(im=image1, box=(0, 0))
        result.paste(im=image2, box=(0, height1))
    
    return result



# ouput
def create_netcdf(lat_south, lat_north, lon_west, lon_east, image_path, output_path):
 
    if not NETCDF_AVAILABLE:
        print("NetCDF4 not available, skipping NetCDF creation")
        return None
    
    try:
        image = Image.open(image_path)
        grayscale_array = np.asarray(image)
        
        lat_array = np.linspace(lat_north, lat_south, grayscale_array.shape[0])
        lon_array = np.linspace(lon_west, lon_east, grayscale_array.shape[1])
        
        nc_file = Dataset(output_path, 'w', format='NETCDF4')
        
        nc_file.createDimension('lat', grayscale_array.shape[0])
        nc_file.createDimension('lon', grayscale_array.shape[1])
        nc_file.createDimension('time', None)
        
        latitudes = nc_file.createVariable("lat", 'f4', ('lat',))
        longitudes = nc_file.createVariable("lon", 'f4', ('lon',))
        time_var = nc_file.createVariable('time', np.float32, ('time',))
        
        inundation = nc_file.createVariable("Inundation", np.uint8, ('time', 'lat', 'lon'))
        
        latitudes[:] = lat_array
        longitudes[:] = lon_array
        inundation[0, :, :] = grayscale_array
        
        nc_file.close()
        
        return output_path
    except Exception as e:
        print(f"Error creating NetCDF: {e}")
        return None


def create_geotiff(nc_path, output_path):
# convert NetCDF to GeoTIFF file
    if not RIOXARRAY_AVAILABLE:
        print("xarray/rioxarray not available, skipping GeoTIFF creation")
        return None
    
    try:
        nc_file = xr.open_dataset(nc_path)
        var = nc_file['Inundation']
        
        var = var.rio.set_spatial_dims('lon', 'lat')
        var.rio.set_crs("epsg:4326")
        var.rio.to_raster(output_path)
        
        nc_file.close()
        return output_path
    except Exception as e:
        print(f"Error creating GeoTIFF: {e}")
        return None


# MAIN 
def setup_directories(base_path):
    dirs = ['Tiles', 'vert', 'PNGs', 'NCs', 'tiffs']
    paths = {}
    
    for d in dirs:
        path = os.path.join(base_path, d)
        os.makedirs(path, exist_ok=True)
        paths[d] = path
    
    return paths


def process_date(date_string, state_name, state_cfg, base_path, paths):

    print(f"\n{'='*60}")
    print(f"Processing: {date_string} for {state_name}")
    print(f"{'='*60}")
    
    bbox_cfg = state_cfg["bbox"]
    delta = state_cfg["delta"]
    state_code = state_cfg["code"]
    
    lat_south = bbox_cfg["lat_south"]
    lat_north = bbox_cfg["lat_north"]
    lon_west = bbox_cfg["lon_west"]
    lon_east = bbox_cfg["lon_east"]
    
    # Generate all bounding boxes
    print("Generating tile grid...")
    bboxes = generate_bboxes(lat_south, lat_north, lon_west, lon_east, delta)
    
    # Calculate grid dimensions
    num_horizontal = int((lon_east - lon_west) / delta) + 1
    num_vertical = int((lat_north - lat_south) / delta) + 1
    
    print(f"Grid: {num_horizontal} x {num_vertical} = {len(bboxes)} tiles")
    
    # Show sample WMS URL for debugging
    sample_url = (
        f"https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms?"
        f"LAYERS=flood%3A{state_code}_{date_string}"
        f"&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap"
        f"&STYLES=&FORMAT=image%2Fpng&SRS=EPSG%3A4326"
        f"&BBOX={bboxes[0]}&WIDTH=256&HEIGHT=256"
    )
    print(f"WMS layer: flood:{state_code}_{date_string}")
    print(f"Sample URL: {sample_url}")
    
    # Download tiles
    print("Downloading tiles...")
    tic = time.perf_counter()
    
    if JOBLIB_AVAILABLE:
        Parallel(n_jobs=mp.cpu_count())(
            delayed(download_tile)(bbox, date_string, state_code, paths['Tiles'])
            for bbox in bboxes
        )
    else:
        for i, bbox in enumerate(bboxes):
            download_tile(bbox, date_string, state_code, paths['Tiles'])
            if (i + 1) % 100 == 0:
                print(f"  Downloaded {i + 1}/{len(bboxes)} tiles")
    
    toc = time.perf_counter()
    print(f"Tiles downloaded in {toc - tic:.2f} seconds")
    
    # Get downloaded tile files
    tile_files = glob.glob(os.path.join(paths['Tiles'], '*.image'))
    
    if len(tile_files) == 0:
        print("Error: No tiles downloaded!")
        return False
    
    # Validate tiles — remove any non-image files (error XMLs from WMS)
    valid_tiles = []
    invalid_count = 0
    for f in tile_files:
        if is_valid_image(f):
            valid_tiles.append(f)
        else:
            invalid_count += 1
            os.remove(f)
    
    if invalid_count > 0:
        print(f"Warning: Removed {invalid_count} invalid tiles (WMS error responses)")
    
    if len(valid_tiles) == 0:
        print("Error: All downloaded tiles are invalid!")
        print("This usually means the WMS layer name is wrong.")
        print(f"Check if layer 'flood:{state_code}_{date_string}' exists on Bhuvan.")
        shutil.rmtree(paths['Tiles'])
        os.makedirs(paths['Tiles'])
        return False
    
    tile_files = valid_tiles
    print(f"Valid tiles: {len(tile_files)}")
    
    # Extract lat/lon bounds from actual tiles
    lats = []
    lons = []
    for f in tile_files:
        parts = os.path.basename(f).split(',')
        if len(parts) >= 4:
            lats.append(float(parts[-1].split('.image')[0]))
            lons.append(float(parts[-2]))
    
    actual_lat_north = max(lats) if lats else lat_north
    actual_lon_east = max(lons) if lons else lon_east
    
    # Sort tiles
    tile_files = sorted(tile_files)
    
    # Stitch tiles vertically first, then horizontally
    print("Stitching tiles vertically...")
    tic = time.perf_counter()
    
    c = 0
    count = 1
    while c + num_vertical <= len(tile_files):
        vert_images = tile_files[c:c + num_vertical]
        vert_images.reverse()  # Reverse for correct orientation
        
        merged_image = vert_images[0]
        for image in vert_images[1:]:
            merged_image = merge_images(merged_image, image, horizontal=False)
        
        c += num_vertical
        merged_image.save(os.path.join(paths['vert'], f'{count}.png'))
        count += 1
    
    # Clean up tiles
    shutil.rmtree(paths['Tiles'])
    os.makedirs(paths['Tiles'])
    
    toc = time.perf_counter()
    print(f"Vertical stitching completed in {toc - tic:.2f} seconds")
    
    # Stitch horizontally
    print("Stitching tiles horizontally...")
    tic = time.perf_counter()
    
    vert_imgs = glob.glob(os.path.join(paths['vert'], '*.png'))
    if NATSORT_AVAILABLE:
        vert_imgs = natsort.natsorted(vert_imgs)
    else:
        vert_imgs = sorted(vert_imgs, key=lambda x: int(os.path.basename(x).split('.')[0]))
    
    if len(vert_imgs) == 0:
        print("Error: No vertical strips created!")
        return False
    
    merged_image = vert_imgs[0]
    for image in vert_imgs[1:]:
        merged_image = merge_images(merged_image, image, horizontal=True)
    
    toc = time.perf_counter()
    print(f"Horizontal stitching completed in {toc - tic:.2f} seconds")
    
    # Process final image (convert to binary mask)
    print("Processing final image...")
    tic = time.perf_counter()
    
    merged_array = np.asarray(merged_image).copy()
    # Convert to binary: 1 = inundated, 0 = not inundated
    merged_array[merged_array < 255] = 1
    merged_array[merged_array == 255] = 0
    
    merged_image = Image.fromarray(merged_array)
    png_path = os.path.join(paths['PNGs'], f'{date_string}.png')
    merged_image.save(png_path)
    
    # Clean up vertical strips
    shutil.rmtree(paths['vert'])
    os.makedirs(paths['vert'])
    
    toc = time.perf_counter()
    print(f"PNG saved in {toc - tic:.2f} seconds: {png_path}")
    
    # Create NetCDF
    print("Creating NetCDF...")
    tic = time.perf_counter()
    
    nc_path = os.path.join(paths['NCs'], f'{date_string}.nc')
    nc_result = create_netcdf(lat_south, actual_lat_north, lon_west, actual_lon_east, png_path, nc_path)
    
    toc = time.perf_counter()
    if nc_result:
        print(f"NetCDF saved in {toc - tic:.2f} seconds: {nc_path}")
    
    # Create GeoTIFF
    if nc_result:
        print("Creating GeoTIFF...")
        tic = time.perf_counter()
        
        tiff_path = os.path.join(paths['tiffs'], f'{date_string}.tif')
        tiff_result = create_geotiff(nc_path, tiff_path)
        
        toc = time.perf_counter()
        if tiff_result:
            print(f"GeoTIFF saved in {toc - tic:.2f} seconds: {tiff_path}")
        
        # Remove intermediate NetCDF
        if os.path.exists(nc_path):
            os.remove(nc_path)
    
    print(f"Completed processing for {date_string}")
    return True

def compute_monthly_zonal_stats(tiff_dir, paths, shapefile_path, year, month):

    if not ZONAL_STATS_AVAILABLE:
        print("Skipping zonal stats — rasterio/geopandas/rasterstats not installed.")
        return None

    # Collect TIFFs that match this year/month
    # Date format in filenames: YYYY_DD_MM_HH.tif or YYYY_DD_MM.tif
    # Month is the 3rd underscore-separated token.
    patterns = [
        os.path.join(tiff_dir, f"{year}_??_{month}*.tif"),
        os.path.join(tiff_dir, f"{year}_??-??_{month}*.tif"),
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))

    if not files:
        print(f"No TIFFs found for {year}-{month}, skipping zonal stats.")
        return None

    print(f"\nComputing zonal stats for {year}-{month} ({len(files)} TIFFs)...")
    tic = time.perf_counter()

    # Load shapefile
    try:
        zone_gdf = gpd.read_file(shapefile_path)
    except Exception as e:
        print(f"Error reading shapefile: {e}")
        return None

    # Stack rasters — sum all dates for the month
    raster = rasterio.open(files[0])
    raster_array = raster.read(1).astype(np.int32)

    for f in files[1:]:
        raster_array = raster_array + rasterio.open(f).read(1).astype(np.int32)

    # Save cumulative monthly raster
    meta = raster.meta.copy()
    meta.update(compress="deflate", count=1, dtype="int32", nodata=-1)
    stitched_path = os.path.join(paths['stitched_monthly'], f"stitched_{year}_{month}.tif")
    with rasterio.open(stitched_path, "w", **meta) as dst:
        dst.write(raster_array, 1)
    print(f"  Monthly raster saved: {stitched_path}")

    # ---- Zonal stat 1: inundation pixel count & percentage ----
    def count_nonzero(x):
        return np.count_nonzero(x.compressed())

    stats_dicts = rasterstats.zonal_stats(
        zone_gdf.to_crs(raster.crs),
        raster_array,
        affine=raster.transform,
        stats=["count"],
        nodata=-1,
        add_stats={"count_nonzero": count_nonzero},
        geojson_out=True,
    )

    dfs = [pd.DataFrame([feat["properties"]]) for feat in stats_dicts]
    zonal_df = pd.concat(dfs).reset_index(drop=True)
    zonal_df["inundation_pct"] = zonal_df["count_nonzero"] / zonal_df["count"]

    # ---- Zonal stat 2: intensity ----
    max_val = raster_array.max()
    if max_val > 0:
        intensity_array = np.divide(raster_array.astype(np.float64), max_val)
    else:
        intensity_array = raster_array.astype(np.float64)

    def nonzero_mean(x):
        x = x.compressed()
        nonzero = x[x != 0]
        return np.mean(nonzero) if len(nonzero) > 0 else 0.0

    intensity_dicts = rasterstats.zonal_stats(
        zone_gdf.to_crs(raster.crs),
        intensity_array,
        affine=raster.transform,
        stats=["mean", "sum"],
        nodata=-1,
        add_stats={"intensity_mean_nonzero": nonzero_mean},
        geojson_out=True,
    )

    idfs = [pd.DataFrame([feat["properties"]]) for feat in intensity_dicts]
    intensity_df = pd.concat(idfs).reset_index(drop=True)
    intensity_df.rename(columns={"mean": "intensity_mean", "sum": "intensity_sum"}, inplace=True)

    # Merge on object_id
    zonal_df = pd.merge(
        zonal_df,
        intensity_df[["intensity_mean", "intensity_mean_nonzero", "intensity_sum", "object_id"]],
        on="object_id",
    )

    # Select and rename final columns
    zonal_df = zonal_df[[
        "object_id", "count", "count_nonzero", "inundation_pct",
        "intensity_mean", "intensity_mean_nonzero", "intensity_sum",
    ]]
    zonal_df.columns = [
        "object_id",
        "count_bhuvan_pixels",
        "count_inundated_pixels",
        "inundation_pct",
        "inundation_intensity_mean",
        "inundation_intensity_mean_nonzero",
        "inundation_intensity_sum",
    ]

    csv_path = os.path.join(paths['csv'], f"inundation_pct_{year}_{month}.csv")
    zonal_df.to_csv(csv_path, index=False)

    toc = time.perf_counter()
    print(f"  CSV saved: {csv_path}  ({toc - tic:.2f}s)")
    return csv_path



def run_extraction(state_name, dates=None, output_dir=None, fetch_new_dates=False,
                   shapefile=None):

    config = load_config()
    
    state_actual_name, state_cfg = get_state_config(state_name, config)
    if not state_cfg:
        print(f"Error: State '{state_name}' not found in configuration.")
        print("Available states:", list(config.get("states", {}).keys()))
        return False
    
    print(f"\n{'='*60}")
    print(f"Bhuvan Inundation Extractor - {state_actual_name}")
    print(f"{'='*60}")
    
    # Set up output directory
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), 'bhuvan_data', state_actual_name.replace(' ', '_'))
    
    os.makedirs(output_dir, exist_ok=True)
    paths = setup_directories(output_dir)
    
    print(f"Output directory: {output_dir}")
    
    # Get dates to process
    if dates:
        date_list = dates
    elif fetch_new_dates:
        date_list = fetch_dates_from_bhuvan(state_actual_name, config)
    else:
        # Try to get cached dates
        cached_dates = config.get("extracted_dates", {}).get(state_actual_name, {}).get("dates", [])
        if cached_dates:
            print(f"Using {len(cached_dates)} cached dates")
            date_list = cached_dates
        else:
            print("No cached dates found. Fetching from Bhuvan...")
            date_list = fetch_dates_from_bhuvan(state_actual_name, config)
    
    if not date_list:
        print("Error: No dates to process!")
        return False
    
    # Normalise all dates: ensure underscores, handle any format
    normalised = []
    for d in date_list:
        n = normalise_date_string(d)
        normalised.append(n)
        if n != d:
            print(f"  Normalised date: {d} -> {n}")
    date_list = normalised
    
    print(f"\nDates to process ({len(date_list)}):")
    for d in date_list[:5]:
        print(f"  - {d}")
    if len(date_list) > 5:
        print(f"  ... and {len(date_list) - 5} more")
    
    # Process each date
    successful = 0
    failed = 0
    
    for date_string in date_list:
        try:
            if process_date(date_string, state_actual_name, state_cfg, output_dir, paths):
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Error processing {date_string}: {e}")
            failed += 1
    

    csv_files = []
    if shapefile and successful > 0:
        if not os.path.exists(shapefile):
            print(f"Warning: Shapefile not found: {shapefile}")
        else:
            # Determine which year/month combos are present in the TIFFs
            tiff_files = glob.glob(os.path.join(paths['tiffs'], '*.tif'))
            year_months = set()
            for tf in tiff_files:
                parts = os.path.basename(tf).replace('.tif', '').split('_')
                if len(parts) >= 3:
                    year_months.add((parts[0], parts[2]))  # (YYYY, MM)

            for year, month in sorted(year_months):
                csv_path = compute_monthly_zonal_stats(
                    paths['tiffs'], paths, shapefile, year, month
                )
                if csv_path:
                    csv_files.append(csv_path)
    elif shapefile is None and successful > 0:
        print("\nNote: No --shapefile provided, skipping zonal stats CSV generation.")
        print("  To generate CSVs, re-run with: --shapefile /path/to/zones.shp")
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Dates processed:{successful} successful, {failed} failed")
    print(f"TIFFs:{paths['tiffs']}")
    if csv_files:
        print(f"CSVs:{paths['csv']}")
        for c in csv_files:
            print(f"{os.path.basename(c)}")
    print(f"Output directory:{output_dir}")
    
    return successful > 0




def main():
    parser = argparse.ArgumentParser(
        description='Extract flood inundation data from ISRO Bhuvan platform',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --state "Assam"                    # Process Assam with cached/fetched dates
  %(prog)s --state "Assam" --fetch-dates      # Fetch fresh dates from Bhuvan
  %(prog)s --state "Bihar" --dates "2023_07_07_18,2023_09_07_18"
  %(prog)s --list-states                      # List available states
  %(prog)s --init-config                      # Create default config file
        """
    )
    
    parser.add_argument('--state', '-s', type=str, help='State name to process')
    parser.add_argument('--dates', '-d', type=str, help='Comma-separated list of dates (YYYY_DD_MM_HH)')
    parser.add_argument('--output', '-o', type=str, help='Output directory')
    parser.add_argument('--fetch-dates', '-f', action='store_true', help='Fetch fresh dates from Bhuvan')
    parser.add_argument('--list-states', '-l', action='store_true', help='List available states')
    parser.add_argument('--init-config', action='store_true', help='Initialize config file with defaults')
    parser.add_argument('--show-dates', action='store_true', help='Show cached dates for a state')
    parser.add_argument('--shapefile', type=str, help='Path to shapefile for zonal stats CSV output')
    
    args = parser.parse_args()
    
    # Initialize config
    if args.init_config:
        config = {"states": DEFAULT_STATE_CONFIG, "extracted_dates": {}}
        save_config(config)
        print("Config file initialized with default state configurations.")
        return
    
    config = load_config()
    
    # List states
    if args.list_states:
        print("\nAvailable states:")
        print("-" * 40)
        for name, cfg in config.get("states", {}).items():
            bbox = cfg.get("bbox", {})
            print(f"  {name}")
            print(f"    Code: {cfg.get('code', 'N/A')}")
            print(f"    Bbox: {bbox.get('lon_west', 'N/A')}, {bbox.get('lat_south', 'N/A')} to "
                  f"{bbox.get('lon_east', 'N/A')}, {bbox.get('lat_north', 'N/A')}")
            
            # Show cached dates count
            cached = config.get("extracted_dates", {}).get(name, {})
            if cached:
                print(f"    Cached dates: {len(cached.get('dates', []))} (updated: {cached.get('last_updated', 'N/A')})")
            print()
        return
    
    # Show cached dates
    if args.show_dates:
        if not args.state:
            print("Error: --state is required with --show-dates")
            return
        
        state_name, _ = get_state_config(args.state, config)
        if not state_name:
            print(f"Error: State '{args.state}' not found")
            return
        
        cached = config.get("extracted_dates", {}).get(state_name, {})
        if cached:
            print(f"\nCached dates for {state_name}:")
            print(f"Last updated: {cached.get('last_updated', 'N/A')}")
            print("-" * 40)
            for d in cached.get("dates", []):
                print(f"  {d}")
        else:
            print(f"No cached dates for {state_name}")
        return
    
    # Require state for processing
    if not args.state:
        parser.print_help()
        print("\nError: --state is required for processing")
        return
    
    # Parse dates if provided
    dates = None
    if args.dates:
        dates = [d.strip() for d in args.dates.split(',')]
    
    # Run extraction
    run_extraction(
        state_name=args.state,
        dates=dates,
        output_dir=args.output,
        fetch_new_dates=args.fetch_dates,
        shapefile=args.shapefile
    )


if __name__ == '__main__':
    main()