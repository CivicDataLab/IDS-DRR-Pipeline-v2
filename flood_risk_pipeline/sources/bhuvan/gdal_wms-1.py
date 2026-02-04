import os
import subprocess
import timeit

from osgeo import gdal

gdal.DontUseExceptions()

path = os.getcwd() + "/Sources/BHUVAN/"

date_strings = [
    "2024_04_07_18",
    "2024_03-04_07_06",
    "2024_03_07_06",
    "2024_01_07_18",
    "2024_26_06_11",
    "2024_20_06_06",
    "2024_17_06_18",
    "2024_12_06_18",
    "2024_08_06_06",
    "2024_07_06_11",
    "2024_06_06_11",
    "2024_03_06_18",
    "2024_03_06_06",
    "2024_02_06_11",
    "2024_02_06_18",
    "2024_31_05_18",
]  # Sample date for assam - "2023_07_07_18"

# Specify the state information to scrape data for.
# state_info = {"state": "Assam", "code": "as"}


for dates in date_strings:

    # Define your input and output paths
    input_xml_path = path + "/data/inundation.xml"
    output_tiff_path = path + f"/data/tiffs/{dates}.tif"

    layer_as = "flood%3Aas_2023_07_07_18"
    bbox_as = "89.6922970,23.990548,96.0205936,28.1690311"
    url_cached = "https://bhuvan-ras2.nrsc.gov.in/mapcache"
    url_as = "https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms"

    # Download the WMS(Web Map Sevice) layer and save as XML.
    command = [
        "gdal_translate",
        "-of",
        "WMS",
        f"WMS:{url_as}?&LAYERS={layer_as}&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&FORMAT=image%2Fpng&SRS=EPSG%3A4326&BBOX={bbox_as}",
        f"{path}/data/inundation.xml",
    ]
    subprocess.run(command)

    # Specify the target resolution in the X and Y directions (50 meters)
    target_resolution_x = 0.00044915  # 0.0008983  # 0.0001716660336923202072
    target_resolution_y = -0.00044915  # -0.0008983  # -0.0001716684356881450775

    # Perform the warp operation using gdal.Warp()
    print("Warping Started")
    starttime = timeit.default_timer()

    gdal.Warp(
        output_tiff_path,
        input_xml_path,
        format="GTiff",
        xRes=target_resolution_x,
        yRes=target_resolution_y,
        creationOptions=["COMPRESS=DEFLATE", "TILED=YES"],
        callback=gdal.TermProgress,
    )

    print("Time took to Warp: ", timeit.default_timer() - starttime)
    print(f"Warping completed. Output saved to: {output_tiff_path}")
