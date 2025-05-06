# import cfgrib
# import boto3
# from botocore import UNSIGNED
# from botocore.client import Config
# import xarray as xr
# import tempfile
# import os

# # Attempt to create an S3 client without explicit credentials
# s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# bucket_name = "noaa-hrrr-bdp-pds"
# key = "hrrr.20250101/conus/hrrr.t06z.wrfsfcf00.grib2"

# try:
#     with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         temp_file_name = temp_file.name
#         print(f"Downloading GRIB file to: {temp_file_name}")

#         # Download the file from S3
#         s3_client.download_file(bucket_name, key, temp_file_name)

#         # Open the downloaded GRIB file with xarray using cfgrib engine
#         # ds = xr.open_dataset(temp_file_name, engine="cfgrib", backend_kwargs={"indexpath": ""})

#         # Use cfgrib to inspect the available keys and levels in the GRIB file
#         # grib_file = cfgrib.open_file(temp_file_name)

#         # # Print available coordinate information like 'typeOfLevel' and 'stepType'
#         # print("\n--- Available Coordinates and Keys ---")
#         # print(grib_file.coords)  # Coordinates (e.g., 'time', 'latitude', 'longitude', 'typeOfLevel', etc.)

#         # # Explore the unique levels available for 'typeOfLevel' and 'stepType'
#         # if "typeOfLevel" in grib_file.coords:
#         #     print("\n--- Unique 'typeOfLevel' values ---")
#         #     print(grib_file.coords["typeOfLevel"].values)

#         # if "stepType" in grib_file.coords:
#         #     print("\n--- Unique 'stepType' values ---")
#         #     print(grib_file.coords["stepType"].values)

#         # # After inspecting this, you will know what filters to apply.
#         # # For example, you might want to use 'typeOfLevel': 'surface' and 'stepType': 'instant'
#         # grib_file.close()  # Don't forget to close the file!

#         # # Cleanup
#         os.remove(temp_file_name)


# except Exception as e:
#     print(f"Error opening GRIB from S3 with boto3 and xarray: {e}")
#     print("Ensure your network is working and that there are no unexpected AWS credential configurations interfering.")


import cfgrib
import os

from config import VARIABLE_MAP

# Assuming the GRIB file is in the same folder as your script
file_name = "hrrr.t06z.wrfsfcf00.grib2"

# Get the absolute path for the file in the current directory
temp_file_name = os.path.join(os.getcwd(), file_name)


try:
    # Extract surface pressure (PRES) data
    grib_file = cfgrib.open_file(temp_file_name, filter_by_keys=VARIABLE_MAP["surface_roughness"])
    print(grib_file.variables["fsr"])
    # if "sr" in grib_file.variables:  # Checking for surface pressure variable
    #     surface_pressure = grib_file.variables["sr"]
    #     print("Surface Pressure Data:", surface_pressure)


except Exception as e:
    print(f"Error opening GRIB file: {e}")
