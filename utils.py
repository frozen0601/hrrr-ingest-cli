import logging
from datetime import date
import xarray as xr
from config import S3_BUCKET_URL, RUN_HOUR, VARIABLE_MAP, setup_logging

setup_logging()


def get_grib2_uri(run_date_dt, forecast_hour, for_idx=False):
    """
    Generates the S3 path for a given run date and forecast hour.
    For example, for the file: hrrr.20250101/conus/hrrr.t06z.wrfsfcf00.grib2
    Year: 2025
    Month: 01
    Day: 01
    Region: conus
    CC (cycle run): 06
    Variable: wrfsfcf
    FH (forecast hour): 48
    """
    sector = "conus"
    product = "wrfsfcf"

    file_path = f"hrrr.t{RUN_HOUR:02}z.{product}{forecast_hour:02}.grib2"
    suffix = ".idx" if for_idx else ""
    url = f"{S3_BUCKET_URL}.s3.amazonaws.com/hrrr.{run_date_dt:%Y%m%d}/{sector}/{file_path}{suffix}"
    return url


def extract_data_from_grib(
    grib_file: str,
    target_points: list,  # list of {'latitude': lat, 'longitude': lon}
    variables_to_ingest: list,  # list of human-readable names like "surface_pressure", relative_humidity_2m, etc.
):
    """
    Opens a GRIB2 file and extracts specified variables for target points.
    Returns a list of dictionaries, each representing a row for the database.
    """

    index_cache = "cfgrib_index_cache"
    extracted_data = []

    # Loop through the variables to be extracted and prepare the filter for each
    for variable in variables_to_ingest:
        if variable not in VARIABLE_MAP:
            logging.warning(f"Variable '{variable}' is not defined in the VARIABLE_MAP.")
            continue

        filter_keys = VARIABLE_MAP[variable]
        try:
            # Open the GRIB2 dataset using xarray and cfgrib engine
            ds = xr.open_dataset(
                grib_file,
                engine="cfgrib",
                backend_kwargs={
                    "indexpath": index_cache,
                    "filter_by_keys": filter_keys,
                    "decode_timedelta": False,  # ToFutureWarning: In a future version of xarray decode_timedelta will default to False rather than None. To silence this warning, set decode_timedelta to True, False, or a 'CFTimedeltaCoder' instance.
                },
                indexpath=index_cache,
            )
            logging.info(f"Successfully opened: {grib_file} for {variable}")

            # Show information about the dataset (this will print all variables and metadata)
            print(ds)

            # Extract data for the specified target points (if any)
            for point in target_points:
                lat = point["latitude"]
                lon = point["longitude"]

                try:
                    # Extract data for the current variable at the given lat/lon point
                    data_point = ds[filter_keys].sel(latitude=lat, longitude=lon, method="nearest").values
                    extracted_data.append(
                        {"latitude": lat, "longitude": lon, "variable": variable, "value": data_point}
                    )
                except Exception as e:
                    logging.error(f"Error extracting {variable} at lat={lat}, lon={lon}: {e}")

        except Exception as e:
            logging.error(f"Error opening GRIB file for {variable}: {e}")
            continue

    return extracted_data


file_name = "hrrr.t06z.wrfsfcf00.grib2"
extract_data_from_grib(file_name, [], list(VARIABLE_MAP.keys()))
# extract_data_from_grib(file_name, [], ["surface_pressure"])
# 20250101
# import datetime

# test_date = datetime.date(2025, 1, 1)
# print(get_grib2_uri(test_date, 0))
