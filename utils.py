import logging
from datetime import date
import xarray as xr
import numpy as np
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


def find_nearest_point(ds, lat, lon):
    lon = lon if lon >= 0 else lon + 360  # wrap around the globe
    lats = ds.latitude.values
    lons = ds.longitude.values
    distance_sq = (lats - lat) ** 2 + (lons - lon) ** 2
    iy, ix = np.unravel_index(np.argmin(distance_sq), distance_sq.shape)
    return iy, ix


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
            # print(ds)
            run_time = ds.time.values.item()
            valid_time = ds.valid_time.values.item()
            print(f"Run time: {run_time}, Valid time: {valid_time}")

            for point in target_points:
                lat = point[0]
                lon = point[1]

                try:
                    iy, ix = find_nearest_point(ds, lat, lon)
                    data_var_name = list(ds.data_vars)[0]
                    value = ds[data_var_name].values[iy, ix]
                    print(f"{variable} at ({lat}, {lon}) -> closest point: ({iy, ix}) -> {value}")
                except Exception as e:
                    logging.error(f"Error extracting {variable} at lat={lat}, lon={lon}: {e}")

        except Exception as e:
            logging.error(f"Error opening GRIB file for {variable}: {e}")
            continue

    return extracted_data


file_name = "hrrr.t06z.wrfsfcf00.grib2"
points = [
    (31.006900, -88.010300),
    (31.756900, -106.375000),
    (32.583889, -86.283060),
    (32.601700, -87.781100),
    (32.618900, -86.254800),
    (33.255300, -87.449500),
    (33.425878, -86.337550),
    (33.458665, -87.356820),
    (43.784500, -86.052400),
]

extract_data_from_grib(file_name, points, list(VARIABLE_MAP.keys()))
# extract_data_from_grib(file_name, points, ["surface_pressure"])
# 20250101
# import datetime

# test_date = datetime.date(2025, 1, 1)
# print(get_grib2_uri(test_date, 0))
