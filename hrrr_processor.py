import logging
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import date, datetime, timezone, timedelta

import xarray as xr
import pandas as pd
import numpy as np

from config import S3_BUCKET_URL, RUN_HOUR, VARIABLE_MAP, setup_logging
from utils import get_grib_s3_key, timeit

setup_logging()


def find_latest_complete_run_date(max_hours: int = 48) -> date:
    """Finds the most recent date with a complete 06z run (up to max_hours)."""
    logging.info(f"Searching for the latest complete run date (up to {max_hours} hours)...")
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    today = datetime.now(timezone.utc).date()
    for i in range(10):  # Check last 10 days
        check_date = today - timedelta(days=i)
        key = get_grib_s3_key(check_date, max_hours)

        try:
            s3_client.head_object(Bucket=S3_BUCKET_URL, Key=key)
            logging.info(f"Found latest complete run date: {check_date.strftime('%Y-%m-%d')}")
            return check_date
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logging.debug(f"Run date {check_date.strftime('%Y-%m-%d')} is incomplete or not available.")
            else:
                logging.error(f"Unexpected error when accessing S3: {e}")
                raise

    logging.error("Could not find a recent complete run date.")
    raise ValueError("Could not determine the latest available complete run date.")


@timeit()
def extract_data_from_grib(
    grib_file: str,
    source_s3: str,
    target_points: list,  # [{"latitude": float, "longitude": float}]
    variables_to_ingest: list,  # list of human-readable names like "surface_pressure", relative_humidity_2m, etc.
) -> pd.DataFrame:
    """
    Opens a GRIB2 file and extracts specified variables for target points.
    Returns a list of dictionaries, each representing a row for the database.
    """

    def find_nearest_point(ds, lat, lon):
        lon = lon if lon >= 0 else lon + 360  # wrap -180 - 180 to 0 - 360
        lats = ds.latitude.values
        lons = ds.longitude.values
        distance_sq = (lats - lat) ** 2 + (lons - lon) ** 2
        iy, ix = np.unravel_index(np.argmin(distance_sq), distance_sq.shape)
        return iy, ix

    extracted_data = []

    # Loop through the variables to be extracted and prepare the filter for each
    for var_name in variables_to_ingest:
        if var_name not in VARIABLE_MAP:
            logging.warning(f"Variable '{var_name}' is not defined in the VARIABLE_MAP.")
            continue

        filter_keys = VARIABLE_MAP[var_name]

        try:
            var_ds = xr.open_dataset(
                grib_file,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": filter_keys,
                    "decode_timedelta": False,  # ToFutureWarning: In a future version of xarray decode_timedelta will default to False rather than None. To silence this warning, set decode_timedelta to True, False, or a 'CFTimedeltaCoder' instance.
                },
            )
            data_var_name = list(var_ds.data_vars)[0]
            data_array = var_ds[data_var_name].values
            run_time_utc = pd.to_datetime(var_ds.time.values.item()).tz_localize("UTC")
            valid_time_utc = pd.to_datetime(var_ds.valid_time.values.item()).tz_localize("UTC")

            for point in target_points:
                lat, lon = point["latitude"], point["longitude"]

                try:
                    iy, ix = find_nearest_point(var_ds, lat, lon)
                    value = data_array[iy, ix]

                    extracted_data.append(
                        {
                            "valid_time_utc": valid_time_utc,
                            "run_time_utc": run_time_utc,
                            "latitude": var_ds.latitude.values[iy, ix],
                            "longitude": var_ds.longitude.values[iy, ix],
                            "variable": var_name,
                            "value": float(value),
                            "source_s3": source_s3,
                        }
                    )
                except Exception as e:
                    logging.error(f"Failed to extract value at ({lat}, {lon}) for {var_name}: {e}")

        except Exception as e:
            logging.error(f"Error opening GRIB file for {var_name}: {e}")
            continue

    return pd.DataFrame(extracted_data)
