import logging
import boto3
import time
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import date, datetime, timezone, timedelta

import xarray as xr
import pandas as pd
import numpy as np

from config import S3_BUCKET_URL, RUN_HOUR, VARIABLE_MAP, setup_logging

setup_logging()


def build_grib_file_path(forecast_hour: int, for_idx: bool = False) -> str:
    """
    Generates the grib file path for a given run date and forecast hour.
    For example, for the file: hrrr.20250101/conus/hrrr.t06z.wrfsfcf00.grib2
    Year: 2025
    Month: 01
    Day: 01
    Region: conus
    CC (cycle run): 06
    Variable: wrfsfcf
    FH (forecast hour): 48
    """
    product = "wrfsfcf"
    file_path = f"hrrr.t{RUN_HOUR:02}z.{product}{forecast_hour:02}.grib2"
    suffix = ".idx" if for_idx else ""
    return f"{file_path}{suffix}"


def get_grib_s3_key(run_date: date, forecast_hour: int, for_idx: bool = False) -> str:
    sector = "conus"
    file_path = build_grib_file_path(forecast_hour, for_idx)
    return f"hrrr.{run_date:%Y%m%d}/{sector}/{file_path}"


def get_grib_s3_uri(run_date: date, forecast_hour: int, for_idx: bool = False) -> str:
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
    key = get_grib_s3_key(run_date, forecast_hour, for_idx)
    url = f"s3://{S3_BUCKET_URL}.s3.amazonaws.com/{key}"
    return url


def read_points(filepath: str) -> list[dict]:
    """Reads lat/lon points from a text file."""
    points = []
    try:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    lat, lon = map(float, line.split(","))
                    points.append({"latitude": lat, "longitude": lon})
                except ValueError:
                    logging.warning(f"Skipping invalid line in points file: {line}")
        if not points:
            raise ValueError(f"No valid points found in {filepath}")
        return points
    except FileNotFoundError:
        logging.error(f"Points file not found: {filepath}")
        raise


def timeit(name=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            label = name or func.__name__
            start = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start
            logging.info(f"[TIMER] {label} took {duration:.2f} seconds")
            return result

        return wrapper

    return decorator
