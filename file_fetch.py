import os
from datetime import date
from utils import get_grib_s3_key, build_grib_file_path
from config import CACHE_DIR


os.makedirs(CACHE_DIR, exist_ok=True)


def download_s3_file(s3_client, bucket: str, key: str, local_path: str):
    s3_client.download_file(bucket, key, local_path)


def download_and_cache_file(s3_client, bucket: str, run_date: date, forecast_hour: int):
    date_folder = os.path.join(CACHE_DIR, f"{run_date:%Y%m%d}")
    os.makedirs(date_folder, exist_ok=True)
    cache_path = os.path.join(date_folder, build_grib_file_path(forecast_hour))

    if not os.path.exists(cache_path):
        print(f"Downloading {cache_path} from S3...")
        s3_key = get_grib_s3_key(run_date, forecast_hour)
        download_s3_file(s3_client, bucket, s3_key, cache_path)
    else:
        print(f"Using cached file: {cache_path}")

    return cache_path


def get_grib_file_path(s3_client, bucket: str, run_date: date, forecast_hour: int):
    """Wrapper to download and cache the GRIB file."""
    print(f"Downloading {run_date} forecast hour {forecast_hour} GRIB file from S3...")
    return download_and_cache_file(s3_client, bucket, run_date, forecast_hour)
