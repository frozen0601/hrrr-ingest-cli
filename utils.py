from config import S3_BUCKET_URL, RUN_HOUR, VARIABLE_MAP


def get_s3_path(run_date_dt, forecast_hour):
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
    url = f"{S3_BUCKET_URL}.s3.amazonaws.com/hrrr.{run_date_dt:%Y%m%d}/{sector}/{file_path}"
    return url
