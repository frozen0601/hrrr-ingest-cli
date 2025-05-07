import logging
import os


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


DB_FILE = "data.db"
TABLE_NAME = "hrrr_forecasts"
S3_BUCKET_URL = "noaa-hrrr-bdp-pds"
CACHE_DIR = os.path.join(os.getcwd(), ".cache", "hrrr")
CACHE_SIZE_LIMIT = 3 * 1024 * 1024 * 1024

RUN_HOUR = 6  # 06z forecast run
DEFAULT_NUM_HOURS = 48

# Mapping from user-friendly names to cfgrib filter keys
# Crosscheck between HRRR grib2 File Inventory, grib_ls, and GRIB Parameter database
VARIABLE_MAP = {
    "surface_pressure": {"shortName": "sp"},  # https://codes.ecmwf.int/grib/param-db/134
    "surface_roughness": {"shortName": "fsr"},  # https://codes.ecmwf.int/grib/param-db/244
    "visible_beam_downward_solar_flux": {"shortName": "vbdsf"},  # https://codes.ecmwf.int/grib/param-db/260346
    "visible_diffuse_downward_solar_flux": {"shortName": "vddsf"},  # https://codes.ecmwf.int/grib/param-db/260347
    "temperature_2m": {"shortName": "2t"},  # https://codes.ecmwf.int/grib/param-db/167
    "dewpoint_2m": {"shortName": "2d"},  # https://codes.ecmwf.int/grib/param-db/168
    "relative_humidity_2m": {"shortName": "2r"},  # https://codes.ecmwf.int/grib/param-db/260242
    "u_component_wind_10m": {"shortName": "10u"},  # https://codes.ecmwf.int/grib/param-db/165
    "v_component_wind_10m": {"shortName": "10v"},  # https://codes.ecmwf.int/grib/param-db/166
    "u_component_wind_80m": {"shortName": "u", "typeOfLevel": "heightAboveGround", "level": 80},
    "v_component_wind_80m": {"shortName": "v", "typeOfLevel": "heightAboveGround", "level": 80},
}

ALL_VARIABLES = list(VARIABLE_MAP.keys())
