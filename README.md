# HRRR Ingest CLI (`hrrr-ingest`)

## Overview

A command-line tool to ingest and transform data from the HRRR (High-Resolution Rapid Refresh) forecasting model published by NOAA. The tool fetches meteorological forecast data, extracts values at specified geographic points, and stores them in a local DuckDB database.

---

## Features

* ✅ **Fetch HRRR GRIB2 data** from NOAA's public AWS S3 bucket.
* ✅ **Parse meteorological variables** using `xarray` and the `cfgrib` engine.
* ✅ **Extract forecast data** for a user-defined set of lat/lon points.
* ✅ **Store results** in a local DuckDB database (`data.db`).
* ✅ **Prevent duplicate downloads** with local caching.
* ✅ **Avoid duplicate DB entries** using idempotent inserts.
* ✅ **Modular codebase** for config, database, processing, and utilities.

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/frozen0601/hrrr-ingest-cli.git
cd hrrr-ingest-cli
```

### 2. Create and activate the Conda environment (**Recommended**)

`cfgrib` depends on binary libraries (`eccodes`) that are most easily installed using Conda:

```bash
conda env create -f environment.yml
conda activate my-env
```

> 💡 Alternatively, you can install dependencies with `pip`, but you’ll need to install `eccodes` system-wide. See [ecCodes installation guide](https://confluence.ecmwf.int/display/ECC/ecCodes+installation) for help.

### 3. Prepare your points file

Create a plain text file (e.g., `points.txt`) with one `latitude,longitude` pair per line:

```
47.6062,-122.3321
34.0522,-118.2437
# Add as many as needed
```

---

## Usage

Run the CLI tool with:

```bash
python hrrr_ingest.py <points_file_path> [OPTIONS]
```

**Required Argument**

<points_file_path>: A text file with one latitude,longitude pair per line.

 **Available Options**
| Option        | Description                                                                        | Default                  |
| ------------- | ---------------------------------------------------------------------------------- | ------------------------ |
| `--run-date`  | Forecast run date to ingest, in `YYYY-MM-DD` format.                               | Latest available 06z run |
| `--variables` | Comma-separated list of meteorological variables to ingest (human-readable names). | All supported variables  |
| `--num-hours` | Number of forecast hours to ingest (e.g., 2 -> `f00` to `f02`).                    | 48                       |


---

## Configuration

The file `config.py` contains key parameters:

| Name                | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `DB_FILE`           | Output database file name (default: `data.db`)                   |
| `TABLE_NAME`        | Database table name (default: `hrrr_forecasts`)                  |
| `S3_BUCKET_URL`     | Public NOAA HRRR S3 bucket URL                                   |
| `CACHE_DIR`         | Directory for caching downloaded GRIB files                      |
| `RUN_HOUR`          | Fixed forecast cycle hour (default: `6` -> 06z run)              |
| `DEFAULT_NUM_HOURS` | Default forecast hours to ingest (e.g., `f00` to `f48`)          |
| `VARIABLE_MAP`      | Mapping of friendly variable names to GRIB keys used by `cfgrib` |

---

## Environment

Here is the Conda environment file (`environment.yml`) used in this project:

```yaml
name: my-env
channels:
  - conda-forge
dependencies:
  - python
  - numpy
  - duckdb
  - xarray  # work with labelled multi-dimensional arrays
  - cfgrib  # A Python interface to map GRIB files to the NetCDF Common Data Model following the CF Convention using ecCodes
  - eccodes # cfgrib wrapper for reading and writing GRIB files
  - boto3
  - requests
```

---

## Notes

* Tested using Python 3.10+ on Linux (WSL) with Conda.
* Make sure paths passed to the CLI use **Linux-style paths** if you're running in WSL (e.g., `/mnt/c/...`).
* Local cache is stored in `./.cache/hrrr/`.

---

## References
 - [Microsoft AI for Earth Data Sets](https://github.com/microsoft/AIforEarthDataSets/blob/main/data/noaa-hrrr.ipynb)
 - [HRRR grib2 File Inventory](https://www.nco.ncep.noaa.gov/pmb/products/hrrr/hrrr.t00z.wrfsfcf00.grib2.shtml)
 - [GRIB Parameter database](https://codes.ecmwf.int/grib/param-db/)


