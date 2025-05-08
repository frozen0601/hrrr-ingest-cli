# HRRR Ingest CLI (`hrrr-ingest`)

## Table of Contents

* [Overview](#overview)
* [Features](#features)
* [Setup](#setup)

  * [1. Clone the repository](#1-clone-the-repository)
  * [2. Create and activate the Conda environment (**Recommended**)](#2-create-and-activate-the-conda-environment-recommended)
  * [3. Prepare your points file](#3-prepare-your-points-file)
* [Usage](#usage)
* [Configuration](#configuration)
* [Project Structure](#project-structure)
* [Notes & Known Issues](#notes--known-issues)
* [References](#references)

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

## Project Structure

```
hrrr-ingest-cli/
├── .cache/                 # Local cache for GRIB files (auto-created)
├── data.db                 # DuckDB database file (auto-created)
├── hrrr_ingest.py          # Main CLI script
├── config.py               # Configuration variables, VARIABLE_MAP
├── db_manager.py           # DuckDB connection and operations
├── file_fetch.py           # S3 file downloading and caching
├── hrrr_processor.py       # Core GRIB data extraction logic
├── utils.py                # Utility functions (paths, points reading, etc.)
├── environment.yml         # Python dependencies
├── points.txt              # Example input points file
└── README.md               # This file
```
---

## Notes & Known Issues

* **`num-hours` Interpretation:** The `--num-hours N` argument is interpreted as the *maximum forecast hour index* to include. For example, `--num-hours 3` will ingest data for forecast hours `f00, f01, f02, f03`.
* **GRIB File Parsing (`extract_data_from_grib`):**
    * It was observed during development that attempting to open the full HRRR GRIB2 files with `xarray.open_dataset` without specific `filter_by_keys` sometimes resulted in errors. To ensure reliable data extraction for each required variable, the current implementation opens the GRIB file separately for each variable using its specific filter keys. While this approach is less performant than a single file open, it proved more robust for these particular files and `cfgrib` behavior.
* **Nearest Point Selection (`find_nearest_point`):**
    * The built-in `xarray.Dataset.sel(..., method="nearest")` method encountered errors during development. As a workaround, a manual method for finding the nearest grid point based on Euclidean distance in latitude/longitude space is used. This is an approximation, especially for projected grids, but was necessary to proceed. Longitude handling assumes input points are `[-180, 180]` and converts them if the GRIB file's internal longitude representation differs (e.g. `[0,360]`).

---

## References
 - [Microsoft AI for Earth Data Sets](https://github.com/microsoft/AIforEarthDataSets/blob/main/data/noaa-hrrr.ipynb)
 - [HRRR grib2 File Inventory](https://www.nco.ncep.noaa.gov/pmb/products/hrrr/hrrr.t00z.wrfsfcf00.grib2.shtml)
 - [GRIB Parameter database](https://codes.ecmwf.int/grib/param-db/)


