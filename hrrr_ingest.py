import argparse
import logging
import sys
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError

from db_manager import get_db_connection, create_table_if_not_exists, insert_data
from utils import read_points, get_grib_s3_uri
from hrrr_processor import find_latest_complete_run_date, extract_data_from_grib
from file_fetch import get_grib_file_path
from config import ALL_VARIABLES, DEFAULT_NUM_HOURS, S3_BUCKET_URL, setup_logging


setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Ingest HRRR forecast data into DuckDB.")

    parser.add_argument("points_file", help="The path to the input data file (e.g., points.txt).")
    parser.add_argument(
        "--run-date",
        help="The forecast run date of the data to ingest. Defaults to the last available date with complete data.",
        default=None,  # Will be determined later if None
    )
    parser.add_argument(
        "--variables",
        help=f"A comma separated list of variables to ingest. The variables should be passed using the human-readable names listed above. Defaults to all supported variables. Available: {', '.join(ALL_VARIABLES)}",
        default=",".join(ALL_VARIABLES),
    )
    parser.add_argument(
        "--num-hours",
        type=int,
        default=DEFAULT_NUM_HOURS,
        help=f"Number of hours of forecast data to ingest (0 to 48). Defaults to {DEFAULT_NUM_HOURS}. This will be useful for testing so that you can work with smaller amounts of data.",
    )
    args = parser.parse_args()

    # --- Input Validation ---
    try:
        target_points = read_points(args.points_file)
    except Exception as e:
        logging.error(f"Error reading points file: {e}")
        sys.exit(1)

    variables_to_ingest = [var.strip() for var in args.variables.split(",")]
    invalid_vars = [v for v in variables_to_ingest if v not in ALL_VARIABLES]
    if invalid_vars:
        logging.error(f"Invalid variable names specified: {', '.join(invalid_vars)}")
        logging.info(f"Available variables: {', '.join(ALL_VARIABLES)}")
        sys.exit(1)

    if not (0 <= args.num_hours <= 48):
        logging.warning(
            f"Number of hours ({args.num_hours}) outside typical range (0-48). Defaulting to {DEFAULT_NUM_HOURS} hours."
        )
        args.num_hours = DEFAULT_NUM_HOURS

    run_date = None
    if args.run_date:
        try:
            run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("Invalid date format for --run-date. Please use YYYY-MM-DD.")
            sys.exit(1)
    else:
        try:
            # Find the latest date with complete data up to the requested num_hours
            run_date = find_latest_complete_run_date(max_hours=args.num_hours)
        except Exception as e:
            logging.error(f"Failed to determine latest run date: {e}")
            sys.exit(1)

    logging.info(f"Target run date: {run_date.strftime('%Y-%m-%d')}")
    logging.info(f"Variables to ingest: {', '.join(variables_to_ingest)}")
    logging.info(f"Number of forecast hours: {args.num_hours}")
    logging.info(f"Number of points: {len(target_points)}")

    # --- Database Setup ---
    try:
        con = get_db_connection()
        create_table_if_not_exists(con)
    except Exception as e:
        logging.error(f"Database connection or setup failed: {e}")
        sys.exit(1)

    # --- Ingestion Loop ---
    all_inserted_data = []
    logging.info("Starting ingestion...")

    # TODO: parellelize
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    for idx, forecast_hour in enumerate(range(args.num_hours + 1)):
        print(f"\rProcessing forecast hours: {idx}/{args.num_hours}", flush=True)
        grib_file_path = get_grib_file_path(s3_client, S3_BUCKET_URL, run_date, forecast_hour)
        new_data_df = extract_data_from_grib(
            grib_file=grib_file_path,
            source_s3=get_grib_s3_uri(run_date, forecast_hour),
            target_points=target_points,
            variables_to_ingest=variables_to_ingest,
        )

        if not new_data_df.empty:
            all_inserted_data.append(new_data_df)

    # --- Batch Insert ---
    if all_inserted_data:
        final_df = pd.concat(all_inserted_data, ignore_index=True)
        logging.info(f"Attempting to insert {len(final_df)} new records into the database...")
        try:
            insert_data(con, final_df)
            # Query to confirm how many were actually inserted (due to UNIQUE constraint)
            # This requires tracking counts or querying before/after, insert_data currently doesn't return count
            logging.info("Batch insertion completed.")
        except Exception as e:
            logging.error(f"Error during batch database insertion: {e}")
    else:
        logging.info("No new data found to insert.")

    # --- Cleanup ---
    con.close()
    logging.info("Ingestion process finished.")


if __name__ == "__main__":
    main()
