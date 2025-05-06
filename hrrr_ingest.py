import argparse
import logging

from config import ALL_VARIABLES, DEFAULT_NUM_HOURS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
        help=f"Number of hours of forecast data to ingest (0 to N-1). Defaults to {DEFAULT_NUM_HOURS}. This will be useful for testing so that you can work with smaller amounts of data.",
    )

    args = parser.parse_args()
    logging.info("Ingestion process finished.")


if __name__ == "__main__":
    main()
