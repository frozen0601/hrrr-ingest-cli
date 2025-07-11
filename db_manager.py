import duckdb
import pandas as pd
from config import DB_FILE, TABLE_NAME


def get_db_connection():
    """Establishes connection to the DuckDB database."""
    return duckdb.connect(database=DB_FILE, read_only=False)


def create_table_if_not_exists(con):
    """Creates the hrrr_forecasts table if it doesn't exist."""
    con.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                valid_time_utc TIMESTAMP,
                run_time_utc TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                variable VARCHAR,
                value FLOAT,
                source_s3 VARCHAR,
                UNIQUE (valid_time_utc, run_time_utc, latitude, longitude, variable)
            );
        """
    )


def insert_data(con, data_df):
    """Inserts data from a Pandas DataFrame into the table. Duplicates are skipped."""
    data_df["run_time_utc"] = pd.to_datetime(data_df["run_time_utc"])
    data_df["valid_time_utc"] = pd.to_datetime(data_df["valid_time_utc"])

    temp_table_name = "temp_hrrr_insert"
    con.register("df_temp", data_df)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table_name} AS SELECT * FROM df_temp")

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (valid_time_utc, run_time_utc, latitude, longitude, variable, value, source_s3)
        SELECT valid_time_utc, run_time_utc, latitude, longitude, variable, value, source_s3
        FROM {temp_table_name}
        ON CONFLICT (valid_time_utc, run_time_utc, latitude, longitude, variable) DO NOTHING;
    """
    con.execute(insert_sql)
    con.unregister("df_temp")
    con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
