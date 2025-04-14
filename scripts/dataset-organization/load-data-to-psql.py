# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "argparse>=1.4.0",
#     "polars>=1.26.0",
#     "psycopg>=3.2.6",
#     "tqdm>=4.67.1",
# ]
# ///

import argparse
from pathlib import Path
from tempfile import mkdtemp
from psycopg.sql import SQL
import polars as pl
import psycopg
from tqdm import tqdm

script_dir = Path(__file__).parent
sql_dir = script_dir.parent.parent / "sql"

with open(sql_dir / "create-table.sql") as f:
    CREATE_SQL = SQL(f.read())

with open(sql_dir / "copy-data.sql") as f:
    COPY_SQL = SQL(f.read())

COLUMN_ORDER = [
    "vendorid",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "store_and_fwd_flag",
    "ratecodeid",
    "pulocationid",
    "dolocationid",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "trip_type",
    "congestion_surcharge",
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data to PostgreSQL.")
    parser.add_argument(
        "--source-file",
        default="green_data.parquet",
        type=str,
        help="Source file path",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        type=str,
        help="Host for PostgreSQL",
    )
    parser.add_argument(
        "--port",
        default=5432,
        type=int,
        help="Port for PostgreSQL",
    )
    parser.add_argument(
        "--user",
        default="postgres",
        type=str,
        help="User for PostgreSQL",
    )
    parser.add_argument(
        "--password",
        default="postgres",
        type=str,
        help="Password for PostgreSQL",
    )
    parser.add_argument(
        "--database",
        default="postgres",
        type=str,
        help="Database for PostgreSQL",
    )

    args = parser.parse_args()

    total = pl.scan_parquet(args.source_file).select(pl.len()).collect().item()

    temp = Path(mkdtemp())
    csv_path = temp / "green_tripdata.csv"
    print(f"generating csv file {csv_path}")
    pl.scan_parquet(
        args.source_file,
        try_parse_hive_dates=True,
    ).rename(lambda x: x.lower()).select(COLUMN_ORDER).sink_csv(
        csv_path,
        include_header=False,
        include_bom=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
        separator="\t",
    )

    try:
        print("connecting to postgres")
        with psycopg.connect(
            f"user={args.user} password={args.password} host={args.host} port={args.port} dbname={args.database}"
        ) as connection:
            print("creating table")
            with connection.cursor() as cursor:
                cursor.execute(CREATE_SQL)
                connection.commit()

            print("copying data")
            with connection.cursor() as cursor, open(csv_path, "r") as f:
                cursor.execute("set datestyle to ISO, DMY;")

                with cursor.copy(COPY_SQL) as copy:
                    for line in tqdm(f, total=total, desc="copying data"):
                        copy.write_row(line)

        print("done!")
    except Exception as e:
        print(e)
    finally:
        csv_path.unlink()
        temp.rmdir()
