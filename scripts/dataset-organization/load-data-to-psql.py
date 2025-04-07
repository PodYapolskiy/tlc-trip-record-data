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
from psycopg.sql import SQL, Identifier
import polars as pl
import psycopg
from tqdm import tqdm


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
COPY_SQL = SQL("""
copy {} (
    vendorid,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    ratecodeid,
    pulocationid,
    dolocationid,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
) from stdin delimiter ',' csv;""")

CREATE_SQL = SQL("""
create table if not exists {} (
    id bigserial primary key,
    vendorid bigint,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    store_and_fwd_flag text,
    ratecodeid bigint,
    pulocationid bigint,
    dolocationid bigint,
    passenger_count bigint,
    trip_distance double precision,
    fare_amount double precision,
    extra double precision,
    mta_tax double precision,
    tip_amount double precision,
    tolls_amount double precision,
    ehail_fee double precision,
    improvement_surcharge double precision,
    total_amount double precision,
    payment_type bigint,
    trip_type double precision,
    congestion_surcharge double precision
);""")


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
    parser.add_argument(
        "--table",
        default="green_data",
        type=str,
        help="Table for PostgreSQL",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force to overwrite the table",
    )

    args = parser.parse_args()

    total = pl.scan_parquet(args.source_file).select(pl.len()).collect().item()

    temp = Path(mkdtemp())
    csv_path = temp / (args.table + ".csv")
    print(f"generating csv file {csv_path}")
    pl.scan_parquet(
        args.source_file,
        try_parse_hive_dates=True,
    ).rename(lambda x: x.lower()).select(COLUMN_ORDER).sink_csv(
        csv_path,
        include_header=False,
        include_bom=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
    )

    print("connecting to postgres")
    with psycopg.connect(
        f"user={args.user} password={args.password} host={args.host} port={args.port} dbname={args.database}"
    ) as connection:
        if args.force:
            print("dropping table")
            with connection.cursor() as cursor:
                cursor.execute(
                    SQL("drop table if exists {};").format(Identifier(args.table))
                )

        print("creating table")
        with connection.cursor() as cursor:
            cursor.execute(CREATE_SQL.format(Identifier(args.table)))
            connection.commit()

        print("copying data")
        with connection.cursor() as cursor, open(csv_path, "r") as f:
            cursor.execute("set datestyle to ISO, DMY;")

            with cursor.copy(COPY_SQL.format(Identifier(args.table))) as copy:
                for line in tqdm(f, total=total, desc="copying data"):
                    copy.write(line)

    print("done!")
    csv_path.unlink()
    temp.rmdir()
