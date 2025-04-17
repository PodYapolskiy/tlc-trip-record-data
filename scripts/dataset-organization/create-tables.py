# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "argparse>=1.4.0",
#     "psycopg>=3.2.6",
# ]
# ///

import argparse
from pathlib import Path
from psycopg.sql import SQL
import psycopg


script_dir = Path(__file__).parent
sql_dir = script_dir.parent.parent / "sql"


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
        "--psql-create-schema",
        default=str(sql_dir / "create-tables-psql.sql"),
        type=str,
    )

    args = parser.parse_args()

    with open(args.psql_create_schema) as f:
        CREATE_SQL = SQL(f.read())

    print("connecting to postgres")
    with psycopg.connect(
        f"user={args.user} password={args.password} host={args.host} port={args.port} dbname={args.database}"
    ) as connection:
        print("creating table")
        with connection.cursor() as cursor:
            cursor.execute(CREATE_SQL)
            connection.commit()

    print("done!")
