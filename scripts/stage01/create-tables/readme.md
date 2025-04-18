# `create-tables.py` script

- [`create-tables.py` script](#create-tablespy-script)
  - [Script Arguments](#script-arguments)
  - [Script Execution](#script-execution)
  - [Used Libraries](#used-libraries)


This is a very simple script, which loads SQL script from the provided path and executes this script against the database using provided credentials.

## Script Arguments

- `--host` - host for PostgreSQL
- `--port` - port for PostgreSQL
- `--user` - user for PostgreSQL
- `--password` - password for PostgreSQL
- `--database` - database for PostgreSQL
- `--psql-create-schema` - path to SQL script for creating tables

## Script Execution

```bash
uv run create-tables/create-tables.py \
    --host localhost \
    --port 5432 \
    --user postgres \
    --password postgres \
    --database postgres \
    --psql-create-schema sql/create-table-psql.sql
```

## Used Libraries

- [argparse](https://docs.python.org/3/library/argparse.html) - for parsing script arguments
- [psycopg](https://www.psycopg.org/) - for connecting to PostgreSQL