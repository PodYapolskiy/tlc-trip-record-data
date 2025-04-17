# `dataloader` scala project

This is a scala project with spark application for performing distributed data loading. Performs according to the following alorigthm:
1. Lists an input directory to find all parquet files
2. Spawn a spark job for each parquet file
3. In each job, it reads the parquet file, converts all column to correct data type, and partitions the data into year and month
  - Partitions are saved in `/user/team18/project/data/year={year:04d}/month={month:02d}/...`
4. Then it spawn a job to collect all partitoned data and load them into PostgreSQL

More specifically, the script creates a `Future` object for each parquet file. Inside each `Future` object, it defines a DAG of spark operations and waits for operation to finish. The script launches all the `Future` objects in parallel, and waits for all of them to finish. After that, it loads the generated partitioned data into PostgreSQL.

Script casts all columns to the correct data type, ingests year and month from filename into the dataframe and partitions data.

Data schema:
- `vendorid` - bigint
- `lpep_pickup_datetime` - timestamp
- `lpep_dropoff_datetime` - timestamp
- `store_and_fwd_flag` - string
- `ratecodeid` - bigint
- `pulocationid` - bigint
- `dolocationid` - bigint
- `passenger_count` - bigint
- `trip_distance` - double precision
- `fare_amount` - double precision
- `extra` - double precision
- `mta_tax` - double precision
- `tip_amount` - double precision
- `tolls_amount` - double precision
- `ehail_fee` - double precision
- `improvement_surcharge` - double precision
- `total_amount` - double precision
- `payment_type` - bigint
- `trip_type` - double precision
- `congestion_surcharge` - double precision
- `year` - int
- `month` - int

## Script Arguments

- `--host` - host for PostgreSQL
- `--port` - port for PostgreSQL
- `--user` - user for PostgreSQL
- `--password` - password for PostgreSQL
- `--database` - database for PostgreSQL
- `--table` - table name for PostgreSQL
- `--source` - source directory for dataset files
- `--merged` - directory for merged partitioned dataset files


## Script Execution

Compile the project using `sbt clean assembly` command. Then, run the following command to load the data into PostgreSQL:
```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class tlcdataloader.TlcDataLoader \
    target/scala-2.12/load-data-assembly-0.1.0.jar \
    --host localhost \
    --port 5432 \
    --username postgres \
    --password postgres \
    --database postgres \
    --table green_tripdata \
    --source "/user/team18/project/rawdata" \
    --merged "/user/team18/project/data"
```

