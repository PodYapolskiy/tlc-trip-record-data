DROP DATABASE IF EXISTS team18_projectdb CASCADE;
CREATE DATABASE team18_projectdb LOCATION "project/hive/warehouse";
USE team18_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE green_tripdata (
    vendorid bigint,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    store_and_fwd_flag string,
    ratecodeid bigint,
    pulocationid bigint,
    dolocationid bigint,
    passenger_count bigint,
    trip_distance double,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    ehail_fee double,
    improvement_surcharge double,
    total_amount double,
    payment_type bigint,
    trip_type bigint,
    congestion_surcharge double,
    year int,
    month int
)
STORED AS AVRO
LOCATION 'project/warehouse/green_tripdata'
TBLPROPERTIES (
  'avro.schema.url'='project/warehouse/avsc/schema.avsc',
  'avro.compress'='snappy'
);

SELECT COUNT(*) FROM green_tripdata;



CREATE TABLE green_tripdata_partitioned_monthly (
    vendorid bigint,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    store_and_fwd_flag string,
    ratecodeid bigint,
    pulocationid bigint,
    dolocationid bigint,
    passenger_count bigint,
    trip_distance double,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    ehail_fee double,
    improvement_surcharge double,
    total_amount double,
    payment_type bigint,
    trip_type bigint,
    congestion_surcharge double
)
PARTITIONED BY (year int, month int);

INSERT OVERWRITE TABLE green_tripdata_partitioned_monthly PARTITION (year, month)
SELECT * from green_tripdata;


SELECT COUNT(*) FROM green_tripdata_partitioned_monthly where year=2014 and month=1;

CREATE TABLE green_tripdata_partitioned_yearly (
    vendorid bigint,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    store_and_fwd_flag string,
    ratecodeid bigint,
    pulocationid bigint,
    dolocationid bigint,
    passenger_count bigint,
    trip_distance double,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    ehail_fee double,
    improvement_surcharge double,
    total_amount double,
    payment_type bigint,
    trip_type bigint,
    congestion_surcharge double,
    month int
)
PARTITIONED BY (year int);

INSERT OVERWRITE TABLE green_tripdata_partitioned_yearly PARTITION (year)
SELECT * from green_tripdata;

SELECT COUNT(*) FROM green_tripdata_partitioned_monthly where year=2014;

DROP TABLE green_tripdata;