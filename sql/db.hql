drop database if exists team18_projectdb cascade;

create database team18_projectdb location 'project/hive/warehouse';

use team18_projectdb;

set
    hive.exec.dynamic.partition = true;

set
    hive.exec.dynamic.partition.mode = nonstrict;

create external table
    green_tripdata (
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
stored as avro location 'project/warehouse/green_tripdata' tblproperties (
    'avro.schema.url' = 'project/warehouse/avsc/schema.avsc',
    'avro.compress' = 'snappy'
);

select
    count(*)
from
    green_tripdata;

create external table
    green_tripdata_monthly (
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
    ) partitioned by (year int, month int)
stored as avro location 'project/hive/warehouse/green_tripdata_monthly' tblproperties ('avro.compress' = 'snappy');

insert
    overwrite table green_tripdata_monthly partition (year, month)
select
    *
from
    green_tripdata;

drop table green_tripdata;

select
    count(*)
from
    green_tripdata_monthly
where
    year = 2014
    and month = 1;