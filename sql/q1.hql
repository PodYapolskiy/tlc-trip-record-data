use team18_projectdb;

drop table if exists q1_results;

create temporary table
    tmp_q1_results as
select
    count(*) - count(vendorid) as vendorid_null_values,
    (count(*) - count(vendorid)) / count(*) as vendorid_null_values_percent,
    count(*) - count(lpep_pickup_datetime) as lpep_pickup_datetime_null_values,
    (count(*) - count(lpep_pickup_datetime)) / count(*) as lpep_pickup_datetime_null_values_percent,
    count(*) - count(lpep_dropoff_datetime) as lpep_dropoff_datetime_null_values,
    (count(*) - count(lpep_dropoff_datetime)) / count(*) as lpep_dropoff_datetime_null_values_percent,
    count(*) - count(store_and_fwd_flag) as store_and_fwd_flag_null_values,
    (count(*) - count(store_and_fwd_flag)) / count(*) as store_and_fwd_flag_null_values_percent,
    count(*) - count(ratecodeid) as ratecodeid_null_values,
    (count(*) - count(ratecodeid)) / count(*) as ratecodeid_null_values_percent,
    count(*) - count(pulocationid) as pulocationid_null_values,
    (count(*) - count(pulocationid)) / count(*) as pulocationid_null_values_percent,
    count(*) - count(dolocationid) as dolocationid_null_values,
    (count(*) - count(dolocationid)) / count(*) as dolocationid_null_values_percent,
    count(*) - count(passenger_count) as passenger_count_null_values,
    (count(*) - count(passenger_count)) / count(*) as passenger_count_null_values_percent,
    count(*) - count(trip_distance) as trip_distance_null_values,
    (count(*) - count(trip_distance)) / count(*) as trip_distance_null_values_percent,
    count(*) - count(fare_amount) as fare_amount_null_values,
    (count(*) - count(fare_amount)) / count(*) as fare_amount_null_values_percent,
    count(*) - count(extra) as extra_null_values,
    (count(*) - count(extra)) / count(*) as extra_null_values_percent,
    count(*) - count(mta_tax) as mta_tax_null_values,
    (count(*) - count(mta_tax)) / count(*) as mta_tax_null_values_percent,
    count(*) - count(tip_amount) as tip_amount_null_values,
    (count(*) - count(tip_amount)) / count(*) as tip_amount_null_values_percent,
    count(*) - count(tolls_amount) as tolls_amount_null_values,
    (count(*) - count(tolls_amount)) / count(*) as tolls_amount_null_values_percent,
    count(*) - count(ehail_fee) as ehail_fee_null_values,
    (count(*) - count(ehail_fee)) / count(*) as ehail_fee_null_values_percent,
    count(*) - count(improvement_surcharge) as improvement_surcharge_null_values,
    (count(*) - count(improvement_surcharge)) / count(*) as improvement_surcharge_null_values_percent,
    count(*) - count(total_amount) as total_amount_null_values,
    (count(*) - count(total_amount)) / count(*) as total_amount_null_values_percent,
    count(*) - count(payment_type) as payment_type_null_values,
    (count(*) - count(payment_type)) / count(*) as payment_type_null_values_percent,
    count(*) - count(trip_type) as trip_type_null_values,
    (count(*) - count(trip_type)) / count(*) as trip_type_null_values_percent,
    count(*) - count(congestion_surcharge) as congestion_surcharge_null_values,
    (count(*) - count(congestion_surcharge)) / count(*) as congestion_surcharge_null_values_percent
from
    green_tripdata_monthly;

create table
    q1_results (
        column_name string,
        null_count bigint,
        null_percent double
    );

insert into
    q1_results (column_name, null_count, null_percent)
select
    'vendorid' as column_name,
    vendorid_null_values as null_count,
    vendorid_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'lpep_pickup_datetime' as column_name,
    lpep_pickup_datetime_null_values as null_count,
    lpep_pickup_datetime_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'lpep_dropoff_datetime' as column_name,
    lpep_dropoff_datetime_null_values as null_count,
    lpep_dropoff_datetime_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'store_and_fwd_flag' as column_name,
    store_and_fwd_flag_null_values as null_count,
    store_and_fwd_flag_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'ratecodeid' as column_name,
    ratecodeid_null_values as null_count,
    ratecodeid_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'pulocationid' as column_name,
    pulocationid_null_values as null_count,
    pulocationid_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'dolocationid' as column_name,
    dolocationid_null_values as null_count,
    dolocationid_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'passenger_count' as column_name,
    passenger_count_null_values as null_count,
    passenger_count_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'trip_distance' as column_name,
    trip_distance_null_values as null_count,
    trip_distance_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'fare_amount' as column_name,
    fare_amount_null_values as null_count,
    fare_amount_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'extra' as column_name,
    extra_null_values as null_count,
    extra_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'mta_tax' as column_name,
    mta_tax_null_values as null_count,
    mta_tax_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'tip_amount' as column_name,
    tip_amount_null_values as null_count,
    tip_amount_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'tolls_amount' as column_name,
    tolls_amount_null_values as null_count,
    tolls_amount_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'ehail_fee' as column_name,
    ehail_fee_null_values as null_count,
    ehail_fee_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'improvement_surcharge' as column_name,
    improvement_surcharge_null_values as null_count,
    improvement_surcharge_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'total_amount' as column_name,
    total_amount_null_values as null_count,
    total_amount_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'payment_type' as column_name,
    payment_type_null_values as null_count,
    payment_type_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'trip_type' as column_name,
    trip_type_null_values as null_count,
    trip_type_null_values_percent as null_percent
from
    tmp_q1_results;

insert into
    q1_results (column_name, null_count, null_percent)
select
    'congestion_surcharge' as column_name,
    congestion_surcharge_null_values as null_count,
    congestion_surcharge_null_values_percent as null_percent
from
    tmp_q1_results;

set
    hive.resultset.use.unique.column.names = false;

insert overwrite directory
    'project/hive/eda/q1_result'
row format delimited fields terminated by ','
select
    *
from
    q1_results
order by
    null_percent desc;