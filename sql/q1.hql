use team18_projectdb;

drop table if exists q1_results;

create table q1_results
row format delimited fields terminated by ',' location 'project/hive/warehouse/q1_results' as 
select
    total_rows.value - vendorid_nn.value as vendorid_null_count,
    (total_rows.value - vendorid_nn.value) / total_rows.value as vendorid_null_percent,
    total_rows.value - lpep_pickup_datetime_nn.value as lpep_pickup_datetime_null_count,
    (total_rows.value - lpep_pickup_datetime_nn.value) / total_rows.value as lpep_pickup_datetime_null_percent,
    total_rows.value - lpep_dropoff_datetime_nn.value as lpep_dropoff_datetime_null_count,
    (total_rows.value - lpep_dropoff_datetime_nn.value) / total_rows.value as lpep_dropoff_datetime_null_percent,
    total_rows.value - store_and_fwd_flag_nn.value as store_and_fwd_flag_null_count,
    (total_rows.value - store_and_fwd_flag_nn.value) / total_rows.value as store_and_fwd_flag_null_percent,
    total_rows.value - ratecodeid_nn.value as ratecodeid_null_count,
    (total_rows.value - ratecodeid_nn.value) / total_rows.value as ratecodeid_null_percent,
    total_rows.value - pulocationid_nn.value as pulocationid_null_count,
    (total_rows.value - pulocationid_nn.value) / total_rows.value as pulocationid_null_percent,
    total_rows.value - dolocationid_nn.value as dolocationid_null_count,
    (total_rows.value - dolocationid_nn.value) / total_rows.value as dolocationid_null_percent,
    total_rows.value - passenger_count_nn.value as passenger_count_null_count,
    (total_rows.value - passenger_count_nn.value) / total_rows.value as passenger_count_null_percent,
    total_rows.value - trip_distance_nn.value as trip_distance_null_count,
    (total_rows.value - trip_distance_nn.value) / total_rows.value as trip_distance_null_percent,
    total_rows.value - fare_amount_nn.value as fare_amount_null_count,
    (total_rows.value - fare_amount_nn.value) / total_rows.value as fare_amount_null_percent,
    total_rows.value - extra_nn.value as extra_null_count,
    (total_rows.value - extra_nn.value) / total_rows.value as extra_null_percent,
    total_rows.value - mta_tax_nn.value as mta_tax_null_count,
    (total_rows.value - mta_tax_nn.value) / total_rows.value as mta_tax_null_percent,
    total_rows.value - tip_amount_nn.value as tip_amount_null_count,
    (total_rows.value - tip_amount_nn.value) / total_rows.value as tip_amount_null_percent,
    total_rows.value - tolls_amount_nn.value as tolls_amount_null_count,
    (total_rows.value - tolls_amount_nn.value) / total_rows.value as tolls_amount_null_percent,
    total_rows.value - ehail_fee_nn.value as ehail_fee_null_count,
    (total_rows.value - ehail_fee_nn.value) / total_rows.value as ehail_fee_null_percent,
    total_rows.value - improvement_surcharge_nn.value as improvement_surcharge_null_count,
    (total_rows.value - improvement_surcharge_nn.value) / total_rows.value as improvement_surcharge_null_percent,
    total_rows.value - total_amount_nn.value as total_amount_null_count,
    (total_rows.value - total_amount_nn.value) / total_rows.value as total_amount_null_percent,
    total_rows.value - payment_type_nn.value as payment_type_null_count,
    (total_rows.value - payment_type_nn.value) / total_rows.value as payment_type_null_percent,
    total_rows.value - trip_type_nn.value as trip_type_null_count,
    (total_rows.value - trip_type_nn.value) / total_rows.value as trip_type_null_percent,
    total_rows.value - congestion_surcharge_nn.value as congestion_surcharge_null_count,
    (total_rows.value - congestion_surcharge_nn.value) / total_rows.value as congestion_surcharge_null_percent,
from -- use mutliple subqueries for better parallelism (for some reason hive query engine is not smart enough to do it by itself)
(
    select count(*) as value from green_tripdata_partitioned_monthly
) as total_rows,
(
    select count(vendorid) as value from green_tripdata_partitioned_monthly
) as vendorid_nn,
(
    select count(lpep_pickup_datetime) from green_tripdata_partitioned_monthly
) as lpep_pickup_datetime_nn,
(
    select count(lpep_dropoff_datetime) from green_tripdata_partitioned_monthly
) as lpep_dropoff_datetime_nn,
(
    select count(store_and_fwd_flag) from green_tripdata_partitioned_monthly
) as store_and_fwd_flag_nn,
(
    select count(ratecodeid) from green_tripdata_partitioned_monthly
) as ratecodeid_nn,
(
    select count(pulocationid) from green_tripdata_partitioned_monthly
) as pulocationid_nn,
(
    select count(dolocationid) from green_tripdata_partitioned_monthly
) as dolocationid_nn,
(
    select count(passenger_count) from green_tripdata_partitioned_monthly
) as passenger_count_nn,
(
    select count(trip_distance) from green_tripdata_partitioned_monthly
) as trip_distance_nn,
(
    select count(fare_amount) from green_tripdata_partitioned_monthly
) as fare_amount_nn,
(
    select count(extra) from green_tripdata_partitioned_monthly
) as extra_nn,
(
    select count(mta_tax) from green_tripdata_partitioned_monthly
) as mta_tax_nn,
(
    select count(tip_amount) from green_tripdata_partitioned_monthly
) as tip_amount_nn,
(
    select count(tolls_amount) from green_tripdata_partitioned_monthly
) as tolls_amount_nn,
(
    select count(ehail_fee) from green_tripdata_partitioned_monthly
) as ehail_fee_nn,
(
    select count(improvement_surcharge) from green_tripdata_partitioned_monthly
) as improvement_surcharge_nn,
(
    select count(total_amount) from green_tripdata_partitioned_monthly
) as total_amount_nn,
(
    select count(payment_type) from green_tripdata_partitioned_monthly
) as payment_type_nn,
(
    select count(trip_type) from green_tripdata_partitioned_monthly
) as trip_type_nn,
(
    select count(congestion_surcharge) from green_tripdata_partitioned_monthly
) as congestion_surcharge_nn;
-- select
--     count(*) - count(vendorid) as vendorid_null_values,
--     (count(*) - count(vendorid)) / count(*) as vendorid_null_values_percent,
--     count(*) - count(lpep_pickup_datetime) as lpep_pickup_datetime_null_values,
--     (count(*) - count(lpep_pickup_datetime)) / count(*) as lpep_pickup_datetime_null_values_percent,
--     count(*) - count(lpep_dropoff_datetime) as lpep_dropoff_datetime_null_values,
--     (count(*) - count(lpep_dropoff_datetime)) / count(*) as lpep_dropoff_datetime_null_values_percent,
--     count(*) - count(store_and_fwd_flag) as store_and_fwd_flag_null_values,
--     (count(*) - count(store_and_fwd_flag)) / count(*) as store_and_fwd_flag_null_values_percent,
--     count(*) - count(ratecodeid) as ratecodeid_null_values,
--     (count(*) - count(ratecodeid)) / count(*) as ratecodeid_null_values_percent,
--     count(*) - count(pulocationid) as pulocationid_null_values,
--     (count(*) - count(pulocationid)) / count(*) as pulocationid_null_values_percent,
--     count(*) - count(dolocationid) as dolocationid_null_values,
--     (count(*) - count(dolocationid)) / count(*) as dolocationid_null_values_percent,
--     count(*) - count(passenger_count) as passenger_count_null_values,
--     (count(*) - count(passenger_count)) / count(*) as passenger_count_null_values_percent,
--     count(*) - count(trip_distance) as trip_distance_null_values,
--     (count(*) - count(trip_distance)) / count(*) as trip_distance_null_values_percent,
--     count(*) - count(fare_amount) as fare_amount_null_values,
--     (count(*) - count(fare_amount)) / count(*) as fare_amount_null_values_percent,
--     count(*) - count(extra) as extra_null_values,
--     (count(*) - count(extra)) / count(*) as extra_null_values_percent,
--     count(*) - count(mta_tax) as mta_tax_null_values,
--     (count(*) - count(mta_tax)) / count(*) as mta_tax_null_values_percent,
--     count(*) - count(tip_amount) as tip_amount_null_values,
--     (count(*) - count(tip_amount)) / count(*) as tip_amount_null_values_percent,
--     count(*) - count(tolls_amount) as tolls_amount_null_values,
--     (count(*) - count(tolls_amount)) / count(*) as tolls_amount_null_values_percent,
--     count(*) - count(ehail_fee) as ehail_fee_null_values,
--     (count(*) - count(ehail_fee)) / count(*) as ehail_fee_null_values_percent,
--     count(*) - count(improvement_surcharge) as improvement_surcharge_null_values,
--     (count(*) - count(improvement_surcharge)) / count(*) as improvement_surcharge_null_values_percent,
--     count(*) - count(total_amount) as total_amount_null_values,
--     (count(*) - count(total_amount)) / count(*) as total_amount_null_values_percent,
--     count(*) - count(payment_type) as payment_type_null_values,
--     (count(*) - count(payment_type)) / count(*) as payment_type_null_values_percent,
--     count(*) - count(trip_type) as trip_type_null_values,
--     (count(*) - count(trip_type)) / count(*) as trip_type_null_values_percent,
--     count(*) - count(congestion_surcharge) as congestion_surcharge_null_values,
--     (count(*) - count(congestion_surcharge)) / count(*) as congestion_surcharge_null_values_percent
-- from green_tripdata_partitioned_monthly;

SET hive.resultset.use.unique.column.names = false;
select * from q1_results;