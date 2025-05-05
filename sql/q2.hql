use team18_projectdb;

drop table if exists q2_results;

create table
    q2_results as
select
    invalid_count.value as invalid_count,
    invalid_count.value / total_count.value as invalid_percent
from
    (
        select
            count(*) as value
        from
            green_tripdata_monthly
        where
            lpep_pickup_datetime >= lpep_dropoff_datetime
            or pulocationid = dolocationid
            or passenger_count = 0
            or trip_distance = 0
            or total_amount = 0
    ) as invalid_count,
    (
        select
            count(*) as value
        from
            green_tripdata_monthly
    ) as total_count;

insert overwrite directory
    'project/hive/eda/q2_result'
row format delimited fields terminated by ','
select
    *
from
    q2_results;