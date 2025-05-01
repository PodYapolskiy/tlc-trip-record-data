use team18_projectdb;

drop table if exists q3_results;

create table q3_results as
row format delimited fields terminated by ',' location 'project/hive/warehouse/q3_results' as
select 
    year,
    corr(
        cast(lpep_pickup_datetime as double)-cast(lpep_dropoff_datetime as double), -- seconds
        total_amount
    ) as corr_duration_price,
    corr(
        trip_distance,
        total_amount
    ) as corr_distance_price
from green_tripdata_partitioned_yearly
group by year;

SET hive.resultset.use.unique.column.names = false;
select * from q3_results;