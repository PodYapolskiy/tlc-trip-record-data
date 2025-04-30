USE team18_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


CREATE EXTERNAL TABLE green_tripdata_partitioned_monthly
PARTITIONED BY (year int, month int) AS
SELECT * FROM green_tripdata;


CREATE TABLE green_tripdata_partitioned_yearly
PARTITIONED BY (year int) AS
SELECT * FROM green_tripdata;

