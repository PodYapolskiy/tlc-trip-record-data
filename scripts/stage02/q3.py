""" Module for calculating correlation between:
- price and duration
- price and distance
- price and passenger count """

from functools import (reduce)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("Q3")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


q3 = reduce(
    lambda x, y: x.union(y),
    [
        spark.sql(
            f"""
            select * from team18_projectdb.green_tripdata_monthly
            where year = {year}
            """
        ).select(
            F.lit(year).alias("year"),
            F.corr(
                F.col("lpep_pickup_datetime").cast("double")
                - F.col("lpep_dropoff_datetime").cast("double"),
                "total_amount",
            ).alias("corr_duration_price"),
            F.corr("trip_distance", "total_amount").alias("corr_distance_price"),
            F.corr("passenger_count", "total_amount").alias("corr_passangers_price"),
        )
        for year in range(2014, 2025)
    ],
)

q3.write.mode("overwrite").saveAsTable("team18_projectdb.q3_results")

q3.write.csv(
    "project/hive/eda/q3_result",
    mode="overwrite",
    header=False,
)

spark.stop()
