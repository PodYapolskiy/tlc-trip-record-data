""" Module for creating table with price, pickup time and drop-off time"""

from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("Q5")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


ops = []

for year in range(2014, 2025):
    for month in range(1, 13):
        op = (
            spark.sql(
                f"""
                select * from team18_projectdb.green_tripdata_monthly
                where year = {year} and month = {month}
                """
            )
            .filter(
                (F.col("lpep_pickup_datetime").isNotNull())
                & (F.col("lpep_dropoff_datetime").isNotNull())
                & (F.year(F.col("lpep_pickup_datetime")) == year)
                & (F.month(F.col("lpep_pickup_datetime")) == month)
                & (F.year(F.col("lpep_dropoff_datetime")) == year)
                & (F.month(F.col("lpep_dropoff_datetime")) == month)
            )
            .select(
                F.col("total_amount").alias("price"),
                F.hour(F.col("lpep_pickup_datetime")).alias("pickup_hour"),
                F.hour(F.col("lpep_dropoff_datetime")).alias("dropoff_hour"),
            )
        )

        ops.append(op)


q5 = reduce(lambda x, y: x.union(y), ops)
q5.write.mode("overwrite").saveAsTable("team18_projectdb.q5_results")
q5.write.csv(
    "project/hive/eda/q5_result",
    mode="overwrite",
    header=False,
)

spark.stop()
