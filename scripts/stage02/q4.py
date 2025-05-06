from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("Q4")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


ops = []

for year in range(2014, 2025):
    for month in range(1, 13):
        op = spark.sql(
            f"""
                select * from team18_projectdb.green_tripdata_monthly
                where year = {year} and month = {month}
                """
        ).select(
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
            F.lit(datetime(year, month, 1)).alias("date"),
            (
                F.col("lpep_dropoff_datetime").cast("double")
                - F.col("lpep_pickup_datetime").cast("double")
            ).alias("duration"),
            F.col("trip_distance").alias("distance"),
            F.col("total_amount").alias("price"),
            F.col("passenger_count").alias("passenger_count"),
        )

        ops.append(op)


q4 = reduce(lambda x, y: x.union(y), ops)
q4.write.mode("overwrite").saveAsTable("team18_projectdb.q4_results")
q4.write.csv(
    "project/hive/eda/q4_result",
    mode="overwrite",
    header=False,
)

spark.stop()
