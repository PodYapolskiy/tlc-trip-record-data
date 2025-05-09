"""Module for calculating total number of records in each month and year"""

from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("q7")
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
            .agg(
                F.count("*").alias("total_records"),
            )
            .select(
                F.lit(year).alias("year"),
                F.lit(month).alias("month"),
                F.lit(datetime(year, month, 1)).alias("date"),
                F.col("total_records"),
            )
        )

        ops.append(op)


q7 = reduce(lambda x, y: x.union(y), ops)
q7.write.mode("overwrite").saveAsTable("team18_projectdb.q7_results")
q7.write.csv(
    "project/hive/eda/q7_result",
    mode="overwrite",
    header=False,
)

spark.stop()
