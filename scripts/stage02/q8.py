"""Module for creating view with day of the year column"""

from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("q8")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


ops = []

for year in range(2014, 2025):
    op = (
        spark.sql(
            f"""
            select * from team18_projectdb.green_tripdata_monthly
            where year = {year}
            """
        )
        .filter(F.year(F.col("lpep_pickup_datetime")) == year)
        .select(
            F.to_date(F.col("lpep_pickup_datetime")).alias("date"),
            F.col("total_amount"),
        )
        .groupBy(F.col("date"))
        .agg(
            F.sum("total_amount").alias("total_earnings"),
            F.avg("total_amount").alias("average_price"),
            F.count("*").alias("number_of_trips"),
        )
        .select(
            F.col("date"),
            F.dayofyear(F.col("date")).alias("day_of_year"),
            F.weekofyear(F.col("date")).alias("week_of_year"),
            F.col("total_earnings"),
            F.col("average_price"),
            F.col("number_of_trips"),
        )
    )

    ops.append(op)


q8 = reduce(lambda x, y: x.union(y), ops)
q8.write.mode("overwrite").saveAsTable("team18_projectdb.q8_results")
q8.write.csv(
    "project/hive/eda/q8_result",
    mode="overwrite",
    header=False,
)

spark.stop()
