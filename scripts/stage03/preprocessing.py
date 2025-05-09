import time
import math
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.base import Transformer
from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.feature import (
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)

start_time = time.time()

spark: SparkSession = (
    SparkSession.builder.config(
        "hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883"
    )
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", "project/hive/warehouse")
    .config("spark.sql.avro.compression.codec", "snappy")
    .config("spark.log.level", "DEBUG")
    .config("spark.ui.port", "4050")
    .enableHiveSupport()
    .getOrCreate()
)


ops = []
for year in range(2021, 2025):
    for month in range(1, 13):
        op = spark.sql(
            f"""
            select * from team18_projectdb.green_tripdata_monthly
            where year = {year} and month = {month}
            """
        )
        ops.append(op)


df = reduce(lambda x, y: x.union(y), ops)
# df = spark.table("team18_projectdb.green_tripdata_monthly")

# df = df.sample(fraction=1, seed=42)

# todo: drop rows

# Handle null for encoding
df = df.fillna(
    value=99,  # stated in dataset description
    subset=["vendorid", "ratecodeid", "payment_type", "trip_type"],
)

#########
# SPLIT #
#########
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

#################
# PREPROCESSING #
#################
# categorical
encoder = OneHotEncoder(
    inputCols=["vendorid", "ratecodeid", "payment_type", "trip_type"],
    outputCols=[
        "vendor_encoded",
        "ratecode_encoded",
        "payment_encoded",
        "trip_encoded",
    ],
    handleInvalid="keep",
)
categorical_assembler = VectorAssembler(
    inputCols=[
        "vendor_encoded",
        "ratecode_encoded",
        "payment_encoded",
        "trip_encoded",
    ],
    outputCol="categorical_features",
    handleInvalid="skip",
)


# datetime
class DateTimeTransformer(
    Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable
):
    def _transform(self, df: DataFrame) -> DataFrame:
        df2: DataFrame = (
            df.withColumn("year", F.year("lpep_pickup_datetime"))
            # month [1, 12]
            .withColumn("month", F.month("lpep_pickup_datetime"))
            .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))
            .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))
            # .withColumn("month_day", F.dayofmonth("lpep_pickup_datetime"))
            # week day [1, 7]
            .withColumn("week_day", F.dayofweek("lpep_pickup_datetime"))
            .withColumn("week_day_sin", F.sin(2 * math.pi * F.col("week_day") / 7))
            .withColumn("week_day_cos", F.cos(2 * math.pi * F.col("week_day") / 7))
            # hour [0, 23]
            .withColumn("hour", F.hour("lpep_pickup_datetime"))
            .withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24))
            .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24))
            # minute [0, 59]
            .withColumn("minute", F.minute("lpep_pickup_datetime"))
            .withColumn("minute_sin", F.sin(2 * math.pi * F.col("minute") / 60))
            .withColumn("minute_cos", F.cos(2 * math.pi * F.col("minute") / 60))
            # second [0, 59]
            .withColumn("second", F.second("lpep_pickup_datetime"))
            .withColumn("second_sin", F.sin(2 * math.pi * F.col("second") / 60))
            .withColumn("second_cos", F.cos(2 * math.pi * F.col("second") / 60))
            # .withColumn(
            #     "trip_time",
            #     F.col("lpep_dropoff_datetime").cast("long")
            #     - F.col("lpep_pickup_datetime").cast("long"),
            # )
        )
        return df2.drop("month", "week_day", "hour", "minute", "second")


date_time_trasformer = DateTimeTransformer()
time_decomposer = VectorAssembler(
    inputCols=[
        "month_sin",
        "month_cos",
        "week_day_sin",
        "week_day_cos",
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        "second_sin",
        "second_cos",
    ],
    outputCol="datetime_features",
)

# numerical columns
num_assembler = VectorAssembler(
    inputCols=[
        # "year",
        "passenger_count",
        "trip_distance",
        # less informative
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
    ],
    outputCol="num_features",
    handleInvalid="skip",
)
scaler = StandardScaler(inputCol="num_features", outputCol="scaled_num_features")

# final and model
final_assembler = VectorAssembler(
    inputCols=[
        # categorical
        "categorical_features",
        # datetime
        "datetime_features",
        # numerical
        "scaled_num_features",
    ],
    outputCol="features",
)

pipeline = Pipeline(
    stages=[
        # categorical
        encoder,  # one hot encode
        categorical_assembler,
        # time
        date_time_trasformer,
        time_decomposer,
        # numerical
        num_assembler,
        scaler,
        # final assembler
        final_assembler,
    ]
)

pipeline_model = pipeline.fit(train_df)
preprocessed_train_df = pipeline_model.transform(train_df)
preprocessed_test_df = pipeline_model.transform(test_df)

########
# JSON #
########
(
    preprocessed_train_df.select("features", "fare_amount")
    .repartition(1)
    .write.mode("overwrite")
    .json("project/data/train")
)

(
    preprocessed_test_df.select("features", "fare_amount")
    .repartition(1)
    .write.mode("overwrite")
    .json("project/data/test")
)

###########
# Parquet #
###########
(
    preprocessed_train_df.select("features", "fare_amount")
    .repartition(1)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet("project/data/train_parquet/")
)

(
    preprocessed_test_df.select("features", "fare_amount")
    .repartition(1)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet("project/data/test_parquet/")
)


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")

spark.stop()
