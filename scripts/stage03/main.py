"""
Stage 3
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.base import Transformer
from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)

start_time = time.time()


TEAM = 18
WAREHOUSE = "project/hive/warehouse"  # location of your Hive database in HDFS

spark: SparkSession = (
    SparkSession.builder.appName(f"{TEAM} - spark ML")
    .master("yarn")
    # hive
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.avro.compression.codec", "snappy")
    .enableHiveSupport()
    # driver
    # .config("spark.driver.cores", 2)
    # .config("spark.driver.memory", "2g")  # '2g'
    # .config("spark.driver.maxResultSize", "2g")  # '2g'
    # # executor
    # .config("spark.executor.cores", 5)
    # .config("spark.executor.memory", "4g")  # '3g'
    # .config("spark.executors.instances", 3)
    # .config("spark.executors.memoryOverhead", "1g")  # '1g'
    # .config("spark.submit.deploymode", "client")
    # .config("spark.log.level", "WARN")
    .getOrCreate()
)


# df = (
#     spark.read.format("avro").option("avroCompressionCodec", "snappy").load("./0000*_0")
# )
df = (
    spark.read.format("avro")
    # .option("avroCompressionCodec", "snappy")
    .table("team18_projectdb.green_tripdata_monthly")
)

# df = spark.sql("SELECT * FROM team18_projectdb.green_tripdata_monthly")

# df.show(1, vertical=True, truncate=False)
# df.groupBy("trip_type").count().show()
# spark.stop()
# import sys
# sys.exit(0)

#########
# SPLIT #
#########
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.repartition(1).write.mode("ignore").json("project/data/train")
test_df.repartition(1).write.mode("ignore").json("project/data/test")

#################
# PREPROCESSING #
#################
# categorical
vendor_indexer = StringIndexer(
    inputCol="vendorid", outputCol="vendor_index", handleInvalid="skip"
)
ratecode_indexer = StringIndexer(
    inputCol="ratecodeid", outputCol="ratecode_index", handleInvalid="skip"
)
payment_indexer = StringIndexer(
    inputCol="payment_type", outputCol="payment_index", handleInvalid="skip"
)
trip_indexer = StringIndexer(
    inputCol="trip_type", outputCol="trip_index", handleInvalid="skip"
)
encoder = OneHotEncoder(
    inputCols=["vendor_index", "ratecode_index", "payment_index", "trip_index"],
    outputCols=[
        "vendor_encoded",
        "ratecode_encoded",
        "payment_encoded",
        "trip_encoded",
    ],
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
            .withColumn("month_sin", F.sin(2 * F.pi() * F.col("month") / 12))
            .withColumn("month_cos", F.cos(2 * F.pi() * F.col("month") / 12))
            # .withColumn("month_day", F.dayofmonth("lpep_pickup_datetime"))
            # week day [1, 7]
            .withColumn("week_day", F.dayofweek("lpep_pickup_datetime"))
            .withColumn("week_day_sin", F.sin(2 * F.pi() * F.col("week_day") / 7))
            .withColumn("week_day_cos", F.cos(2 * F.pi() * F.col("week_day") / 7))
            # hour [0, 23]
            .withColumn("hour", F.hour("lpep_pickup_datetime"))
            .withColumn("hour_sin", F.sin(2 * F.pi() * F.col("hour") / 24))
            .withColumn("hour_cos", F.cos(2 * F.pi() * F.col("hour") / 24))
            # minute [0, 59]
            .withColumn("minute", F.minute("lpep_pickup_datetime"))
            .withColumn("minute_sin", F.sin(2 * F.pi() * F.col("minute") / 60))
            .withColumn("minute_cos", F.cos(2 * F.pi() * F.col("minute") / 60))
            # second [0, 59]
            .withColumn("second", F.second("lpep_pickup_datetime"))
            .withColumn("second_sin", F.sin(2 * F.pi() * F.col("second") / 60))
            .withColumn("second_cos", F.cos(2 * F.pi() * F.col("second") / 60))
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
        # "ehail_fee", # fals with error, to many nulls
        "improvement_surcharge",
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

lr = LinearRegression(
    featuresCol="features", labelCol="fare_amount", loss="squaredError", maxIter=500
)
lr_pipeline = Pipeline(
    stages=[
        # categorical
        vendor_indexer,
        ratecode_indexer,
        payment_indexer,
        trip_indexer,
        encoder,  # one hot encode
        categorical_assembler,
        # time
        date_time_trasformer,
        time_decomposer,
        # numerical
        num_assembler,
        scaler,
        # final assembler and model
        final_assembler,
        lr,
    ]
)
rf = RandomForestRegressor(featuresCol="features", labelCol="fare_amount", seed=42)
rf_pipeline = Pipeline(
    stages=[
        # categorical
        vendor_indexer,
        ratecode_indexer,
        payment_indexer,
        trip_indexer,
        encoder,  # one hot encode
        categorical_assembler,
        # time
        date_time_trasformer,
        time_decomposer,
        # numerical
        num_assembler,
        scaler,
        # final assembler and model
        final_assembler,
        rf,
    ]
)

rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="fare_amount")
r2_evaluator = RegressionEvaluator(metricName="r2", labelCol="fare_amount")

###########################
# Linear Regression Model #
###########################
lr_param_grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1, 1])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    # .addGrid(lr.epsilon, [1.10, 1.2, 1.3, 1.4, 1.5])  # use only with huber loss
    .build()
)
cv = CrossValidator(
    estimator=lr_pipeline,
    estimatorParamMaps=lr_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(train_df)
best_model_lr = cv_model.bestModel

# save the best lr
best_model_lr.write().overwrite().save("project/models/model1")

# Predict and save results
predictions_lr = best_model_lr.transform(test_df)
predictions_lr.select("fare_amount", "prediction").repartition(1).write.mode(
    "overwrite"
).csv("project/output/model1_predictions", header=True)

rmse_lr = rmse_evaluator.evaluate(predictions_lr)
r2_lr = r2_evaluator.evaluate(predictions_lr)

print("rmse: ", rmse_lr)
print("r2: ", r2_lr)

#################
# Random Forest #
#################
rf_param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [25, 50, 75])
    .addGrid(rf.maxDepth, [5, 7, 9])
    .build()
)
cv = CrossValidator(
    estimator=rf_pipeline,
    estimatorParamMaps=rf_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(train_df)
best_model_rf = cv_model.bestModel

# Save the best model
best_model_rf.write().overwrite().save("project/models/model2")

# Predict and save results
predictions_rf = best_model_rf.transform(test_df)
predictions_rf.select("fare_amount", "prediction").repartition(1).write.mode(
    "overwrite"
).csv("project/output/model2_predictions", header=True)

rmse_rf = rmse_evaluator.evaluate(predictions_rf)
r2_rf = r2_evaluator.evaluate(predictions_rf)

print("rmse: ", rmse_rf)
print("r2: ", r2_rf)

##################
# COMPARE MODELS #
##################
# rmse_lr, r2_lr = 5.0, 0.75
# best_model_lr = lr_pipeline.fit(train_df)

# rmse_rf, r2_rf = 4.4, 0.78
# best_model_rf = rf_pipeline.fit(train_df)

model_lr = f"LinearRegression(regParam={best_model_lr.stages[-1]._java_obj.getRegParam()}, elasticNetParam={best_model_lr.stages[-1]._java_obj.getElasticNetParam()})"  # f"LinearRegression(regParam={best_model_lr.stages[-1].extractParamMap()['regParam']}, elasticNetParam={best_model_lr.stages[-1].extractParamMap()['elasticNetParam']})"
model_rf = f"RandomForestRegressor(maxDepth={best_model_rf.stages[-1]._java_obj.getMaxDepth()}, numTrees={best_model_rf.stages[-1]._java_obj.getNumTrees()})"  # f"RandomForestRegressor(maxDepth={best_model_rf.stages[-1].maxDepth}, numTrees={best_model_rf.stages[-1].numTrees})"

models = [[model_lr, rmse_lr, r2_lr], [model_rf, rmse_rf, r2_rf]]
models_df = spark.createDataFrame(models, ["model", "RMSE", "R2"])
models_df.show(truncate=False)

models_df.coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option(
    "header", "true"
).save("project/output/evaluation", format="csv")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")

spark.stop()


# ⢀⡴⠑⡄⠀⠀⠀⠀⠀⠀⠀⣀⣀⣤⣤⣤⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
# ⠸⡇⠀⠿⡀⠀⠀⠀⣀⡴⢿⣿⣿⣿⣿⣿⣿⣿⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠑⢄⣠⠾⠁⣀⣄⡈⠙⣿⣿⣿⣿⣿⣿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⢀⡀⠁⠀⠀⠈⠙⠛⠂⠈⣿⣿⣿⣿⣿⠿⡿⢿⣆⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⢀⡾⣁⣀⠀⠴⠂⠙⣗⡀⠀⢻⣿⣿⠭⢤⣴⣦⣤⣹⠀⠀⠀⢀⢴⣶⣆
# ⠀⠀⢀⣾⣿⣿⣿⣷⣮⣽⣾⣿⣥⣴⣿⣿⡿⢂⠔⢚⡿⢿⣿⣦⣴⣾⠁⠸⣼⡿
# ⠀⢀⡞⠁⠙⠻⠿⠟⠉⠀⠛⢹⣿⣿⣿⣿⣿⣌⢤⣼⣿⣾⣿⡟⠉⠀⠀⠀⠀⠀
# ⠀⣾⣷⣶⠇⠀⠀⣤⣄⣀⡀⠈⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀
# ⠀⠉⠈⠉⠀⠀⢦⡈⢻⣿⣿⣿⣶⣶⣶⣶⣤⣽⡹⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠉⠲⣽⡻⢿⣿⣿⣿⣿⣿⣿⣷⣜⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣷⣶⣮⣭⣽⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⣀⣀⣈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠇⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠹⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠛⠻⠿⠿⠿⠿⠛⠉
