"""
Stage 3
"""

import time
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.base import Transformer
from pyspark.sql import Row
from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.feature import (
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
# df = (
#     spark.read.format("avro")
#     # .option("avroCompressionCodec", "snappy")
#     .table("team18_projectdb.green_tripdata_monthly")
# )

df = spark.sql("SELECT * FROM team18_projectdb.green_tripdata_monthly").sample(
    fraction=0.01 * 0.1, seed=42
)

# Handle null for encoding
# Compute a safe placeholder (e.g., max value + 1)
max_value = df.agg({"ratecodeid": "max"}).collect()[0][0]
placeholder = max_value + 1 if max_value is not None else 0
df = df.fillna(
    placeholder, subset=["vendorid", "ratecodeid", "payment_type", "trip_type"]
)

#########
# SPLIT #
#########
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.write.mode("overwrite").json("project/data/train")
test_df.write.mode("overwrite").json("project/data/test")

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
gb = GBTRegressor(featuresCol="features", labelCol="fare_amount", seed=42)
gb_pipeline = Pipeline(
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
        # final assembler and model
        final_assembler,
        gb,
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
    saveMode="overwrite"
).csv("project/output/model1_predictions", header=True)
predictions_lr.write.mode("overwrite").saveAsTable(
    "team18_projectdb.linear_regression_prediction"
)

rmse_lr = rmse_evaluator.evaluate(predictions_lr)
r2_lr = r2_evaluator.evaluate(predictions_lr)

print("rmse: ", rmse_lr)
print("r2: ", r2_lr)

lr_metrics = Row(rmse=rmse_lr, r2=r2_lr)

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
predictions_rf.write.mode("overwrite").saveAsTable(
    "team18_projectdb.random_forest_prediction"
)

rmse_rf = rmse_evaluator.evaluate(predictions_rf)
r2_rf = r2_evaluator.evaluate(predictions_rf)

print("rmse: ", rmse_rf)
print("r2: ", r2_rf)

rf_metrics = Row(rmse=rmse_rf, r2=r2_rf)

###############################
# Gradient Boosting Regressor #
###############################
gb_param_grid = ParamGridBuilder().addGrid(gb.maxDepth, [3, 5, 7]).build()
cv = CrossValidator(
    estimator=gb_pipeline,
    estimatorParamMaps=gb_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(train_df)
best_model_gb = cv_model.bestModel

# save the best lr
best_model_gb.write().overwrite().save("project/models/model3")

# Predict and save results
predictions_gb = best_model_gb.transform(test_df)
predictions_gb.select("fare_amount", "prediction").write.mode("overwrite").csv(
    "project/output/model3_predictions", header=True
)
predictions_gb.write.mode("overwrite").saveAsTable(
    "team18_projectdb.gradient_boosting_prediction"
)

rmse_gb = rmse_evaluator.evaluate(predictions_gb)
r2_gb = r2_evaluator.evaluate(predictions_gb)

print("rmse: ", rmse_gb)
print("r2: ", r2_gb)

gb_metrics = Row(rmse=rmse_gb, r2=r2_gb)

##################
# COMPARE MODELS #
##################
model_lr = f"LinearRegression(regParam={best_model_lr.stages[-1]._java_obj.getRegParam()}, elasticNetParam={best_model_lr.stages[-1]._java_obj.getElasticNetParam()})"
model_rf = f"RandomForestRegressor(maxDepth={best_model_rf.stages[-1]._java_obj.getMaxDepth()}, numTrees={best_model_rf.stages[-1]._java_obj.getNumTrees()})"
model_gb = f"GradientBoostingRegressor(maxDepth={best_model_gb.stages[-1]._java_obj.getMaxDepth()})"

models = [
    [model_lr, rmse_lr, r2_lr],
    [model_rf, rmse_rf, r2_rf],
    [model_gb, rmse_gb, r2_gb],
]
models_df = spark.createDataFrame(models, ["model", "RMSE", "R2"])
models_df.show(truncate=False)

models_df.coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option(
    "header", "true"
).save("project/output/evaluation", format="csv")
models_df.write.mode("overwrite").saveAsTable("team18_projectdb.evaluation")

# models_df.write.mode("overwrite").saveAsTable("team18_projectdb.evaluation")
spark.createDataFrame([lr_metrics, rf_metrics, gb_metrics]).write.mode(
    "overwrite"
).saveAsTable("team18_projectdb.metrics")

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
