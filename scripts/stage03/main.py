import os
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


team = 18
warehouse = "project/hive/warehouse"  # location of your Hive database in HDFS

spark: SparkSession = (
    SparkSession.builder.appName("{} - spark ML".format(team))
    # .master("yarn")
    # hive
    # .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
    # .config("spark.sql.warehouse.dir", warehouse)
    # .config("spark.sql.catalogImplementation", "hive")
    # .config("spark.sql.avro.compression.codec", "snappy")
    # .enableHiveSupport()
    # driver
    # .config("spark.driver.cores", 2)
    # .config("spark.driver.memory", "2g")  # '2g'
    # .config("spark.driver.maxResultSize", "2g")  # '2g'
    # # executor
    # .config("spark.executor.cores", 5)
    # .config("spark.executor.memory", "4g")  # '3g'
    # .config("spark.executors.instances", 3)
    # .config("spark.executors.memoryOverhead", "1g")  # '1g'
    .config("spark.submit.deploymode", "client").config("spark.log.level", "WARN")
    # .config("spark.jars.packages", "ai.catboost:catboost-spark_3.5_2.12")
    .getOrCreate()
)


df = (
    spark.read.format("avro").option("avroCompressionCodec", "snappy").load("./0000*_0")
)


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
from pyspark.ml.param.shared import HasOutputCols, Param, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


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
        # ...
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
if False:
    lr_param_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.0, 0.01, 0.1, 1])
        .addGrid(lr.elasticNetParam, [0.0, 0.25, 0.5, 0.75, 1.0])
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
    print("regParam: ", best_model_lr.stages[-1]._java_obj.getRegParam())
    print("elasticNetParam: ", best_model_lr.stages[-1]._java_obj.getElasticNetParam())

    # Save the best model
    best_model_lr.write().overwrite().save("project/models/model1")

    # Predict and save results
    predictions_lr = best_model_lr.transform(test_df)
    predictions_lr.select("fare_amount", "prediction").repartition(1).write.mode(
        "overwrite"
    ).csv("project/output/model1_predictions")

    rmse_lr = rmse_evaluator.evaluate(predictions_lr)
    r2_lr = r2_evaluator.evaluate(predictions_lr)

    print("rmse: ", rmse_lr)
    print("r2: ", r2_lr)

#################
# Random Forest #
#################
if True:
    rf_param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [25])
        .addGrid(rf.maxDepth, [5, 7])
        # .addGrid(rf.numTrees, [25, 50, 75])
        # .addGrid(rf.maxDepth, [5, 7, 9])
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

    os.makedirs("project/output", exist_ok=True)
    with open("project/output/best_params_model2.txt", "w") as f:
        f.write("params:\n")
        for k, v in best_model_rf.stages[-1].extractParamMap().items():
            if k.name in {"numTrees", "maxDepth"}:
                f.write(f"{k.name}: {v}\n")

    # Save the best model
    best_model_rf.write().overwrite().save("project/models/model2")

    # Predict and save results
    predictions_rf = best_model_rf.transform(test_df)
    predictions_rf.select("fare_amount", "prediction").repartition(1).write.mode(
        "overwrite"
    ).csv("project/output/model2_predictions")

    rmse_lr = rmse_evaluator.evaluate(predictions_rf)
    r2_lr = r2_evaluator.evaluate(predictions_rf)

    with open("project/output/metrics2.txt", "w") as f:
        f.write(f"rmse: {rmse_lr}\n")
        f.write(f"r2: {r2_lr}\n")

    print("rmse: ", rmse_lr)
    print("r2: ", r2_lr)


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")


spark.stop()
