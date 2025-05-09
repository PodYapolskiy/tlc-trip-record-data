import time

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

start_time = time.time()

spark: SparkSession = (
    SparkSession.builder.appName("team18 - spark ML")
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", "project/hive/warehouse")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.avro.compression.codec", "snappy")
    .config("spark.log.level", "DEBUG")
    .config("spark.ui.port", "4050")
    .enableHiveSupport()
    .getOrCreate()
)

####################################
# Load preprocessed data from HDFS #
####################################
preprocessed_train_df = spark.read.parquet("project/data/train")
preprocessed_test_df = spark.read.parquet("project/data/test")

rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="fare_amount")
r2_evaluator = RegressionEvaluator(metricName="r2", labelCol="fare_amount")

###########################
# Linear Regression Model #
###########################
lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount",
    loss="squaredError",
    solver="l-bfgs",
    maxIter=200,
)
lr_param_grid = (
    ParamGridBuilder()
    # .addGrid(lr.regParam, [0.01, 0.1, 1])
    # .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])  # 0.0 is pure L2 and 1.0 is pure L1
    # .addGrid(lr.epsilon, [1.10, 1.2, 1.3, 1.4, 1.5])  # use only with huber loss
    .build()
)
cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=lr_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(preprocessed_train_df)
best_model_lr = cv_model.bestModel

# save the best lr
best_model_lr.write().overwrite().save("project/models/model1")

# Predict and save results
predictions_lr = best_model_lr.transform(preprocessed_test_df)
(
    predictions_lr.select("fare_amount", "prediction")
    .repartition(1)
    .write.mode(saveMode="overwrite")
    .csv("project/output/model1_predictions", header=True)
)
(
    predictions_lr.select("fare_amount", "prediction")
    .write.mode("overwrite")
    .saveAsTable("team18_projectdb.linear_regression_prediction")
)

###########
# Metrics #
###########
rmse_lr = rmse_evaluator.evaluate(predictions_lr)
r2_lr = r2_evaluator.evaluate(predictions_lr)

print("rmse: ", rmse_lr)
print("r2: ", r2_lr)

# same rmse_lr and r2_lr into hdfs
metrics_df = spark.createDataFrame(
    [
        (
            "LinearRegression",
            rmse_lr,
            r2_lr,
            best_model_lr._java_obj.getRegParam(),
            best_model_lr._java_obj.getElasticNetParam(),
        )
    ],
    ["model", "RMSE", "R2", "regParam", "elasticNetParam"],
)
metrics_df.write.mode("overwrite").saveAsTable(
    "team18_projectdb.linear_regression_metrics"
)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")

spark.stop()
