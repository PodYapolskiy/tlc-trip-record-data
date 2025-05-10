import time
from pyspark.sql import SparkSession
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor

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

preprocessed_train_df = spark.read.parquet("project/data/train_parquet")
preprocessed_test_df = spark.read.parquet("project/data/test_parquet")

rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="fare_amount")
r2_evaluator = RegressionEvaluator(metricName="r2", labelCol="fare_amount")

gb = GBTRegressor(featuresCol="features", labelCol="fare_amount", seed=42, maxIter=50)
gb_param_grid = ParamGridBuilder().addGrid(gb.maxDepth, [2, 3, 4, 5, 7]).build()
cv = CrossValidator(
    estimator=gb,
    estimatorParamMaps=gb_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(preprocessed_train_df)
best_model_gb = cv_model.bestModel

# save the best lr
best_model_gb.write().overwrite().save("project/models/model3")

# Predict and save results
predictions_gb = best_model_gb.transform(preprocessed_test_df)
(
    predictions_gb.select("fare_amount", "prediction")
    .repartition(1)
    .write.mode("overwrite")
    .csv("project/output/model3_predictions", header=True)
)
(
    predictions_gb.select("fare_amount", "prediction")
    .write.mode("overwrite")
    .saveAsTable("team18_projectdb.gradient_boosting_prediction")
)

rmse_gb = rmse_evaluator.evaluate(predictions_gb)
r2_gb = r2_evaluator.evaluate(predictions_gb)

print("rmse: ", rmse_gb)
print("r2: ", r2_gb)

metrics_df = spark.createDataFrame(
    [
        (
            "GradientBoosting",
            rmse_gb,
            r2_gb,
            best_model_gb._java_obj.getMaxDepth(),
        )
    ],
    ["model", "RMSE", "R2", "maxDepth"],
)
metrics_df.write.mode("overwrite").save("project/output/gradient_boosting_metrics")
metrics_df.write.mode("overwrite").saveAsTable(
    "team18_projectdb.gradient_boosting_metrics"
)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")


spark.stop()
