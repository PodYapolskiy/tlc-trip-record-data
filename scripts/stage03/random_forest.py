import time

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
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
preprocessed_train_df = spark.read.parquet("project/data/train_parquet")
preprocessed_test_df = spark.read.parquet("project/data/test_parquet")

rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="fare_amount")
r2_evaluator = RegressionEvaluator(metricName="r2", labelCol="fare_amount")

###########################
# Random Forest Regressor #
###########################
rf = RandomForestRegressor(featuresCol="features", labelCol="fare_amount", seed=42)
rf_param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [25, 50, 75])
    .addGrid(rf.maxDepth, [3, 4, 5])
    .build()
)
cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=rf_param_grid,
    evaluator=rmse_evaluator,
    numFolds=3,
)
cv_model = cv.fit(preprocessed_train_df)
best_model_rf = cv_model.bestModel

# Save the best model
best_model_rf.write().overwrite().save("project/models/model2")

# Predict and save results
predictions_rf = best_model_rf.transform(preprocessed_test_df)
(
    predictions_rf.select("fare_amount", "prediction")
    .repartition(1)
    .write.mode("overwrite")
    .csv("project/output/model2_predictions", header=True)
)
(
    predictions_rf.select("fare_amount", "prediction")
    .write.mode("overwrite")
    .saveAsTable("team18_projectdb.random_forest_prediction")
)

rmse_rf = rmse_evaluator.evaluate(predictions_rf)
r2_rf = r2_evaluator.evaluate(predictions_rf)

print("rmse: ", rmse_rf)
print("r2: ", r2_rf)

metrics_df = spark.createDataFrame(
    [
        (
            "RandomForestRegressor",
            rmse_rf,
            r2_rf,
            best_model_rf._java_obj.getNumTrees(),
            best_model_rf._java_obj.getMaxDepth(),
        )
    ],
    ["model", "RMSE", "R2", "numTree", "maxDepth"],
)
metrics_df.write.mode("overwrite").save("project/output/random_forest_metrics")
metrics_df.write.mode("overwrite").saveAsTable("team18_projectdb.random_forest_metrics")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time elapsed: {elapsed_time:.2f} seconds")

spark.stop()
