#!/bin/bash

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Identified scripts directory as $SCRIPTS"

. "$SCRIPTS/load-secrets.sh"
if [ $? -ne 0 ]; then
    exit 1
fi

bash "$SCRIPTS/prepare-bin.sh"
if [ $? -ne 0 ]; then
    exit 1
fi

BIN="$SCRIPTS/bin"

hdfs dfs -mkdir -p $HDFS_ROOT/project/output
hdfs dfs -mkdir -p $HDFS_ROOT/project/models

#################
# Preprocessing #
#################
echo "Submitting preprocessing job..."
spark-submit \
    --name "Team18 | Spark ML | Preprocessing" \
    --master yarn \
    --driver-memory 2g \
    --num-executors 3 \
    --executor-memory 4g \
    --executor-cores 5 \
    $SCRIPTS/stage03/preprocessing.py

# Check if preprocessing succeeded
if [ $? -ne 0 ]; then
  echo "Preprocessing failed. Aborting..."
  exit 1
fi

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/data/train output
hdfs dfs -copyToLocal -f $HDFS_ROOT/project/data/test output


#####################
# Linear Regression #
#####################
echo "Submitting Linear Regression job..."
spark-submit \
    --name "Team18 | Spark ML | Linear Regression" \
    --master yarn \
    --driver-memory 2g \
    --num-executors 3 \
    --executor-memory 4g \
    --executor-cores 5 \
    $SCRIPTS/stage03/linear_regression.py

if [ $? -ne 0 ]; then
  echo "Linear Regression failed. Aborting..."
  exit 1
fi

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/models/model1 models/
hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/model1_predictions output/

exit 0


###########################
# Random Forest Regressor #
###########################
# echo "Submitting Random Forest Regressor job..."
# spark-submit \
#     --name "Team18 | Spark ML | Random Forest Regressor" \
#     --master yarn \
#     --driver-memory 2g \
#     --num-executors 3 \
#     --executor-memory 4g \
#     --executor-cores 5 \ q
#     $SCRIPTS/stage03/random_forest.py

# if [ $? -ne 0 ]; then
#   echo "Random Forest failed. Aborting..."
#   exit 1
# fi

# hdfs dfs -copyToLocal -f $HDFS_ROOT/project/models/model2 models/
# hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/model2_predictions output/

# exit 0

###########################
# Catboost Regressor #
###########################
# echo "Submitting Boosting Regressor job..."
# spark-submit \
#     --master yarn \
#     --driver-memory 2g \
#     --num-executors 3 \
#     --executor-memory 4g \
#     --executor-cores 5 \
#     --packages ai.catboost:catboost-spark_3.5_2.12:1.2.8 \
#     $SCRIPTS/stage03/catboost.py

# if [ $? -ne 0 ]; then
#   echo "Catboost failed. Aborting..."
#   exit 1
# fi

# exit 0


##############
# Comparison #
##############

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/evaluation output/

echo "Stage 3 completed successfully."



# rm -rf $SCRIPTS/stage03/.venv
# python3 -m venv $SCRIPTS/stage03/.venv
# source $SCRIPTS/stage03/.venv/bin/activate
# pip install -r $SCRIPTS/stage03/requirements.txt
# rm -rf $BIN/.venv.tar.gz
# $SCRIPTS/stage03/.venv/bin/venv-pack -o $BIN/.venv.tar.gz

# export PYSPARK_DRIVER_PYTHON=".venv/bin/python"
# export PYSPARK_PYTHON="$SCRIPTS/stage03/.venv/bin/python"

# --archives $BIN/.venv.tar.gz#.venv \
# --deploy-mode cluster \


# spark-submit \
#     --master yarn \
#     --driver-memory 2g \
#     --num-executors 3 \
#     --executor-memory 4g \
#     --executor-cores 5 \
#     $SCRIPTS/stage03/main.py

# --packages org.apache.spark:spark-avro_2.12:3.5.1 \
# --packages org.apache.hadoop:hadoop-aws:3.4.1 \

# if [ $? -eq 1 ]; then
#     echo "Stage 3 failed."
#     exit 1
# fi

###############################
# Gradient Boosting Regressor #
###############################
# hdfs dfs -copyToLocal $HDFS_ROOT/project/models/model1 models/model1
# hdfs dfs -cat project/output/model3_predictions.csv/*.csv > output/model3_predictions.csv

