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

rm -rf $SCRIPTS/stage03/.venv
python3 -m venv $SCRIPTS/stage03/.venv
source $SCRIPTS/stage03/.venv/bin/activate
pip install -r $SCRIPTS/stage03/requirements.txt
rm -rf $BIN/.venv.tar.gz
$SCRIPTS/stage03/.venv/bin/venv-pack -o $BIN/.venv.tar.gz

export PYSPARK_DRIVER_PYTHON=".venv/bin/python"
export PYSPARK_PYTHON="$SCRIPTS/stage03/.venv/bin/python"

spark-submit \
    --master yarn \
    --archives $BIN/.venv.tar.gz#.venv \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 8g \
    --executor-cores 8 \
    $SCRIPTS/stage03/main.py

# --packages org.apache.spark:spark-avro_2.12:3.5.1 \
# --packages ai.catboost:catboost-spark_3.5_2.12 \
# --packages org.apache.hadoop:hadoop-aws:3.4.1 \

if [ $? -eq 1 ]; then
    echo "Stage 3 failed."
    exit 1
fi

########
# Data #
########
hdfs dfs -copyToLocal $HDFS_ROOT/project/data/train output/train.json
hdfs dfs -copyToLocal $HDFS_ROOT/project/data/test output/test.json

#####################
# Linear Regression #
#####################
hdfs dfs -copyToLocal $HDFS_ROOT/project/models/model1 models/model1
hdfs dfs -copyToLocal $HDFS_ROOT/project/output/model1_predictions output/model1_predictions.csv

###########################
# Random Forest Regressor #
###########################
hdfs dfs -copyToLocal $HDFS_ROOT/project/models/model2 models/model2
hdfs dfs -copyToLocal $HDFS_ROOT/project/output/model2_predictions output/model2_predictions.csv

##############
# Comparison #
##############
hdfs dfs -copyToLocal $HDFS_ROOT/project/output/evaluation output/evaluation.csv

echo "Stage 3 completed successfully."
