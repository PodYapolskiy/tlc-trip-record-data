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
  $SCRIPTS/stage03/preprocessing.py

hdfs dfs -cat $HDFS_ROOT/project/data/train/*.json > data/train.json
hdfs dfs -cat $HDFS_ROOT/project/data/test/*.json > data/test.json


#####################
# Linear Regression #
#####################
echo "Submitting Linear Regression job..."
spark-submit \
  --name "Team18 | Spark ML | Linear Regression" \
  --master yarn \
  $SCRIPTS/stage03/linear_regression.py

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/models/model1 models/
hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/model1_predictions output/


###########################
# Random Forest Regressor #
###########################
echo "Submitting Random Forest Regressor job..."
spark-submit \
    --name "Team18 | Spark ML | Random Forest Regressor" \
    --master yarn \
    $SCRIPTS/stage03/random_forest.py

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/models/model2 models/
hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/model2_predictions output/


##############################
# Graient Boosting Regressor #
##############################
echo "Submitting Gradient Boosting Regressor job..."
spark-submit \
    --name "Team18 | Spark ML | Gradient Boosting Regressor" \
    --master yarn \
    $SCRIPTS/stage03/gradient_boosting.py

hdfs dfs -copyToLocal -f $HDFS_ROOT/project/models/model3 models/
hdfs dfs -copyToLocal -f $HDFS_ROOT/project/output/model3_predictions output/

echo "Stage 3 completed successfully."
