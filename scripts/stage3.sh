#!/bin/bash

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Identified scripts directory as $SCRIPTS"

spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.4.1 \
    --packages org.apache.spark:spark-avro_2.12:3.5.1 \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 5 \
    $SCRIPTS/stage03/main.py
    # --master yarn \
    # --packages ai.catboost:catboost-spark_3.5_2.12 \


if [ $? -eq 0 ]; then
    echo "Stage 3 completed successfully."
else
    echo "Stage 3 failed."
    exit 1
fi