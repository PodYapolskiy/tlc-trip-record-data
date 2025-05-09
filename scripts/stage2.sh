#!/bin/bash

set -e # set 'exit-on-error' mode
export HADOOP_CONF_DIR=/etc/hadoop/conf

GREEN='\033[0;32m'
NC='\033[0m'

log() {
    echo -e "${GREEN}$1${NC}"
}

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log "Identified scripts directory as $SCRIPTS"

. "$SCRIPTS/load-secrets.sh"
if [ $? -ne 0 ]; then
    exit 1
fi

PROJECT_ROOT="$(cd "$SCRIPTS/.." && pwd)"
log "Identified root directory as $PROJECT_ROOT"

bash "$SCRIPTS/prepare-bin.sh"
if [ $? -ne 0 ]; then
    exit 1
fi

BIN="$SCRIPTS/bin"

log "clearning hive data"
hdfs dfs -rm -r -f -skipTrash ${HDFS_ROOT}/project/hive/warehouse

log "generating avro schema"
hdfs dfs -rm -r -f -skipTrash ${HDFS_ROOT}/project/warehouse/avsc
hadoop jar $BIN/avro-tools.jar getschema ${HDFS_ROOT}/project/warehouse/green_tripdata/part-m-00000.avro >/tmp/schema.avsc
hdfs dfs -mkdir -p ${HDFS_ROOT}/project/warehouse/avsc
hdfs dfs -put /tmp/schema.avsc ${HDFS_ROOT}/project/warehouse/avsc/
rm /tmp/schema.avsc
log "avro schema generated"

log "preparing output file"
mkdir -p $PROJECT_ROOT/output
rm -f $PROJECT_ROOT/output/hive_results.txt
touch $PROJECT_ROOT/output/hive_results.txt

log "creating initial table"
hdfs dfs -mkdir -p ${HDFS_ROOT}/project/hive/warehouse
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/db.hql >>$PROJECT_ROOT/output/hive_results.txt

# beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD
hdfs dfs -rm -r -f -skipTrash ${HDFS_ROOT}/project/hive/eda
hdfs dfs -mkdir -p ${HDFS_ROOT}/project/hive/eda

log "running q1"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/q1.hql
echo "column_name,null_count,null_percent" >$PROJECT_ROOT/output/q1_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q1_result/*" >>$PROJECT_ROOT/output/q1_result.txt
cp $PROJECT_ROOT/output/q1_result.txt $PROJECT_ROOT/output/q1.csv

log "running q2"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/q2.hql
echo "invalid_count,invalid_percent" >$PROJECT_ROOT/output/q2_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q2_result/*" >>$PROJECT_ROOT/output/q2_result.txt
cp $PROJECT_ROOT/output/q2_result.txt $PROJECT_ROOT/output/q2.csv

log "running q3"

rm -rf $SCRIPTS/stage02/.venv
python3 -m venv $SCRIPTS/stage02/.venv
source $SCRIPTS/stage02/.venv/bin/activate
pip install -r $SCRIPTS/stage02/requirements.txt
rm -rf $BIN/.venv.tar.gz
$SCRIPTS/stage02/.venv/bin/venv-pack -o $BIN/.venv.tar.gz

export PYSPARK_DRIVER_PYTHON=".venv/bin/python"
export PYSPARK_PYTHON="$SCRIPTS/stage02/.venv/bin/python"

spark-submit \
    --master yarn \
    --archives $BIN/.venv.tar.gz#.venv \
    --deploy-mode cluster \
    $SCRIPTS/stage02/q3.py

echo "year,corr_duration_price,corr_distance_price" >$PROJECT_ROOT/output/q3_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q3_result/*" >>$PROJECT_ROOT/output/q3_result.txt
cp $PROJECT_ROOT/output/q3_result.txt $PROJECT_ROOT/output/q3.csv

log "running q4"

spark-submit \
    --master yarn \
    --archives $BIN/.venv.tar.gz#.venv \
    --deploy-mode cluster \
    $SCRIPTS/stage02/q4.py

echo "year,month,distance,price,passenger_count" >$PROJECT_ROOT/output/q4_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q4_result/*" >>$PROJECT_ROOT/output/q4_result.txt
cp $PROJECT_ROOT/output/q4_result.txt $PROJECT_ROOT/output/q4.csv

log "running q5"

spark-submit \
    --master yarn \
    --archives $BIN/.venv.tar.gz#.venv \
    --deploy-mode cluster \
    $SCRIPTS/stage02/q5.py

echo "price,pickup_hour,dropoff_hour" >$PROJECT_ROOT/output/q5_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q5_result/*" >>$PROJECT_ROOT/output/q5_result.txt
cp $PROJECT_ROOT/output/q5_result.txt $PROJECT_ROOT/output/q5.csv

# this query is too heavy for cluster
# log "running q6"

# spark-submit \
#     --master yarn \
#     --archives $BIN/.venv.tar.gz#.venv \
#     --deploy-mode cluster \
#     $SCRIPTS/stage02/q6.py

# echo "price,pickup_location,dropoff_location" >$PROJECT_ROOT/output/q6_result.txt
# hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q6_result/*" >>$PROJECT_ROOT/output/q6_result.txt
# cp $PROJECT_ROOT/output/q6_result.txt > $PROJECT_ROOT/output/q6.csv

log "running q7"

spark-submit \
    --master yarn \
    --archives $BIN/.venv.tar.gz#.venv \
    --deploy-mode cluster \
    $SCRIPTS/stage02/q7.py

echo "year,month,date,total_records" >$PROJECT_ROOT/output/q7_result.txt
hdfs dfs -cat "${HDFS_ROOT}/project/hive/eda/q7_result/*" >>$PROJECT_ROOT/output/q7_result.txt
cp $PROJECT_ROOT/output/q7_result.txt $PROJECT_ROOT/output/q7.csv
