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


hdfs dfs -rm -r -f -skipTrash ${HDFS_ROOT}/project/hive/warehouse
hdfs dfs -mkdir -p ${HDFS_ROOT}/project/hive/warehouse/avsc

hadoop jar $BIN/avro-tools.jar getschema ${HDFS_ROOT}/project/hive/warehouse/green_tripdata/part-m-00000.avro > schema.avsc
hdfs dfs -put schema.avsc ${HDFS_ROOT}/project/hive/warehouse/avsc
rm schema.avsc

mkdir -p output
rm -f output/hive_results.txt
touch output/hive_results.txt

log "creating initial table"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/db.hql >> output/hive_results.txt

log "Creating partitioned tables"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/partition.hql >> output/hive_results.txt