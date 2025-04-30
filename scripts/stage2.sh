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
hadoop jar $BIN/avro-tools.jar getschema ${HDFS_ROOT}/project/warehouse/green_tripdata/part-m-00000.avro > /tmp/schema.avsc
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
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/db.hql >> $PROJECT_ROOT/output/hive_results.txt
