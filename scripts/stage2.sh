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

# beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD

log "running q1"
echo "vendorid_null_values,vendorid_null_values_percent,lpep_pickup_datetime_null_values,lpep_pickup_datetime_null_values_percent | lpep_dropoff_datetime_null_values,lpep_dropoff_datetime_null_values_percent | store_and_fwd_flag_null_values,store_and_fwd_flag_null_values_percent,ratecodeid_null_values,ratecodeid_null_values_percent,pulocationid_null_values,pulocationid_null_values_percent,dolocationid_null_values,dolocationid_null_values_percent,passenger_count_null_values,passenger_count_null_values_percent,trip_distance_null_values,trip_distance_null_values_percent,fare_amount_null_values,fare_amount_null_values_percent,extra_null_values,extra_null_values_percent,mta_tax_null_values,mta_tax_null_values_percent,tip_amount_null_values,tip_amount_null_values_percent,tolls_amount_null_values,tolls_amount_null_values_percent,ehail_fee_null_values,ehail_fee_null_values_percent,improvement_surcharge_null_values,improvement_surcharge_null_values_percent | total_amount_null_values,total_amount_null_values_percent,payment_type_null_values,payment_type_null_values_percent,trip_type_null_values,trip_type_null_values_percent,congestion_surcharge_null_values,congestion_surcharge_null_values_percent" > $PROJECT_ROOT/output/q1_result.txt
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/q1.hql >> $PROJECT_ROOT/output/q1_result.txt

log "running q2"
echo "invalid_count,invalid_percent" > $PROJECT_ROOT/output/q2_result.txt
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/q2.hql >> $PROJECT_ROOT/output/q2_result.txt

log "running q3"
echo "year,corr_duration_price,corr_distance_price" > $PROJECT_ROOT/output/q3_result.txt
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $HIVE_PASSWORD -f $PROJECT_ROOT/sql/q3.hql > $PROJECT_ROOT/output/q3_result.txt
