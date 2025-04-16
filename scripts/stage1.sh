#!/bin/bash

set -e # set 'exit-on-error' mode

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
log "Identified binaries directory as $BIN"

DATA="$PROJECT_ROOT/data"
log "Identified data directory as $DATA"

log "Downloading green data"
$BIN/uv run "$SCRIPTS/dataset-organization/download-sources.py" \
    --base-url https://storage.yandexcloud.net/dartt0n/ibd/ \
    --start-year 2014 \
    --end-year 2024 \
    --start-month 1 \
    --end-month 12 \
    --file-prefix green_tripdata \
    --file-extension parquet \
    --output-dir "$DATA" \
    --max-concurrent 12

log "Loading data to $HDFS_ROOT/project/rawdata"
hdfs dfs -rm -r -f $HDFS_ROOT/project/rawdata
hdfs dfs -mkdir -p $HDFS_ROOT/project/rawdata
hdfs dfs -put $DATA $HDFS_ROOT/project/rawdata
export PARQUET_SOURCE="/user/$TEAMNAME/project/rawdata/*.parquet"

log "Creating tables in PostgreSQL"
$BIN/uv run "$SCRIPTS/dataset-organization/create-tables.py" \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --user $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --database $POSTGRES_DATABASE

log "Clearning directory $HDFS_ROOT/project/warehouse before loading data"
hdfs dfs -rm -r -f $HDFS_ROOT/project/warehouse
hdfs dfs -mkdir -p $HDFS_ROOT/project/warehous

log "Building scala jar"
ROLLBACK=$pwd
cd $SCRIPTS/dataset-organization
$BIN/sbt assembly
cp $SCRIPTS/dataset-organization/target/scala-2.12/tlddataloader-1.0.0.jar $SCRIPTS/dataset-organization/tlddataloader.jar
cd $ROLLBACK

log "Loading data into Postgres using spark"
spark-submit --class TripDataLoader $SCRIPTS/dataset-organization/tlddataloader.jar \
    --conf "spark.executorEnv.POSTGRES_HOST=$POSTGRES_HOST" \
    --conf "spark.executorEnv.POSTGRES_PORT=$POSTGRES_PORT" \
    --conf "spark.executorEnv.POSTGRES_USERNAME=$POSTGRES_USERNAME" \
    --conf "spark.executorEnv.POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    --conf "spark.executorEnv.POSTGRES_DATABASE=$POSTGRES_DATABASE" \
    --conf "spark.executorEnv.PARQUET_SOURCE=$PARQUET_SOURCE"

# log "Loading data from PostgreSQL to cluster using scoop"
# sqoop import-all-tables \
#     --connect jdbc:postgresql:/$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DATABASE \
#     --username $POSTGRES_USERNAME \
#     --password $POSTGRES_PASSWORD \
#     --compression-codec=zstd \
#     --compress \
#     --warehouse-dir=project/warehouse \
#     --m 1
