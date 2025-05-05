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
$BIN/uv run "$SCRIPTS/stage01/download/download-sources.py" \
    --base-url https://storage.yandexcloud.net/dartt0n/ibd/ \
    --start-year 2014 \
    --end-year 2024 \
    --start-month 1 \
    --end-month 12 \
    --file-prefix green_tripdata \
    --file-extension parquet \
    --output-dir "$DATA" \
    --max-concurrent 12

log "Setting up $HDFS_ROOT/project/"
hdfs dfs -rm -r -f $HDFS_ROOT/project/rawdata
hdfs dfs -mkdir -p $HDFS_ROOT/project/rawdata
hdfs dfs -mkdir -p $HDFS_ROOT/project/merged
hdfs dfs -put $DATA $HDFS_ROOT/project/rawdata
hdfs dfs -rm -r -f $HDFS_ROOT/project/warehouse
hdfs dfs -mkdir -p $HDFS_ROOT/project/warehouse

log "Creating tables in PostgreSQL"
$BIN/uv run "$SCRIPTS/stage01/create-tables/create_tables.py" \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --user $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --database $POSTGRES_DATABASE \
    --psql-create-schema $PROJECT_ROOT/sql/create-table-psql.sql

log "Building scala jar"
ROLLBACK=$pwd
cd $SCRIPTS/stage01/dataloader
$BIN/sbt clean assembly
cd $ROLLBACK

log "Loading data into Postgres using spark"
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class tlcdataloader.TlcDataLoader \
    $SCRIPTS/stage01/dataloader/target/scala-2.12/load-data-assembly-0.1.0.jar \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --username $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --database $POSTGRES_DATABASE \
    --table green_tripdata \
    --source "/user/$TEAMNAME/project/rawdata/data" \
    --merged "/user/$TEAMNAME/project/data"

log "Loading data from PostgreSQL to cluster using scoop"
sqoop import \
    --connect jdbc:postgresql://$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DATABASE \
    --username $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --table green_tripdata \
    --compression-codec=snappy \
    --compress \
    --warehouse-dir=$HDFS_ROOT/project/warehouse \
    --split-by year \
    -m 10
