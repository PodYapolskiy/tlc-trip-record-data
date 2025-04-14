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

PROJECT_ROOT="$(cd "$SCRIPTS/.." && pwd)"
log "Identified root directory as $PROJECT_ROOT"

bash "$SCRIPTS/prepare-bin.sh"

BIN="$SCRIPTS/bin"
log "Identified binaries directory as $BIN"

DATA="$PROJECT_ROOT/data"
log "Identified data directory as $DATA"

log "Downloading green data"
$BIN/uv run "$SCRIPTS/dataset-organization/download-sources.py" \
    --base-url https://d37ci6vzurychx.cloudfront.net/trip-data/ \
    --start-year 2014 \
    --end-year 2024 \
    --start-month 1 \
    --end-month 12 \
    --file-prefix green_tripdata \
    --file-extension parquet \
    --output-dir "$DATA" \
    --max-concurrent 12

log "Creating tables in PostgreSQL"
$BIN/uv run "$SCRIPTS/dataset-organization/create-tables.py" \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --user $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --database $POSTGRES_DATABASE

log "Clearning directory $HDFS_ROOT/project/warehouse before loading data"
hdfs dfs -rm -r -f $HDFS_ROOT/project/warehouse
hdfs dfs -mkdir -p $HDFS_ROOT/project/warehouse

log "Loading data into Postgres using spark"
spark-shell -i "$SCRIPTS/dataset-organization/load-data.sc" \
    --conf "spark.executorEnv.POSTGRES_HOST=$POSTGRES_HOST" \
    --conf "spark.executorEnv.POSTGRES_PORT=$POSTGRES_PORT" \
    --conf "spark.executorEnv.POSTGRES_USERNAME=$POSTGRES_USERNAME" \
    --conf "spark.executorEnv.POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    --conf "spark.executorEnv.POSTGRES_DATABASE=$POSTGRES_DATABASE"

log "Loading data from PostgreSQL to cluster using scoop"
sqoop import-all-tables \
    --connect jdbc:postgresql:/$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DATABASE \
    --username $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --compression-codec=zstd \
    --compress \
    --warehouse-dir=project/warehouse \
    --m 1
