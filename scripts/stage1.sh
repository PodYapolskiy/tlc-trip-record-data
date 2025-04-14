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

if [ -f "$DATA/green_data.parquet" ]; then
    log "Green data already exists"
else
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

    log "Merging green data"
    $BIN/uv run "$SCRIPTS/dataset-organization/merge-parquets.py" \
        --source-dir $DATA \
        --output-file $DATA/green_data.parquet \
        --prefix "green_tripdata_" \
        --compression zstd \
        --compression-level 22 \
        --file-extension ".parquet"

    log "Generated $DATA/green_data.parquet"
fi

log "Loading data to PostgreSQL"
$BIN/uv run "$SCRIPTS/dataset-organization/load-data-to-psql.py" \
    --source-file $DATA/green_data.parquet \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --user $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --database $POSTGRES_DATABASE

log "Deleted $DATA"
rm -rf "$DATA"

log "Clearning directory $HDFS_ROOT/project/warehouse before loading data"
hdfs dfs -rm -rf $HDFS_ROOT/project/warehouse
hdfs dfs -mkdir -p $HDFS_ROOT/project/warehouse

log "Loading data from PostgreSQL to cluster using scoop"
sqoop import-all-tables \
    --connect jdbc:postgresql:/$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DATABASE \
    --username $POSTGRES_USERNAME \
    --password $POSTGRES_PASSWORD \
    --compression-codec=snappy \
    --compress \
    --as-avrodatafile \
    --warehouse-dir=project/warehouse \
    --m 1

# todo: try zstd?
# todo: move files to output?
