#!/bin/bash

set -e # set 'exit-on-error' mode

GREEN='\033[0;32m'
NC='\033[0m'

log() {
    echo -e "${GREEN}$1${NC}"
}

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log "Identified scripts directory as $SCRIPTS"

PROJECT_ROOT="$(cd "$SCRIPTS/.." && pwd)"
log "Identified root directory as $ROOT_DIR"

bash "$SCRIPTS/prepare-bin.sh"

BIN="$SCRIPTS/bin"
log "Identified binaries directory as $BIN"

$BIN/uv run "$SCRIPTS/dataset-organization/download-sources.py" \
    --base-url https://d37ci6vzurychx.cloudfront.net/trip-data/ \
    --start-year 2014 \
    --end-year 2024 \
    --start-month 1 \
    --end-month 12 \
    --file-prefix green_tripdata \
    --file-extension parquet \
    --output-dir "$PROJECT_ROOT/data" \
    --max-concurrent 12

$BIN/uv run merge-parquets.py \
    --source-dir $PROJECT_ROOT/data \
    --output-file $PROJECT_ROOT/data/green_data.parquet \
    --prefix "green_tripdata_" \
    --file-extension ".parquet"
