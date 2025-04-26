#!/bin/bash

GREEN='\033[0;32m'
NC='\033[0m'

log() {
    echo -e "${GREEN}$1${NC}"
}

log "loading env variables"

export HADOOP_CONF_DIR=/etc/hadoop/conf
log "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"

export SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log "SCRIPTS=$SCRIPTS"

export PROJECT_ROOT="$(cd "$SCRIPTS/.." && pwd)"
log "PROJECT_ROOT=$PROJECT_ROOT"


files=(
    "POSTGRES_USERNAME"
    "POSTGRES_PASSWORD"
    "POSTGRES_HOST"
    "POSTGRES_PORT"
    "POSTGRES_DATABASE"
    "TEAMNAME"
)
missing=0

for file in "${files[@]}"; do
    if [ ! -f "$PROJECT_ROOT/secrets/$file" ]; then
        echo "Error: $PROJECT_ROOT/secrets/$file is missing"
        missing=1
    else
        echo "Found: $file"
    fi
done

if [ $missing -eq 1 ]; then
    log "MISSING SETTING FILES!!!"
    exit 1
fi

export POSTGRES_USERNAME=$(cat "$PROJECT_ROOT/secrets/POSTGRES_USERNAME")
log "POSTGRES_USERNAME is loaded"

export POSTGRES_PASSWORD=$(cat "$PROJECT_ROOT/secrets/POSTGRES_PASSWORD")
log "POSTGRES_PASSWORD is loaded"

export POSTGRES_HOST=$(cat "$PROJECT_ROOT/secrets/POSTGRES_HOST")
log "POSTGRES_HOST is loaded"

export POSTGRES_PORT=$(cat "$PROJECT_ROOT/secrets/POSTGRES_PORT")
log "POSTGRES_PORT is loaded"

export POSTGRES_DATABASE=$(cat "$PROJECT_ROOT/secrets/POSTGRES_DATABASE")
log "POSTGRES_DATABASE is loaded"

export TEAMNAME=$(cat "$PROJECT_ROOT/secrets/TEAMNAME")
log "TEAMNAME is loaded"

export HDFS_ROOT="hdfs:///user/$TEAMNAME"
log "HDFS_ROOT is loaded"

export HIVE_PASSWORD=$(cat "$PROJECT_ROOT/secrets/HIVE_PASSWORD")
log "HIVE_PASSWORD is loaded"
