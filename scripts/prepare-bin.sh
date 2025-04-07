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
log "Identified root directory as $PROJECT_ROOT"

BIN="$SCRIPTS/bin"
log "All required binaries will be downloaded to $BIN"

if [ -d "$BIN" ]; then
    log "Directory $BIN already exists"
else
    log "Creating directory $BIN"
    mkdir -p "$BIN"
fi

if [ -f "$BIN/uv" ]; then
    log "File $BIN/uv already exists"
else
    UV_ARCHIVE="uv-i686-unknown-linux-gnu.tar.gz"
    # UV_ARCHIVE="uv-aarch64-apple-darwin.tar.gz" # for ARM Mac

    if [ -d "$SCRIPTS/$UV_ARCHIVE" ]; then
        log "Directory $SCRIPTS/$UV_ARCHIVE already exists"
    else
        log "Downloading uv-i686-unknown-linux-gnu.tar.gz"
        wget "https://github.com/astral-sh/uv/releases/download/0.6.11/$UV_ARCHIVE" -O "$SCRIPTS/$UV_ARCHIVE"

        log "Extracting $SCRIPTS/$UV_ARCHIVE"
        tar -xzf $SCRIPTS/$UV_ARCHIVE -C $SCRIPTS
    fi

    log "Moving uv binary to $BIN"
    mv "$SCRIPTS/${UV_ARCHIVE%%.*}/uv" "$BIN/uv"

    log "Removing $SCRIPTS/$UV_ARCHIVE"
    rm -r "$SCRIPTS/${UV_ARCHIVE%%.*}" "$SCRIPTS/$UV_ARCHIVE"
fi

log "Loading environment variables"

export -x POSTGRES_USERNAME=$(cat "$PROJECT_ROOT/secrets/POSTGRES_USERNAME")
export -x POSTGRES_PASSWORD=$(cat "$PROJECT_ROOT/secrets/POSTGRES_PASSWORD")
export -x POSTGRES_HOST=$(cat "$PROJECT_ROOT/secrets/POSTGRES_HOST")
export -x POSTGRES_PORT=$(cat "$PROJECT_ROOT/secrets/POSTGRES_PORT")
export -x POSTGRES_DATABASE=$(cat "$PROJECT_ROOT/secrets/POSTGRES_DATABASE")
export -x TEAMNAME=$(cat "$PROJECT_ROOT/secrets/TEAMNAME")
