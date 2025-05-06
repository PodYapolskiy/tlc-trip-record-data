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
    UV_ARCHIVE="uv-x86_64-unknown-linux-gnu.tar.gz"
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

if [ -f "$BIN/sbt" ]; then
    log "File $BIN/sbt exists"
else
    log "downloading coursier to setup scala"
    curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d >cs && chmod +x cs && ./cs setup -y
    log "removing coursier"
    rm cs
    log "copying files to bin folder"
    cp ~/.local/share/coursier/bin/* $BIN/
fi

if [ -f "$BIN/avro-tools.jar" ]; then
    log "File $BIN/avro-tools.jar exists"
else
    log "Downloading avro-tools"
    wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar -O $BIN/avro-tools.jar

    log "avro-tools downloaded to $BIN/avro-tools.jar"
fi

log "loaded binaries: $(ls $BIN/)"
