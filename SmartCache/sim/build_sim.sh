#!/usr/bin/env bash

echo "[BUILD SIMULATOR]" &&
    go build -a -v -o bin -ldflags "-X simulator/v2/cache/cmd.buildstamp=$(date -u '+%Y-%m-%d_%I:%M:%S%p') -X simulator/v2/cache/cmd.githash=$(git rev-parse --short HEAD)" ./... &&
    echo "[BUILD SIMULATOR][DONE]"
