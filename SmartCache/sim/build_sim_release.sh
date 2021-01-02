#!/usr/bin/env bash

echo "[BUILD SIMULATOR]" &&
    go build -a -v -o bin -ldflags "-s -w -X main.buildstamp=$(date -u '+%Y-%m-%d_%I:%M:%S%p') -X main.githash=$(git rev-parse --short HEAD)" ./... &&
    # upx --brute bin/simulator # to cut executable size by compression
    echo "[BUILD SIMULATOR][DONE]"
