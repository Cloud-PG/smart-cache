#!/usr/bin/env bash

echo "[BUILD SIMULATOR]" &&
go build -o bin -ldflags "-s -w -X main.buildstamp=`date -u '+%Y-%m-%d_%I:%M:%S%p'` -X main.githash=`git rev-parse HEAD`" ./... &&
echo "[BUILD SIMULATOR][DONE]"
