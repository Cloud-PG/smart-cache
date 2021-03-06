#!/usr/bin/env bash

echo "[BUILD SIMULATOR]" &&
go build -a -v -o bin -ldflags "-X main.buildstamp=`date -u '+%Y-%m-%d_%I:%M:%S%p'` -X main.githash=`git rev-parse HEAD`" ./... &&
echo "[BUILD SIMULATOR][DONE]"
