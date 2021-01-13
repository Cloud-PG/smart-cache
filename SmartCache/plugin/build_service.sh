#!/usr/bin/env bash

go build -ldflags "-X simulator/v2/cache/cmd.buildstamp=$(date -u '+%Y-%m-%d_%I:%M:%S%p') -X simulator/v2/cache/cmd.githash=$(git rev-parse HEAD)" -o goXcachePlugin main.go
