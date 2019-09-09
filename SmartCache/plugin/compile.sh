#!/usr/bin/env bash

cd dasgoclient &&
GOPATH=$HOME/go make build_linux &&
mv dasgoclient_linux ..