#!/usr/bin/env bash

protoc -I . ./ai.proto --go_out=plugins=grpc:../../sim/cache/aiService/

python -m grpc_tools.protoc -I ./ --python_out=. --grpc_python_out=. ./ai.proto
sed -i -e 's/import\ ai\_pb2/from\ \.\ import\ ai\_pb2/g' ai_pb2_grpc.py
rm -f ai_pb2_grpc.py-e