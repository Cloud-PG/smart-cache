#!/usr/bin/env bash

protoc -I . ./ai.proto --go_out=plugins=grpc:../../sim/cache/aiService/

python -m grpc_tools.protoc -I ./ --python_out=. --grpc_python_out=. ./ai.proto
# touch simService/__init__.py
# rm -rf pySimService
# mv -f simService pySimService
# sed -i -e 's/from\ simService/from\ \./g' pySimService/simService_pb2_grpc.py
# rm -f "pySimService/simService_pb2_grpc.py-e"
