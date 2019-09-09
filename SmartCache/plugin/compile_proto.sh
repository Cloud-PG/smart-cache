#!/usr/bin/env bash

protoc -I service service/pluginProto/pluginService.proto --go_out=plugins=grpc:service/

python -m grpc_tools.protoc -I service --python_out=. --grpc_python_out=. service/pluginProto/pluginService.proto
touch pluginProto/__init__.py
sed -i -e 's/from\ pluginProto/from\ \./g' pluginProto/pluginService_pb2_grpc.py
rm -f "pluginProto/pluginService_pb2_grpc.py-e"
