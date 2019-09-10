#!/usr/bin/env bash

protoc -I service service/pluginProto/pluginProto.proto --go_out=plugins=grpc:service/

python -m grpc_tools.protoc -I service --python_out=. --grpc_python_out=. service/pluginProto/pluginProto.proto
touch pluginProto/__init__.py
sed -i -e 's/from\ pluginProto/from\ \./g' pluginProto/pluginProto_pb2_grpc.py
rm -f "pluginProto/pluginProto_pb2_grpc.py-e"
