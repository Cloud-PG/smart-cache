#!/usr/bin/env bash

# Useful for vscode tasks with python virtualenvs
export PATH="$EXTRA_PYTHON_PATH:$PATH"

echo "[GENERATE GO GRPC]"
protoc -I cache cache/simService/simService.proto --go_out=plugins=grpc:cache/
echo "[GENERATE GO GRPC][DONE]"

echo "[GENERATE Python GRPC]"
python -m grpc_tools.protoc -I cache --python_out=. --grpc_python_out=. cache/simService/simService.proto
echo "[GENERATE Python GRPC][DONE]"

echo "[FIX Python GRPC MODULE]"
touch simService/__init__.py
rm -rf pySimService
mv -f simService pySimService
sed -i -e 's/from\ simService/from\ \./g' pySimService/simService_pb2_grpc.py
rm -f "pySimService/simService_pb2_grpc.py-e"
echo "[FIX Python GRPC MODULE][DONE]"
