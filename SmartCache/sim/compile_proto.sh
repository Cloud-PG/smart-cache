#!/urs/bin/env bash

protoc -I cache cache/simService/simService.proto --go_out=plugins=grpc:cache/

python -m grpc_tools.protoc -I cache --python_out=. --grpc_python_out=. cache/simService/simService.proto
touch simService/__init__.py
rm -rf pySimService
mv -f simService pySimService
sed -i -e 's/from\ simService/from\ \./g' ./pySimService/simService_pb2_grpc.py
rm ./pySimService/simService_pb2_grpc.py-e

