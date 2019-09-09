import json
from subprocess import check_output

import grpc
from flask import Flask, jsonify, request
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from os import path

from pluginProto import pluginService_pb2, pluginService_pb2_grpc

app = Flask(__name__)
channel = grpc.insecure_channel("localhost:4243")
stubSimService = pluginService_pb2_grpc.SimServiceStub(channel)
stubSimService.ResetHistory(google_dot_protobuf_dot_empty__pb2.Empty())


def getSize(dasgresult: list, filename: str):
    for result in dasgresult:
        if 'das' in result and 'file' in result:
            for file_ in result['file']:
                if file_['name'] == filename:
                    return file_['size']
    return 1024. ** 2


SIZES = {}


@app.route("/resolve")
def resolve():
    file_name = request.args.get('lfn', False)
    if file_name:
        file_name = f"/store{file_name.split('//store')[1].split('.root')[0]}.root"
        print(f"[Request][Filename: {file_name}]")
        if file_name not in SIZES:
            dasgocresult = json.loads(
                check_output(
                    f'X509_USER_PROXY={path.abspath("./proxy")} ./dasgoclient_linux -query="file={file_name}" -json',
                    shell=True
                )
            )
            print(json.dumps(dasgocresult, indent=2))
            SIZES[file_name] = getSize(dasgocresult, file_name)
        file_size = SIZES[file_name]
        print(f"[Request][Size: {file_size}]")
        stubSimService.UpdateStats(
            pluginService_pb2.FileRequest(
                file_name,
                file_size,
                False
            )
        )
        result = stubSimService.GetHint(
            pluginService_pb2.FileRequest(
                file_name,
                file_size,
                False
            )
        )
        return jsonify({'store': result.store, 'filename': result.filename})
    else:
        return jsonify({'error': "lfn argument missing"})


if __name__ == '__main__':
    print("[Server start on port 4242]")
    app.run(host='0.0.0.0', port=4242)
