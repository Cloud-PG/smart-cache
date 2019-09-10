import json
import math
from os import path
from subprocess import check_output

import grpc
from flask import Flask, jsonify, request
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

from pluginProto import pluginProto_pb2, pluginProto_pb2_grpc

app = Flask(__name__)
channel = grpc.insecure_channel("localhost:4243")
stubSimService = pluginProto_pb2_grpc.SimServiceStub(channel)
stubSimService.ResetHistory(google_dot_protobuf_dot_empty__pb2.Empty())


def getSize(dasgresult: list, filename: str):
    for result in dasgresult:
        if 'das' in result and 'file' in result:
            for file_ in result['file']:
                if file_['name'] == filename:
                    return file_['size']
    return 1024. ** 2


SIZES = {}


@app.route("/resolve", methods=['GET', 'POST'])
def resolve():
    file_name = request.values.get('lfn', False)
    mean_time = request.values.get('time', math.nan)
    n_access = request.values.get('naccess', False)
    file_downloaded = request.values.get('downloaded', 0.)
    print(json.dumps({
        'filename': file_name,
        'mean_time': mean_time,
        'n_acces': n_access,
        'downloaded': file_downloaded
    }, indent=2, sort_keys=True))
    if file_name:
        file_name = f"/store{file_name.split('//store')[1].split('.root')[0]}.root"
        print(f"[Request][Filename: {file_name}]")
        # TO DO
        # - Calculate the % of file read
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
        # if it is the first time we assign the whole size as
        # file downloaded info
        if file_downloaded == 0.:
            file_downloaded = file_size
        stubSimService.UpdateStats(
            pluginProto_pb2.FileRequest(
                file_name,
                file_downloaded,
                False,
                mean_time,
                n_access
            )
        )
        result = stubSimService.GetHint(
            pluginProto_pb2.FileHint(
                file_name,
                False,
            )
        )
        return jsonify({'store': result.store, 'filename': result.filename})
    else:
        return jsonify({'error': "lfn argument missing"})


if __name__ == '__main__':
    print("[Server start on port 4242]")
    app.run(host='0.0.0.0', port=4242)
