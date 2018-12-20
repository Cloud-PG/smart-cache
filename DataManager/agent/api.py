import hashlib
import json

import requests
import urllib3


class ElasticSearchHttp(object):

    def __init__(self, url, auth):
        self.__url = url
        if self.__url[-1] != "/":
            self.__url += "/"
        self.__auth = tuple(auth.split(":")) if auth != "" else None

    def put(self, data):
        blake2s = hashlib.blake2s()
        urllib3.disable_warnings()

        json_data = json.dumps(data)
        blake2s.update(json_data.encode("utf-8"))
        id_data = blake2s.hexdigest()

        res = requests.put(
            self.__url + id_data,
            auth=self.__auth,
            data=json_data,
            headers={'Content-Type': "application/json"},
            verify=False
        )

        return res
