import hashlib
import simplejson

import requests
import urllib3


class ElasticSearchHttp(object):

    def __init__(self, url, auth):
        self.__url = url
        if self.__url[-1] != "/":
            self.__url += "/"
        self.__auth = tuple(auth.split(":")) if auth != "" else None

    @staticmethod
    def __gen_id(string):
        blake2s = hashlib.blake2s()
        blake2s.update(string.encode("utf-8"))
        return blake2s.hexdigest()

    def put(self, data):

        urllib3.disable_warnings()

        if isinstance(data, list):
            all_objects = [simplejson.dumps(elm) for elm in data]
            all_object_ids = [
                simplejson.dumps(
                    {"index": {"_id": self.__gen_id(elm)}}
                )
                for elm in all_objects
            ]
            data2send = zip(all_object_ids, all_objects)
            bulk = "\n".join((
                "\n".join(elms) for elms in data2send
            )) + "\n"

            res = requests.put(
                self.__url + "_bulk",
                auth=self.__auth,
                data=bulk,
                headers={'Content-Type': "application/json"},
                verify=False
            )
        else:
            json_data = simplejson.dumps(data)
            id_data = self.__gen_id(json_data)

            res = requests.put(
                self.__url + id_data,
                auth=self.__auth,
                data=json_data,
                headers={'Content-Type': "application/json"},
                verify=False
            )

        return res
