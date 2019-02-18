import hashlib
import json
from os import path

import requests
import urllib3


class HTTPFS(object):

    def __init__(self, url, http_user=None, http_password=None, verify=False, allow_redirects=False, hadoop_user='root'):
        self._server_url = url
        self._http_user = http_user
        self._http_password = http_password
        self._verify = verify
        self._allow_redirects = allow_redirects
        self._api_url = "/webhdfs/v1"
        self._hadoop_user = hadoop_user

    def mkdirs(self, hdfs_path):
        res = requests.put(
            "{}{}{}".format(
                self._server_url,
                self._api_url,
                hdfs_path
            ),
            params={
                'op': "MKDIRS",
                'user.name': self._hadoop_user
            },
            auth=(self._http_user, self._http_password),
            verify=self._verify,
            allow_redirects=self._allow_redirects
        )
        if res.status_code != 200:
            raise Exception("Error on make folders:\n{}".format(res.text))

    def liststatus(self, hdfs_path, print_list=False):
        res = requests.get(
            "{}{}{}".format(
                self._server_url,
                self._api_url,
                hdfs_path
            ),
            params={
                'op': "LISTSTATUS",
                'user.name': self._hadoop_user
            },
            auth=(self._http_user, self._http_password),
            verify=self._verify,
            allow_redirects=self._allow_redirects
        )
        if res.status_code != 200:
            raise Exception("Error on liststatus of folder '{}':\n{}".format(
                hdfs_path, json.dumps(res.json(), indent=2)))
        res = res.json()
        if print_list:
            print("### hdfs path: {} ###".format(hdfs_path))
            for record in res['FileStatuses']['FileStatus']:
                print("-[{}] {}".format(record['type'], record['pathSuffix']))
        # Generator
        for record in res['FileStatuses']['FileStatus']:
            yield record['type'], record['pathSuffix'], path.join(hdfs_path, record['pathSuffix'])

    def delete(self, hdfs_path, recursive=True):
        res = requests.get(
            "{}{}{}".format(
                self._server_url,
                self._api_url,
                hdfs_path
            ),
            params={
                'op': "DELETE",
                'user.name': self._hadoop_user,
                'recursive': recursive
            },
            auth=(self._http_user, self._http_password),
            verify=self._verify,
            allow_redirects=self._allow_redirects
        )
        if res.status_code != 200:
            raise Exception("Error on delete path '{}':\n{}".format(
                hdfs_path, json.dumps(res.json(), indent=2)))

        return res.json()['boolean']

    def create(self, hdfs_path, file_path, overwrite=False, noredirect=True):
        file_url = "{}{}{}".format(
            self._server_url,
            self._api_url,
            hdfs_path
        )
        res = requests.put(
            file_url,
            params={
                'op': "CREATE",
                'user.name': self._hadoop_user,
                'noredirect': noredirect,
                'overwrite': overwrite
            },
            auth=(self._http_user, self._http_password),
            verify=self._verify,
            allow_redirects=self._allow_redirects
        )
        if res.status_code not in [200, 201, 307]:
            raise Exception("Error on create file:\n{}".format(
                json.dumps(res.json(), indent=2)))
        with open(file_path, 'rb') as file_:
            res = requests.put(
                file_url,
                headers={
                    'content-type': "application/octet-stream"
                },
                params={
                    'op': "CREATE",
                    'user.name': self._hadoop_user,
                    'noredirect': noredirect,
                    'overwrite': overwrite,
                    'data': True
                },
                auth=(self._http_user, self._http_password),
                verify=self._verify,
                allow_redirects=self._allow_redirects,
                data=file_
            )
        if res.status_code not in [200, 201, 307]:
            raise Exception("Error on upload file:\n{}".format(
                json.dumps(res.json(), indent=2)))


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
            all_objects = [json.dumps(elm) for elm in data]
            all_object_ids = [
                json.dumps(
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
            json_data = json.dumps(data)
            id_data = self.__gen_id(json_data)

            res = requests.put(
                self.__url + id_data,
                auth=self.__auth,
                data=json_data,
                headers={'Content-Type': "application/json"},
                verify=False
            )

        return res
