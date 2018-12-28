import unittest
import os
import json


class TestConverters(unittest.TestCase):

    def test_jsonDataFile_gz(self):
        from .json import JSONDataFileWriter, JSONDataFileReader

        FILENAME = "test.json.gz"
        with JSONDataFileWriter(FILENAME, ['{"a": 2}']) as data:
            data.append(json.dumps({}))
            data.append([json.dumps({})])
            data.append([{}, {"a": 2}, {}])

        with JSONDataFileReader(FILENAME) as data:
            self.assertEqual(data[0], data[4])

        os.remove(FILENAME)

    def test_jsonDataFile_bz2(self):
        from .json import JSONDataFileWriter, JSONDataFileReader

        FILENAME = "test.json.bz2"
        with JSONDataFileWriter(FILENAME, ['{"a": 2}']) as data:
            data.append(json.dumps({}))
            data.append([json.dumps({})])
            data.append([{}, {"a": 2}, {}])

        with JSONDataFileReader(FILENAME) as data:
            self.assertEqual(data[0], data[4])

        os.remove(FILENAME)

    def test_jsonDataFile_binary(self):
        from .json import JSONDataFileWriter, JSONDataFileReader

        FILENAME = "test.json"
        with JSONDataFileWriter(FILENAME, ['{"a": 2}']) as data:
            data.append(json.dumps({}))
            data.append([json.dumps({})])
            data.append([{}, {"a": 2}, {}])

        with JSONDataFileReader(FILENAME) as data:
            self.assertEqual(data[0], data[4])

        os.remove(FILENAME)


if __name__ == '__main__':
    unittest.main()
