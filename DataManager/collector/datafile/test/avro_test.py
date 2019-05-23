import unittest

import sys

sys.path.append("..")

from utils import AvroObjectTranslator
from fastavro import parse_schema


class TestAvroTranslator(unittest.TestCase):

    def test_deduce_schema(self):
        example = {
            'a': 1,
            'b': 2.5,
            'c': "hello",
            'd': True,
            'e': None,
            'f': "hellobytes".encode("utf-8"),
            'g': [1, 2, 3],
            'h': {'a': 2},
            'i': {'a': 2, 'b': "hello"}
        }

        schema = AvroObjectTranslator().deduce_scheme(example)
        parsed_schema = parse_schema(schema)

        self.assertEqual(parsed_schema['__fastavro_parsed'], True)


if __name__ == '__main__':
    unittest.main()
