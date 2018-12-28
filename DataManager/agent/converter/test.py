import unittest


class TestConverters(unittest.TestCase):

    def test_to_svm(self):
        from .libSVM import LibSVMConverter
        data = {
            'label': 5,
            'feature_1': 0.0,
            'feature_2': 1.0,
            'feature_3': 2.0,
        }
        result = "5 1:0.0 2:1.0 3:2.0"
        converter = LibSVMConverter()
        self.assertEqual(converter.labeled_row(data, 'label'), result)

    def test_to_csv(self):
        from .csv import CSVConverter
        data_0 = {
            'feature_1': 0.0,
            'feature_2': 1.0,
            'feature_3': 2.0,
        }
        data_1 = [0.0, 1.0, 2.0]
        result = "0.0,1.0,2.0"
        converter = CSVConverter()
        self.assertEqual(converter.row(data_0), result)
        self.assertEqual(converter.row(data_1), result)


if __name__ == '__main__':
    unittest.main()
