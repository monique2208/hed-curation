import json
import pandas as pd
import numpy as np
import unittest
from curation.remodeling.operations.factor_column_op import factor_column


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sample_data = [[0.0776, 0.5083, 'go', 'n/a', 0.565, 'correct', 'right', 'female'],
                           [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 0.49, 'correct', 'right', 'female'],
                           [9.5856, 0.5084, 'go', 'n/a', 0.45, 'correct', 'right', 'female'],
                           [13.5939, 0.5083, 'succesful_stop', 0.2, 'n/a', 'n/a', 'n/a', 'female'],
                           [17.1021, 0.5083, 'unsuccesful_stop', 0.25, 0.633, 'correct', 'left', 'male'],
                           [21.6103, 0.5083, 'go', 'n/a', 0.443, 'correct', 'left', 'male']]
        cls.factored = [[0.0776, 0.5083, 'go', 'n/a', 0.565, 'correct', 'right', 'female', 0, 0],
                        [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 0.49, 'correct', 'right', 'female', 0, 1],
                        [9.5856, 0.5084, 'go', 'n/a', 0.45, 'correct', 'right', 'female', 0, 0],
                        [13.5939, 0.5083, 'succesful_stop', 0.2, 'n/a', 'n/a', 'n/a', 'female', 1, 0],
                        [17.1021, 0.5083, 'unsuccesful_stop', 0.25, 0.633, 'correct', 'left', 'male', 0, 1],
                        [21.6103, 0.5083, 'go', 'n/a', 0.443, 'correct', 'left', 'male', 0, 0]]
        cls.sample_columns = ['onset', 'duration', 'trial_type', 'stop_signal_delay', 'response_time',
                              'response_accuracy', 'response_hand', 'sex']
        base_parameters = {
            "column_name": "trial_type",
            "factor_values": ["succesful_stop", "unsuccesful_stop"],
            "factor_names": ["stopped", "stop_failed"],
            "ignore_missing": True,
            "overwrite_existing": True
        }
        cls.default_factor_columns = ["trial_type.succesful_stop", "trial_type.unsuccesful_stop"]
        cls.json_parms = json.dumps(base_parameters)


    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_factors_no_extras(self):
        parms = json.loads(self.json_parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = factor_column(df_test, parms)
        self.assertEqual(len(df_check), len(df_test),
                         "factor_column should not change number of rows with ignore missing")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column check should have extra columns with no extras and ignore missing")
        self.assertTrue(list(df_test.columns) == list(df_check.columns),
                        "factor_column resulting df should have correct columns with no extras and ignore missing")
        self.assertTrue(np.array_equal(df_test.to_numpy(), df_check.to_numpy()),
                        "factor_column should have expected values when no extras and ignore missing")

    def test_valid_factors_no_extras_no_ignore(self):
        parms = json.loads(self.json_parms)
        parms["ignore_missing"] = False
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = factor_column(df_test, parms)
        self.assertEqual(len(df_check), len(df_test),
                         "factor_column should not change number of rows with no extras and no ignore")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column check should have extra columns with no extras and no ignore")
        self.assertTrue(list(df_test.columns) == list(df_check.columns),
                        "factor_column resulting df should have correct columns with no extras and no ignore")
        self.assertTrue(np.array_equal(df_test.to_numpy(), df_check.to_numpy()),
                        "factor_column should have expected values when no extras and no ignore")

    def test_valid_factors_extras_ignore(self):
        parms = json.loads(self.json_parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
        parms["factor_values"] = ["succesful_stop", "unsuccesful_stop", "face"]
        parms["factor_names"] = ["stopped", "stop_failed", "baloney"]
        df_check["baloney"] = [0, 0, 0, 0, 0, 0]
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = factor_column(df_test, parms)
        self.assertEqual(len(df_check), len(df_test),
                         "factor_column should not change number of rows with extras and ignore missing")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column check should have extra columns with extras and ignore missing")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column should have extra columns with extras and ignore missing")
        self.assertTrue(list(df_test.columns) == list(df_check.columns),
                        "factor_column resulting df should have correct columns with extras and ignore missing")
        self.assertTrue(np.array_equal(df_test.to_numpy(), df_check.to_numpy()),
                        "factor_column should have expected values with extras and ignore missing")

    def test_valid_factors_extras_no_ignore(self):
        parms = json.loads(self.json_parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
        parms["factor_values"] = ["succesful_stop", "unsuccesful_stop", "face"]
        parms["factor_names"] = ["stopped", "stop_failed", "baloney"]
        parms["ignore_missing"] = False
        df_check["baloney"] = [0, 0, 0, 0, 0, 0]
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = factor_column(df_test, parms)
        self.assertEqual(len(df_check), len(df_test),
                         "factor_column should not change number of rows with extras and ignore missing")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column check should have extra columns with extras and ignore missing")
        self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
                         "factor_column should have extra columns with extras and ignore missing")
        self.assertTrue(list(df_test.columns) == list(df_check.columns),
                        "factor_column resulting df should have correct columns with extras and ignore missing")
        self.assertTrue(np.array_equal(df_test.to_numpy(), df_check.to_numpy()),
                        "factor_column should have expected values with extras and ignore missing")


if __name__ == '__main__':
    unittest.main()
