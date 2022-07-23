import json
import pandas as pd
import numpy as np
import unittest
from curation.remodeling.operations.ops import rename_columns


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        sample_data = [[0.0776, 0.5083, 'go', 'n/a', 0.565, 'correct', 'right', 'female'],
                       [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 0.49, 'correct', 'right', 'female'],
                       [9.5856, 0.5084, 'go', 'n/a', 0.45, 'correct', 'right', 'female'],
                       [13.5939, 0.5083, 'succesful_stop', 0.2, 'n/a', 'n/a', 'n/a', 'female'],
                       [17.1021, 0.5083, 'unsuccesful_stop', 0.25, 0.633, 'correct', 'left', 'male'],
                       [21.6103, 0.5083, 'go', 'n/a', 0.443, 'correct', 'left', 'male']]
        sample_columns = ['onset', 'duration', 'trial_type', 'stop_signal_delay', 'response_time',
                          'response_accuracy', 'response_hand', 'sex']
        cls.sample_data = sample_data
        cls.sample_columns = sample_columns
        cls.df_test = pd.DataFrame(sample_data, columns=sample_columns)
        base_parameters = {
            "column_mapping": {
                "stop_signal_delay": "stop_delay",
                "response_hand": "hand_used",
                "sex": "image_sex"
            },
            "ignore_missing": True
        }
        cls.json_parms = json.dumps(base_parameters)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_no_extras_ignore_missing(self):
        parms = json.loads(self.json_parms)
        df_new = rename_columns(self.df_test, parms)
        renamed_columns = ['onset', 'duration', 'trial_type', 'stop_delay', 'response_time', 'response_accuracy',
                           'hand_used', 'image_sex']
        new_columns = list(df_new.columns)
        self.assertTrue(renamed_columns == new_columns, "rename_columns resulting df should have correct columns")
        df_new_values = df_new.to_numpy()
        df_values = self.df_test.to_numpy()
        self.assertTrue(np.array_equal(df_values, df_new_values), "rename_columns does not change the values")

    def test_valid_extras_ignore_missing(self):
        parms = json.loads(self.json_parms)
        parms["ignore_missing"] = True
        parms["column_mapping"]["random_column"] = "new_random_column"
        df_new = rename_columns(self.df_test, parms)
        renamed_columns = ['onset', 'duration', 'trial_type', 'stop_delay', 'response_time', 'response_accuracy',
                           'hand_used', 'image_sex']
        new_columns = list(df_new.columns)
        self.assertTrue(renamed_columns == new_columns, "rename_columns resulting df should have correct columns")
        df_new_values = df_new.to_numpy()
        df_values = self.df_test.to_numpy()
        self.assertTrue(np.array_equal(df_values, df_new_values), "rename_columns does not change the values")

    def test_valid_no_extras(self):
        parms = json.loads(self.json_parms)
        parms["ignore_missing"] = False
        df_new = rename_columns(self.df_test, parms)
        renamed_columns = ['onset', 'duration', 'trial_type', 'stop_delay', 'response_time', 'response_accuracy',
                           'hand_used', 'image_sex']
        new_columns = list(df_new.columns)
        self.assertTrue(renamed_columns == new_columns,
                        "rename_columns resulting df should have correct columns when ignore_missing is false")
        df_new_values = df_new.to_numpy()
        df_values = self.df_test.to_numpy()
        self.assertTrue(np.array_equal(df_values, df_new_values),
                        "rename_columns does not change the values when ignore_missing is false")

    def test_invalid_extras(self):
        parms = json.loads(self.json_parms)
        parms["ignore_missing"] = False
        parms["column_mapping"]["random_column"] = "new_random_column"
        with self.assertRaises(KeyError):
            rename_columns(self.df_test, parms),


if __name__ == '__main__':
    unittest.main()
