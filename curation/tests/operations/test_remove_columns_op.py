import json
import pandas as pd
import unittest
from curation import remove_columns


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sample_data = [[0.0776, 0.5083, 'go', 'n/a', 0.565, 'correct', 'right', 'female'],
                           [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 0.49, 'correct', 'right', 'female'],
                           [9.5856, 0.5084, 'go', 'n/a', 0.45, 'correct', 'right', 'female'],
                           [13.5939, 0.5083, 'succesful_stop', 0.2, 'n/a', 'n/a', 'n/a', 'female'],
                           [17.1021, 0.5083, 'unsuccesful_stop', 0.25, 0.633, 'correct', 'left', 'male'],
                           [21.6103, 0.5083, 'go', 'n/a', 0.443, 'correct', 'left', 'male']]
        cls.sample_columns = ['onset', 'duration', 'trial_type', 'stop_signal_delay', 'response_time',
                              'response_accuracy', 'response_hand', 'sex']

        base_parameters = {
            "remove_names": ["stop_signal_delay", "response_accuracy"],
            "ignore_missing": True
        }
        cls.json_parms = json.dumps(base_parameters)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_no_extras_ignore_missing(self):
        parms = json.loads(self.json_parms)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_new = remove_columns(df_test, parms)
        remaining_columns = ['onset', 'duration', 'trial_type', 'response_time', 'response_hand', 'sex']
        new_columns = list(df_new.columns)
        self.assertTrue(remaining_columns == new_columns, "remove_columns resulting df should have correct columns")
        self.assertEqual(len(df_test), len(df_new), "remove_columns should not change the number of events")

    def test_valid_extras_ignore_missing(self):
        parms = json.loads(self.json_parms)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        len_before = len(parms)
        parms["remove_names"].append("face")
        self.assertEqual(len_before+1, len(parms["remove_names"]),
                         "remove_columns adding extra column to remove is okay")
        df_new = remove_columns(df_test, parms)
        remaining_columns = ['onset', 'duration', 'trial_type', 'response_time', 'response_hand', 'sex']
        new_columns = list(df_new.columns)
        self.assertTrue(remaining_columns == new_columns, "remove_columns resulting df should have correct columns")
        self.assertEqual(len(df_test), len(df_new), "remove_columns should not change the number of events")

    def test_valid(self):
        parms = json.loads(self.json_parms)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        parms["ignore_missing"] = False
        df_new = remove_columns(df_test, parms)
        remaining_columns = ['onset', 'duration', 'trial_type', 'response_time', 'response_hand', 'sex']
        new_columns = list(df_new.columns)
        self.assertTrue(remaining_columns == new_columns, "remove_columns resulting df should have correct columns")
        self.assertEqual(len(df_test), len(df_new), "remove_columns should not change the number of events")

    def test_invalid_extras(self):
        parms = json.loads(self.json_parms)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        len_before = len(parms)
        parms["remove_names"].append("face")
        self.assertEqual(len_before + 1, len(parms["remove_names"]),
                         "remove_columns adding extra column to remove is okay")
        parms["ignore_missing"] = False
        with self.assertRaises(KeyError):
            remove_columns(df_test, parms),


if __name__ == '__main__':
    unittest.main()
