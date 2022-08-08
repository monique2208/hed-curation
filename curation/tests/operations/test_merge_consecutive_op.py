import json
import numpy as np
import pandas as pd
import unittest
from curation import MergeConsecutiveOp


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sample_data = [[0.0776, 0.5083, 'go', 'n/a', 'right', 'female'],
                           [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 'right', 'female'],
                           [9.5856, 0.5084, 'go', 'n/a', 'right', 'female'],
                           [13.5939, 0.5083, 'succesful_stop', 0.2, 'n/a', 'female'],
                           [14.2, 0.5083, 'succesful_stop', 0.2,  'n/a', 'female'],
                           [15.3, 0.5083, 'succesful_stop', 0.2,  'n/a', 'female'],
                           [17.3, 0.5083, 'succesful_stop', 0.25,  'n/a', 'female'],
                           [19.0, 0.5083, 'succesful_stop', 0.25, 'n/a', 'female'],
                           [21.1021, 0.5083, 'unsuccesful_stop', 0.25, 'left', 'male'],
                           [22.6103, 0.5083, 'go', 'n/a', 'left', 'male']]
        cls.sample_columns = ['onset', 'duration', 'trial_type', 'stop_signal_delay',  'response_hand', 'sex']

        cls.result_data = [[0.0776, 0.5083, 'go', 'n/a', 'right', 'female'],
                           [5.5774, 0.5083, 'unsuccesful_stop', 0.2, 'right', 'female'],
                           [9.5856, 0.5084, 'go', 'n/a', 'right', 'female'],
                           [13.5939, 2.2144, 'succesful_stop', 0.2, 'n/a', 'female'],
                           [17.3, 2.2083, 'succesful_stop', 0.25,  'n/a', 'female'],
                           [21.1021, 0.5083, 'unsuccesful_stop', 0.25, 'left', 'male'],
                           [22.6103, 0.5083, 'go', 'n/a', 'left', 'male']]

        base_parameters = {
                "column_name": "trial_type",
                "event_code": "succesful_stop",
                "match_columns": ['stop_signal_delay', 'response_hand', 'sex'],
                "set_durations": True,
                "ignore_missing": True
            }
        cls.json_parms = json.dumps(base_parameters)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid(self):
        # Test when no extras but ignored.
        parms = json.loads(self.json_parms)
        op = MergeConsecutiveOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df = df.replace('n/a', np.NaN)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = df_test.replace('n/a', np.NaN)
        x = np.array_equal(df.to_numpy(), df_test.to_numpy())
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
                        "merge_consecutive should not change the input df values")
        df_new = op.do_op(df_test)
        self.assertTrue(list(df_new.columns) == list(df.columns),
                        "merge_consecutive should not change the number of columns")
        df_results = pd.DataFrame(self.result_data, columns=self.sample_columns)
        df_results.replace('n/a', np.NaN)
        self.assertTrue(list(df_new.columns) == list(df_results.columns),
                        "merge_consecutive should has correct number of columns after merge")
        for index, row in df_new.iterrows():
            if df_new.loc[index, "onset"] != df_results.loc[index, "onset"]:
                print(f"onset: {index} {df_new.loc[index, 'onset']} {df_results.loc[index, 'onset']}")
            if df_new.loc[index, "onset"] != df_results.loc[index, "onset"]:
                print(f" duration: {index} {df_new.loc[index, 'duration']} {df_results.loc[index, 'duration']}")

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "merge_consecutive should not change the input df columns when no extras and not ignored")
        x1 = df.to_numpy()
        x2 = df_test.to_numpy()
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy(), equal_nan=True),
                        "merge_consecutive should not change the input df values when no extras and not ignored")

        print("to here")
    # def test_valid_missing(self):
    #     # Test when no extras but ignored.
    #     parms = json.loads(self.json_parms)
    #     before_len = len(parms["map_list"])
    #     parms["map_list"] = parms["map_list"][:-1]
    #     after_len = len(parms["map_list"])
    #     self.assertEqual(after_len + 1, before_len)
    #     op = RemapColumnsOp(parms)
    #     df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_new = op.do_op(df_test)
    #     self.assertNotIn("response_type", df.columns, "remap_columns before does not have response_type column")
    #     self.assertIn("response_type", df_new.columns, "remap_columns after has response_type column")
    #
    # def test_invalid_missing(self):
    #     # Test when no extras but ignored.
    #     parms = json.loads(self.json_parms)
    #     before_len = len(parms["map_list"])
    #     parms["map_list"] = parms["map_list"][:-1]
    #     parms["ignore_missing"] = False
    #     after_len = len(parms["map_list"])
    #     self.assertEqual(after_len + 1, before_len)
    #     op = RemapColumnsOp(parms)
    #     df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     with self.assertRaises(ValueError):
    #         df_new = op.do_op(df_test)


if __name__ == '__main__':
    unittest.main()
