import json
import pandas as pd
import numpy as np
import unittest
from curation.remodeling.operations.ops import factor_column


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
            "column_name": "trial_type",
            "factor_values": ["succesful_stop", "unsuccesful_stop"],
            "factor_names": ["stopped", "stop_failed"],
            "overwrite_existing": True
        }
        cls.json_parms = json.dumps(base_parameters)


    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_factors(self):
        parms = json.loads(self.json_parms)
        df_new = factor_column(self.df_test, parms)



if __name__ == '__main__':
    unittest.main()
