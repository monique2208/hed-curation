import os
import json
import pandas as pd
import numpy as np
import unittest
import hed.schema as hedschema
from curation.remodeling.operations.factor_hed_type_op import FactorHedTypeOp
from hed import TabularInput


class Test(unittest.TestCase):
    """

    TODO: Test when no factor names and values are given.

    """
    @classmethod
    def setUpClass(cls):
        cls.data_path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                         '../data/sub-002_task-FacePerception_run-1_events.tsv'))
        cls.json_path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                         '../data/task-FacePerception_events.json'))
        base_parameters = {
            "type_tag": "Condition-variable",
            "type_values": [],
            "overwrite_existing": False,
            "factor_encoding": "one-hot"
        }
        cls.json_parms = json.dumps(base_parameters)
        cls.hed_schema = hedschema.load_schema_version('8.1.0')

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_using_files(self):
        # Test correct when all valid and no unwanted information
        parms = json.loads(self.json_parms)
        op = FactorHedTypeOp(parms)

        input_data = TabularInput(self.data_path, hed_schema=self.hed_schema, sidecar=self.json_path)
        df_new = op.do_op(self.data_path, hed_schema=self.hed_schema, sidecar=self.json_path)

    # def test_valid_factors_no_extras_no_ignore(self):
    #     # Test when no extras and extras not ignored.
    #     parms = json.loads(self.json_parms)
    #     parms["ignore_missing"] = False
    #     op = FactorColumnOp(parms)
    #     df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
    #     df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_new = op.do_op(df_test)
    #     self.assertEqual(len(df_check), len(df_new),
    #                      "factor_column should not change number of rows with no extras and no ignore")
    #     self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
    #                      "factor_column check should have extra columns with no extras and no ignore")
    #     self.assertTrue(list(df_new.columns) == list(df_check.columns),
    #                     "factor_column resulting df should have correct columns with no extras and no ignore")
    #     self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
    #                     "factor_column should have expected values when no extras and no ignore")
    #
    #     # Test that df has not been changed by the op
    #     self.assertTrue(list(df.columns) == list(df_test.columns),
    #                     "factor_column should not change the input df columns when no extras and no ignore missing")
    #     self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
    #                     "factor_column should not change the input df values when no extras and no ignore missing")
    #
    # def test_valid_factors_extras_ignore(self):
    #     # Test when extra factor values but ignored
    #     parms = json.loads(self.json_parms)
    #     df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
    #     parms["factor_values"] = ["succesful_stop", "unsuccesful_stop", "face"]
    #     parms["factor_names"] = ["stopped", "stop_failed", "baloney"]
    #     op = FactorColumnOp(parms)
    #     df_check["baloney"] = [0, 0, 0, 0, 0, 0]
    #     df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_new = op.do_op(df_test)
    #     self.assertEqual(len(df_check), len(df_new),
    #                      "factor_column should not change number of rows with extras and ignore missing")
    #     self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
    #                      "factor_column check should have extra columns with extras and ignore missing")
    #     self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
    #                      "factor_column should have extra columns with extras and ignore missing")
    #     self.assertTrue(list(df_new.columns) == list(df_check.columns),
    #                     "factor_column resulting df should have correct columns with extras and ignore missing")
    #     self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
    #                     "factor_column should have expected values with extras and ignore missing")
    #
    #     # Test that df has not been changed by the op
    #     self.assertTrue(list(df.columns) == list(df_test.columns),
    #                     "factor_column should not change the input df columns when extras and no ignore missing")
    #     self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
    #                     "factor_column should not change the input df values when extras and no ignore missing")
    #
    # def test_valid_factors_extras_no_ignore(self):
    #     # Test when extra factors are included but not ignored.
    #     parms = json.loads(self.json_parms)
    #     df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_check = pd.DataFrame(self.factored, columns=self.sample_columns + parms['factor_names'])
    #     parms["factor_values"] = ["succesful_stop", "unsuccesful_stop", "face"]
    #     parms["factor_names"] = ["stopped", "stop_failed", "baloney"]
    #     parms["ignore_missing"] = False
    #     op = FactorColumnOp(parms)
    #     df_check["baloney"] = [0, 0, 0, 0, 0, 0]
    #     df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
    #     df_new = op.do_op(df_test)
    #     self.assertEqual(len(df_check), len(df_new),
    #                      "factor_column should not change number of rows with extras and ignore missing")
    #     self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
    #                      "factor_column check should have extra columns with extras and ignore missing")
    #     self.assertEqual(len(df_check.columns), len(df.columns) + len(parms["factor_values"]),
    #                      "factor_column should have extra columns with extras and ignore missing")
    #     self.assertTrue(list(df_new.columns) == list(df_check.columns),
    #                     "factor_column resulting df should have correct columns with extras and ignore missing")
    #     self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
    #                     "factor_column should have expected values with extras and ignore missing")
    #
    #     # Test that df has not been changed by the op
    #     self.assertTrue(list(df.columns) == list(df_test.columns),
    #                     "factor_column should not change the input df columns when extras and no ignore missing")
    #     self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
    #                     "factor_column should not change the input df values when extras and no ignore missing")


if __name__ == '__main__':
    unittest.main()
