import pandas as pd
import numpy as np
from curation.remodeling.operations.base_op import BaseOp
from hed import TabularInput
from hed.tools import get_assembled_strings, HedVariableManager, HedTypeFactors

# TODO: restricted factor values are not implemented yet.

PARAMS = {
    "command": "factor_hed_type",
    "required_parameters": {
        "type_tag": str,
        "type_values": list,
        "overwrite_existing": bool,
        "factor_encoding": str
    },
    "optional_parameters": {}
}


class FactorHedTypeOp(BaseOp):
    """ Create factors based on a specified type of tag such as Condition-variable.

        Notes: The required parameters are
             - type_tag (str)     HED tag used to find the factors (most commonly Condition-variable).
             - type_values (list) Factor values to include. If empty all values of that type_tag are used.
             - overwrite_existing (bool)  If true, existing factor columns are overwritten.
             - factor_encoding (str)  Type of factor encoding. Valid encodings are 'categorical' and 'one-hot'.

     """

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.type_tag = parameters["type_tag"]
        self.type_values = parameters["type_values"]
        self.overwrite_existing = parameters["overwrite_existing"]
        self.factor_encoding = parameters["factor_encoding"].lower()
        if self.factor_encoding not in HedTypeFactors.ALLOWED_ENCODINGS:
            raise ValueError("BadFactorEncoding",
                             f"{self.factor_encoding} is not in the allowed encodings: " +
                             f"{str(HedTypeFactors.ALLOWED_ENDCODINGS)}")

    def do_op(self, dispatcher, df, name, sidecar=None, verbose=False):
        """ Factor columns based on HED type.

        Args:
            dispatcher (Dispatcher) - dispatcher object for context
            df (DataFrame) - The DataFrame to be remodeled.
            name (str) - Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like)   Only needed for HED operations.
            verbose (bool) If True output informative messages during operation.

        Returns
            DataFrame - a new DataFame with that includes the factors

        Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.

        """

        input_data = TabularInput(df, hed_schema=dispatcher.hed_schema, sidecar=sidecar)
        df = input_data.dataframe.copy()
        df_list = [df]
        hed_strings = get_assembled_strings(input_data, hed_schema=dispatcher.hed_schema, expand_defs=False)

        definitions = input_data.get_definitions()
        var_manager = HedVariableManager(hed_strings, dispatcher.hed_schema, definitions)
        var_manager.add_type_variable(self.type_tag.lower())

        df_factors = var_manager.get_factor_vectors(self.type_tag, [], factor_encoding=self.factor_encoding)
        if len(df_factors.columns) > 0:
            df_list.append(df_factors)
        df_new = pd.concat(df_list, axis=1)
        df_new.replace('n/a', np.NaN, inplace=True)
        return df_new
