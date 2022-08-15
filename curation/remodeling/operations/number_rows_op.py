import numpy as np
from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "add_structure_numbers",
    "required_parameters": {
        "number_column_name": str
    },
    "optional_parameters": {"overwrite": bool, "match_value": dict}
}

class NumberRowsOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.number_column_name = parameters['number_column_name']
        self.overwrite = parameters.get('overwrite', False)
        self.match_value = parameters.get('match_value', False)
        if self.match_value:
            self.match_value_params = {"column": str, "value": str}
            required = set(self.match_value_params.keys())
            required_missing = required.difference(set(self.match_value.keys()))
            if required_missing:
                raise KeyError("MissingRequiredParameters",
                               f"Specified match_value for number_rows requires parameters {list(required_missing)}")
            for param_name, param_value in self.match_value.items():
                if param_name in required:
                    param_type = self.match_value_params[param_name]
                else:
                    raise KeyError("BadParameter",
                                   f"{param_name} not a required or optional parameter for {self.command}")
                if not isinstance(param_value, param_type):
                    raise TypeError("BadType" f"{param_value} has type {type(param_value)} not {param_type}")

    def do_op(self, dispatcher, df, name, sidecar=None, verbose=False):
        """ Add numbers events dataframe.

        Args:
            dispatcher (Dispatcher) - dispatcher object for context
            df (DataFrame) - The DataFrame to be remodeled.
            name (str) - Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like)   Only needed for HED operations.
            verbose (bool) If True output informative messages during operation.

        Returns:
            Dataframe - a new dataframe after processing.

        """
        if self.number_column_name in df.columns:
            if self.overwrite is False:
                raise ValueError("ExistingNumberColumn",
                                 f"Column {self.number_column_name} already exists in event file.", "")

        if self.match_value:
            if self.match_value['column'] not in df.columns:
                raise ValueError("MissingMatchColumn",
                                 f"Column {self.match_value['column']} does not exist in event file.", "")
            if self.match_value['value'] not in df[self.match_value['column']].tolist():
                raise ValueError("MissingMatchValue",
                                 f"Value {self.match_value['value']} does not exist in event file column {self.match_value['column']}.", "")

        df_new = df.copy()
        df_new[self.number_column_name] = np.nan
        if self.match_value:
            filter = df_new[self.match_value['column']] == self.match_value['value']
            numbers = [*range(1, sum(filter)+1)]
            df_new.loc[filter, self.number_column_name] = numbers
        else:
            df_new[self.number_column_name] = df_new.index + 1

        return df_new
