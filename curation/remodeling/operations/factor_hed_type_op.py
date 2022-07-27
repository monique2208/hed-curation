from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "factor_hed_type",
    "required_parameters": {
        "type_tag": str,
        "type_values": list,
        "overwrite_existing": bool
    },
    "optional_parameters": {}
}


class FactorHedTypeOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.type_tag = parameters["type_tag"]
        self.type_values = parameters["type_values"]
        self.overwrite_existing = parameters["overwrite_existing"]

    def do_op(self, df, hed_schema=None, sidecar=None):
        """ Factor columns based on HED type.

        Args:
            df (DataFrame) - The DataFrame whose rows are to be removed.

        Returns
            DataFrame - a new DataFame with that includes the factors

        Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.

        """

        return df


