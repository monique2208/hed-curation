from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "factor_hed_tags",
    "required_parameters": {
        "column_name": str,
        "remove_values": list
    },
    "optional_parameters": {}
}


class FactorHedTagsOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)

    def do_op(self, df):
        """ Factor.

        Args:
            df (DataFrame) - The DataFrame whose rows are to be removed.

        Raises:

        Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.

        """

        return df
