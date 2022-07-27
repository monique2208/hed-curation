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

    """

            Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.
    """

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)

    def do_op(self, df, hed_schema=None, sidecar=None):
        """ Factor the column using the tags

        Args:
            df (DataFrame) - The DataFrame to be remodeled.
            hed_schema (HedSchema or HedSchemaGroup) Only needed for HED operations.
            sidecar (Sidecar or file-like)   Only needed for HED operations

        Returns:
            Dataframe - a new dataframe after processing.

        """

        return df
