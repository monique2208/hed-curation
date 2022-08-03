from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "factor_hed_tags",
    "required_parameters": {
        "factor_name": str,
        "remove_types": list,
        "filter_queries": list,
        "overwrite_existing": bool
    },
    "optional_parameters": {}
}


class FactorHedTagsOp(BaseOp):
    """ Create factors based series of tag filters.

        Notes: The required parameters are
             - factor_name (str)       Column name of the created factor.
             - remove_types (list)     Structural HED tags to be removed (usually *Condition-variable* and *Task*).
             - filterc_queries (list)   Queries to be applied successively as filters.
             - overwrite_existing (bool)  If true, existing an factor column is overwritten.

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
