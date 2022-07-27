from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "factor_column",
    "required_parameters": {
        "column_name": str,
        "factor_values": list,
        "factor_names": list,
        "ignore_missing": bool,
        "overwrite_existing": bool
    },
    "optional_parameters": {}
}


class FactorColumnOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.column_name = parameters['column_name']
        self.factor_values = parameters['factor_values']
        self.factor_names = parameters['factor_names']
        self.overwrite_existing = parameters['overwrite_existing']
        self.ignore_missing = parameters['ignore_missing']
        if len(self.factor_values) != len(self.factor_names):
            raise ValueError("FactorNameLenBad", "The lengths of factor_values and factor_names must be the same.")

    def do_op(self, df):
        """ Create factor columns corresponding to values in a specified column.

        Args:
            df (DataFrame) - The DataFrame to be factored.

        Raises:
            ValueError:
                - If lengths of factor_values and factor_names are not the same.
                - If factor_name is already a column and overwrite_existing is False.

        Notes:
            - column_name (string)  The name of a column in the dataframe.
            - factor_values (list)  Values in the column column_name to create factors for.
            - factor_names (list)   Names to use as the factor columns.
            - overwrite_existing (boolean) If true, overwrite an existing factor column.
            - ignore_missing (boolean) If true, a factor value not appear column column_name should not be factored.

        """

        if len(self.factor_values) == 0:
            factor_values = df[self.column_name].unique()
            factor_names = [self.column_name + '.' + str(column_value) for column_value in factor_values]
        if not self.overwrite_existing:
            overlap = set(df.columns).intersection(set(self.factor_names))
            if overlap:
                raise ValueError("FactorColumnsExist",
                                 f"Factor columns {str(overlap)} in dataframe and overwrite_existing is false")

        df_new = df.copy()
        for index, factor_value in enumerate(self.factor_values):
            factor_index = df_new[self.column_name].map(str).isin([str(factor_value)])
            column = self.factor_names[index]
            df_new[column] = factor_index.astype(int)
        return df_new
