def factor_column(df, parameters):
    """ Create factor columns corresponding to values in a specified column.

    Args:
        df (DataFrame) - The DataFrame to be factored.
        parameters (dict) - Dictionary of parameters.

    Raises:
        ValueError:
            - If lengths of factor_values and factor_names are not the same.
            - If factor_name is already a column and overwrite_existing is False.

    Notes:
        - column_name (string)  The name of a column in the dataframe.
        - factor_values (list)  Values in the column column_name to create factors for.
        - factor_names (list)   Names to use as the factor columns.
        - overwrite_existing (boolean) If true, overwrite an existing factor column.
        - ignore_missing (boolean) If true, a factor value that does not appear in column_name should not be factored.

    """
    # TODO: Better handling of argument checking and unit testing
    # TODO: Implementation in process

    column_name = parameters['column_name']
    factor_values = parameters['factor_values']
    factor_names = parameters['factor_names']
    overwrite_existing = parameters['overwrite_existing']
    ignore_missing = parameters['ignore_missing']
    if len(factor_values) != len(factor_names):
        raise ValueError("FactorNameLenBad", "The lengths of factor_values and factor_names must be the same.")
    elif len(factor_values) == 0:
        factor_values = df[column_name].unique()
        factor_names = [column_name + '.' + str(column_value) for column_value in factor_values]
    if not overwrite_existing:
        overlap = set(df.columns).intersection(set(factor_names))
        if overlap:
            raise ValueError("FactorColumnsExist",
                             f"Factor columns {str(overlap)} are already in dataframe and overwrite_existing is false")
    df_new = df.copy()
    for index, factor_value in enumerate(factor_values):
        factor_index = df[column_name].map(str).isin([str(factor_value)])
        column = factor_names[index]
        df_new[column] = factor_index.astype(int)
    return df_new