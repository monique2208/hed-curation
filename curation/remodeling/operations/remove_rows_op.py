def remove_rows(df, parameters):
    """ Removes rows with the values indicated in the columns.

    Args:
        df (DataFrame) - The DataFrame whose rows are to be removed.
        parameters (dict) - Dictionary of parameters.

    Raises:

    Notes:
        - column_name (str)     The name of column to be tested.
        - remove_values (list)  The values to test for row removal.

    If column_name is not a column in df, df is just returned.

    """
    # TODO: needs argument checking and unit testing
    column = parameters["column_name"]
    remove_values = parameters["remove_values"]
    if column not in df.columns:
        return df
    for value in remove_values:
        df = df.loc[df[column] != value, :]
    return df