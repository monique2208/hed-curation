def remove_columns(df, parameters):
    """ Remove indicated columns from the dataframe.

    Args:
        df (DataFrame) - The DataFrame whose columns are to be removed.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError if ignore_missing is false and

    Notes:
        - remove_names (list)      The names of the columns to be removed.
        - ignore_missing (boolean) If true, the names in remove_names that are not columns in df should be ignored.

    """
    # TODO: needs argument checking and unit testing
    remove_names = parameters['remove_names']
    ignore_missing = parameters['ignore_missing']
    if ignore_missing:
        error_handling = 'ignore'
    else:
        error_handling = 'raise'
    return df.drop(remove_names, axis=1, errors=error_handling)