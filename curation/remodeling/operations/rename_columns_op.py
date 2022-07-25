def rename_columns(df, parameters):
    """ Rename columns as specified in column_mapping dictionary.

    Args:
        df (DataFrame) - The DataFrame whose columns are to be renamed.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError - when ignore_missing is false and column_mapping has columns not in df.

    Notes:
        - column_mapping (dict)     Mapping of old column names to new column names.
        - ignore_missing (boolean)  If true, old column names must be in df.

    """
    # TODO: needs argument checking
    column_mapping = parameters['column_mapping']
    ignore_missing = parameters['ignore_missing']
    if ignore_missing:
        error_handling = 'ignore'
    else:
        error_handling = 'raise'
    return df.rename(columns=column_mapping, errors=error_handling)