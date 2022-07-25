def reorder_columns(df, parameters):
    """ Reorders columns as specified in event dictionary. """
    # TODO needs rewriting
    column_order = parameters['column_order']
    ignore_missing = parameters['ignore_missing']
    # TODO this doesn't handle ignore_missing yet
    df = df.loc[:, column_order]
    return df