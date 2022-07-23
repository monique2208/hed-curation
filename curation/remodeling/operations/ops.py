import pandas as pd
import numpy as np
import copy
import curation.remodeling.operations.base as base
import curation.remodeling.operations.structure as structure
from hed.errors import HedFileError
# from curation.remapping.operations.base import get_indices, is_number, split_consecutive_parts, tuple_to_range
# from curation.remapping.operations.structure import add_column_value, add_summed_events
# TODO: eventually the parameters parsing should be pulled out with a schema.

def run_operations(df, operations_list):
    """ Run operations from list on a dataframe.

    Args:
        df (DataFrame)          A dataframe containing the file to be remodeled.
        operations_list (list)  A list of dictionaries representing commands

    """

    # string to functions
    dispatch = {
                'add_structure_columns': add_structure_columns,
                'add_structure_events': add_structure_events,
                'add_structure_numbers': add_structure_numbers,
                'derive_column': derive_column,
                'factor_column': factor_column,
                'factor_hed': factor_hed,
                'merge_events': merge_events,
                'reorder_columns': reorder_columns,
                'rename_columns': rename_columns,
                'remove_columns': remove_columns,
                'remove_rows': remove_rows,
                'split_events': split_events,
                }

    df = prep_events(df)
    for operation in operations_list:
        command = operation["command"]
        parameters = operation["parameters"]
        df = dispatch[command](df, parameters)
    df = df.fillna('n/a')
    return df


def add_structure_columns(df, structure_dict):
    # TODO: this is not written
    dispatch = {'structure_column': structure.add_column_value,
                'structure_event': structure.add_summed_events}

    for element in structure_dict:
        process = structure_dict[element]
        indices = base.tuple_to_range(base.get_indices(df, process['marker_column'], process['marker_list']))
        column = (process['new_column'] if 'new_column' in process else process['marker_column'])
        value = (process['label'] if 'label' in process else element)
        number = process['number']
        df = dispatch[process.pop('type')](df, indices, column, value, number)
    return df


def add_structure_events(df, structure_dict):
    # TODO: This is not written
    dispatch = {'structure_column': structure.add_column_value,
                'structure_event': structure.add_summed_events}

    for element in structure_dict:
        process = structure_dict[element]
        indices = base.tuple_to_range(base.get_indices(df, process['marker_column'], process['marker_list']))
        column = (process['new_column'] if 'new_column' in process else process['marker_column'])
        value = (process['label'] if 'label' in process else element)
        number = process['number']
        df = dispatch[process.pop('type')](df, indices, column, value, number)
    return df


def add_structure_numbers(df, trial_dict):
    # TODO: this is not written
    df['trial'] = np.nan
    block_indices = base.tuple_to_range(base.get_indices(df, trial_dict['block']['marker_column'],
                                                         trial_dict['block']['marker_list']))
    trial_indices = base.tuple_to_range(base.get_indices(df, trial_dict['trial']['marker_column'],
                                                         trial_dict['trial']['marker_list']))

    block_separated = []
    for block_sublist in block_indices:
        new_block_list = []
        for trial_sublist in trial_indices:
            if trial_sublist[0] in block_sublist:
                new_block_list.append(trial_sublist)
        block_separated.append(new_block_list)

    for block in block_separated:
        count = 0
        for trial in block:
            count += 1
            df.loc[trial, 'trial'] = count
    return df


def derive_column(df, derive_dict):
    # TODO: this has not been rewritten
    """ Add column based on other columns as specified by dictionary."""

    for column in derive_dict:
        new_column = pd.Series(np.nan, index=range(len(df)))

        combination_index = 0
        indexes_index = 1

        mapping_source = 1
        mapping_value = 0

        for comb in df.groupby(
                derive_dict[column]['source_columns']).groups.items():
            for derive_map in derive_dict[column]['mapping']:
                # for each key value pair, check if equal to a specific mapping
                if ((set(comb[combination_index]) == set(derive_map[mapping_source]))
                        or (comb[combination_index] == derive_map[mapping_source][0])):
                    entry = derive_map[mapping_value]
                    new_column.loc[comb[indexes_index]] = entry

        new_column = new_column.dropna(axis=0)
        if column not in df.columns:
            df[column] = np.nan
        df.loc[new_column.index.tolist(), column] = new_column
    return df


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
        - column_name is the name of a column in the dataframe.
        - factor_values is a list of the values in column_name to create factors for.
        - factor_names is a list of the names to use as the factor columns.
        - overwrite_existing is a boolean indicating an existing factor column should be overwritten.

    """
    # TODO: Better handling of argument checking and unit testing
    # TODO: Implementation in process
    column_name = parameters['column_name']
    factor_values = parameters['factor_values']
    factor_names = parameters['factor_names']
    overwrite_existing = parameters['overwrite_existing']
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
    # for factor_value in factor_values:
    #     factor_index =
    print(f"{factor_values}: {factor_names}")
    return df


def factor_hed (df, parameters):
    # TODO: this has not be written
    return df


def merge_events(df, merge_dict):
    # TODO: this has not be rewritten
    consecutive_list = base.split_consecutive_parts(
        df.index[df[merge_dict['column']] == merge_dict['value']].tolist())
    for series in consecutive_list:
        df.loc[series[0], 'duration'] = sum(df['duration'][series])
        df.loc[series[1:], 'duration'] = 'to_remove'
    df = df[df.duration != 'to_remove']
    df = df.reset_index(drop=True)
    return df


def prep_events(df):
    """ Replace all n/a entries in the data frame by np.NaN for processing.

    Args:
        df (DataFrame) - The DataFrame to be processed.

    """
    df = df.replace('n/a', np.NaN)
    return df


def rename_columns(df, parameters):
    """ Rename columns as specified in column_mapping dictionary.

    Args:
        df (DataFrame) - The DataFrame whose columns are to be renamed.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError - when ignore_missing is false and column_mapping has columns not in df.

    Notes:
        - column_mapping is a dictionary of old column name to new column name.
        - ignore_missing is a boolean indicating whether old column names must be in df.

    """
    # TODO: needs argument checking
    column_mapping = parameters['column_mapping']
    ignore_missing = parameters['ignore_missing']
    if ignore_missing:
        error_handling = 'ignore'
    else:
        error_handling = 'raise'
    return df.rename(columns=column_mapping, errors=error_handling)


def remove_rows(df, parameters):
    """ Removes rows with the values indicated in the columns.

    Args:
        df (DataFrame) - The DataFrame whose rows are to be removed.
        parameters (dict) - Dictionary of parameters.

    Raises:

    Notes:
        - column_name is name of column to be tested.
        - remove_values is a list of values to test for row removal.
        - if column_name is not a column in df, df is just returned.

    """
    # TODO: needs argument checking and unit testing
    column = parameters["column_name"]
    remove_values = parameters["remove_values"]
    if column not in df.columns:
        return df
    for value in remove_values:
        df = df.loc[df[column] != value, :]
    return df


def remove_columns(df, parameters):
    """ Remove indicated columns from the dataframe.

    Args:
        df (DataFrame) - The DataFrame whose columns are to be removed.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError if ignore_missing is false and

    Notes:
        - remove_names is a list of columns to be removed.
        - ignore_missing indicates whether names in remove_names that are not columns in df should be ignored.

    """
    # TODO: needs argument checking and unit testing
    remove_names = parameters['remove_names']
    ignore_missing = parameters['ignore_missing']
    if ignore_missing:
        error_handling = 'ignore'
    else:
        error_handling = 'raise'
    return df.drop(remove_names, axis=1, errors=error_handling)


def reorder_columns(df, parameters):
    """ Reorders columns as specified in event dictionary. """
    # TODO needs rewriting
    column_order = parameters['column_order']
    ignore_missing = parameters['ignore_missing']
    # TODO this doesn't handle ignore_missing yet
    df = df.loc[:, column_order]
    return df


def split_events(df, parameters):
    """ Splits a row representing a particular event into multiple rows.

    Args:
        df (DataFrame) - The DataFrame whose rows are to be split.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError if ignore_missing is false and

    Notes:
        - remove_names is a list of columns to be removed.
        - ignore_missing indicates whether names in remove_names that are not columns in df should be ignored.

    """

    # Do we need to check that the new column doesn't exist
    # TODO: this only handles parent events that are all trials
    # TODO: needs unit testing and argument checking
    anchor_column = parameters["anchor_column"]
    new_events = parameters["new_events"]
    add_trial_numbers = parameters["add_trial_numbers"]
    remove_parent_event = parameters["remove_trial_parent"]
    if add_trial_numbers:
        df['trial_number'] = df.index+1

    if anchor_column not in df.columns:
        df[anchor_column] = np.nan
    # new_df = pd.DataFrame('n/a', index=range(len(df.index)), columns=df.columns)
    if remove_parent_event:
        df_list = []
    else:
        df_list = [df]
    for event, event_parms in new_events.items():
        add_events = pd.DataFrame([], columns=df.columns)
        onsets = df['onset']

        for onset in event_parms['onset_source']:
            if isinstance(onset, float) or isinstance(onset, int):
                onsets = onsets + onset
            elif isinstance(onset, str) and onset in list(df.columns):
                onsets = onsets + df[onset]
            else:
                raise HedFileError("BadOnsetInModel",
                                   f"Remodeling onset {str(onset)} must either be numeric or a column name", "")
        add_events['onset'] = onsets
        add_events[anchor_column] = event
            # remove events if there is no onset (=no response)
        duration = event_parms['duration']
        if isinstance(duration, float) or isinstance(duration, int):
            add_events['duration'] = duration
        elif isinstance(duration, str) and duration in list(df.columns):
            add_events['duration'] = df[duration]
        else:
            raise HedFileError("BadDurationInModel",
                               f"Remodeling onset {str(duration)} must either be numeric or a column name", "")

        if len(event_parms['copy_columns']) > 0:
            for column in event_parms['copy_columns']:
                add_events[column] = df[column]


        # add_events['event_type'] = event
        add_events = add_events.dropna(axis='rows', subset=['onset'])
        df_list.append(add_events)

    df_new = pd.concat(df_list, axis=1)
    df_new = df_new.sort_values('onset').reset_index(drop=True)
    return df_new
