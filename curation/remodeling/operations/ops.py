import pandas as pd
import numpy as np
import curation.remodeling.util.base as base
import curation.remodeling.util.structure as structure
# from curation.remapping.operations.base import get_indices, is_number, split_consecutive_parts, tuple_to_range
# from curation.remapping.operations.structure import add_column_value, add_summed_events
# TODO: eventually the parameters parsing should be pulled out with a schema.
from curation.remodeling.operations.remove_columns_op import remove_columns
from curation.remodeling.operations.reorder_columns_op import reorder_columns
from curation.remodeling.operations.split_events_op import split_events
from curation.remodeling.operations.factor_column_op import factor_column
from curation.remodeling.operations.remove_rows_op import remove_rows
from curation.remodeling.operations.rename_columns_op import rename_columns


def run_operations(df, operations_list):
    """ Run operations from list on a dataframe.

    Args:
        df (DataFrame)          A dataframe containing the file to be remodeled.
        operations_list (list)  A list of dictionaries representing commands

    """

    # string to functions
    dispatch = {
        'add_structure_columns': {
            'command': add_structure_columns,
            'required_args': []
        },
        'add_structure_events': {
            'command': add_structure_events,
            'required_args': []
        },
        'add_structure_numbers': {
            'command': add_structure_numbers,
            'required_args': []
        },
        'derive_column': {
            'command': derive_column,
            'required_args': []
        },
        'factor_column': {
            'command': factor_column,
            'required_args': ["column_name", "factor_values", "factor_names", "ignore_missing", "overwrite_existing"]
        },
        'factor_hed': {
            'command': factor_hed,
            'required_args': []
        },
        'merge_events': {
            'command': merge_events,
            'required_args': []
        },
        'remove_columns': {
            'command': remove_columns,
            'required_args': ["remove_names", "ignore_missing"]
        },
        'remove_rows': {
            'command': remove_rows,
            'required_args': ["column_name", "remove_values"]
        },
        'rename_columns': {
            'command': rename_columns,
            'required_args': ["column_mapping", "ignore_missing"]
        },
        'reorder_columns': {
            'command': reorder_columns,
            'required_args': ["column_order", "ignore_missing"]
        },
        'split_events': {
            'command': split_events,
            'required_args': ["anchor_event", "event_selection", "new_events"]
        }
    }

    df = prep_events(df)
    for operation in operations_list:
        command = operation.get("command", None)
        if command is None or command not in dispatch:
            raise KeyError("InvalidCommand",
                           f"The command parameter {command} is missing or not valid")
        parameters = operation.get("parameters", None)
        if parameters is None:
            raise KeyError("InvalidParameters",
                           f"The parameters argument for command {command} is missing or not valid")
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


def factor_hed(df, parameters):
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


