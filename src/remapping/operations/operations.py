import pandas as pd
import numpy as np
import copy
from operations.util import base
from operations.util import structure


def run_operations(df, operations_list):
    '''Takes an events dataframe and a list of dictionaries and runs a series
    of operations as specified in the dictionaries.'''

    # string to functions
    dispatch = {'split_events': split_trial_events,
                'derive_columns': derive_columns,
                'rename': rename_columns,
                'remove': remove_columns,
                'order': order_columns,
                'merge_consecutive_events': merge_consecutive_events,
                'add_structure': add_structure,
                'trial_numbers': add_trial_numbers
                }

    df = prep_events(df)
    for operation_dict in operations_list:
        operation_key = list(operation_dict.keys())[0]
        # deepcopy for pop in split events
        df = dispatch[operation_key](
            df, copy.deepcopy(operation_dict[operation_key])
                                     )
        print('FINISHED %s' % operation_key)
        print(df.head())
        print('')
    df = df.fillna('n/a')
    return df


def merge_consecutive_events(df, merge_dict):
    consecutive_list = base.split_consecutive_parts(
        df.index[df[merge_dict['column']] == merge_dict['value']].tolist())
    for series in consecutive_list:
        df.loc[series[0], 'duration'] = sum(df['duration'][series])
        df.loc[series[1:], 'duration'] = 'to_remove'
    df = df[df.duration != 'to_remove']
    df = df.reset_index(drop=True)
    return df


def derive_columns(df, derive_dict):
    '''Add a new column based on values in other columns as specified by an
    event dictionary.'''

    for column in derive_dict:
        new_column = pd.Series(np.nan, index=range(len(df)))

        combination_index = 0
        indexes_index = 1

        mapping_source = 1
        mapping_value = 0

        for comb in df.groupby(
                derive_dict[column]['source_columns']).groups.items():
            for map in derive_dict[column]['mapping']:
                # for each key value pair, check if equal to a specific mapping
                if ((set(comb[combination_index]) == set(map[mapping_source]))
                        or (comb[combination_index] == map[mapping_source][0])):
                    entry = map[mapping_value]
                    new_column.loc[comb[indexes_index]] = entry

        new_column = new_column.dropna(axis=0)
        if column not in df.columns:
            df[column] = np.nan
        df.loc[new_column.index.tolist(), column] = new_column
    return df


def remove_columns(df, remove_dict):
    '''Remove columns as specified in event dictionary'''
    return df.drop(remove_dict, axis=1)


def split_trial_events(df, split_dict):
    '''Splits trial row into events based on an event dictionary.'''

    df['trial_number'] = df.index+1

    add_column = 'event_type'
    add_column_location = 3
    remove_trial_parent = split_dict.pop('remove_trial_parent')

    df.insert(add_column_location, add_column, np.repeat(np.nan, len(df)))
    new_events = pd.DataFrame([], columns=df.columns)

    for event in split_dict:
        add_events = pd.DataFrame([], columns=df.columns)
        print('Adding event %s \n' % event)
        print('')

        if base.is_number(split_dict[event]['onset_source']):
            add_events['onset'] = (df['onset'] +
                                   split_dict[event]['onset_source'])

        elif isinstance(split_dict[event]['onset_source'], str):
            add_events['onset'] = (df['onset'] +
                                   df[split_dict[event]['onset_source']])
            # remove events if there is no onset (=no response)
            add_events = add_events.dropna(axis='rows', subset=['onset'])

        elif isinstance(split_dict[event]['onset_source'], list):
            add_events['onset'] = (df['onset'] +
                                   df[split_dict[event]['onset_source'][0]] +
                                   split_dict[event]['onset_source'][1])

            add_events = add_events.dropna(axis='rows', subset=['onset'])

        if base.is_number(split_dict[event]['duration']):
            add_events['duration'] = split_dict[event]['duration']
        elif isinstance(split_dict[event]['duration'], str):
            add_events['duration'] = df[split_dict[event]['duration']]

        if len(split_dict[event]['copy_columns']) > 0:
            for column in split_dict[event]['copy_columns']:
                add_events[column] = df[column]
        add_events['trial_number'] = df['trial_number']

        if len(split_dict[event]['move_columns']) > 0:
            for column in split_dict[event]['move_columns']:
                add_events[column] = df[column]
                df[column] = np.NaN

        add_events['event_type'] = event
        new_events = new_events.append(add_events)

    df = df.append(new_events)

    if remove_trial_parent:
        df = df.dropna(axis='rows', subset=['event_type'])

    df = df.sort_values('onset').reset_index(drop=True)
    return df


def prep_events(df):
    df = df.replace('n/a', np.NaN)
    return df


def rename_columns(df, rename_dict):
    '''Rename columns as specified in event dictionary'''
    return df.rename(columns=rename_dict)


def order_columns(df, order_dict):
    '''Reorder columns as specified in event dictionary'''
    return df[order_dict]


def add_structure(df, structure_dict):
    dispatch = {'structure_column': structure.add_column_value,
                'structure_event': structure.add_summed_events}

    for element in structure_dict:
        process = structure_dict[element]
        indices = base.tuple_to_range(
            base.get_indices(df,
                             process['marker_column'],
                             process['marker_list']))
        column = (process['new_column'] if 'new_column' in
                  process else
                  process['marker_column'])
        value = (process['label'] if 'label' in process else
                 element)
        number = process['number']
        df = dispatch[process.pop('type')](df, indices, column, value, number)
    return df


def add_trial_numbers(df, trial_dict):
    df['trial'] = np.nan
    block_indices = base.tuple_to_range(base.get_indices(
        df,
        trial_dict['block']['marker_column'],
        trial_dict['block']['marker_list']
                                                         )
                                        )
    trial_indices = base.tuple_to_range(base.get_indices(
        df,
        trial_dict['trial']['marker_column'],
        trial_dict['trial']['marker_list']
                                                         )
                                        )

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
