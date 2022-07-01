import pandas as pd
import numpy as np
import copy
import curation.remapping.operations.base as base
import curation.remapping.operations.structure as structure


def run_operations(df, operations_list):
    """ Runs operations from list on a dataframe."""

    # string to functions
    dispatch = {'split_events': split_trial_events,
                'derive_columns': derive_columns,
                'rename_columns': rename_columns,
                'remove_columns': remove_columns,
                'order_columns': order_columns,
                'merge_consecutive_events': merge_consecutive_events,
                'add_structure': add_structure,
                'add_numbers': add_numbers,
                'remove_rows': remove_rows
                }

    df = prep_events(df)
    for operation_dict in operations_list:
        operation_key = list(operation_dict.keys())[0]
        # deepcopy for pop in split events
        df = dispatch[operation_key](df, copy.deepcopy(operation_dict[operation_key]))
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


def remove_columns(df, remove_dict):
    """ Removes columns as specified by dictionary."""
    return df.drop(remove_dict, axis=1)


def split_trial_events(df, split_dict):
    """ Splits trial row into events based on dictionary. """

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
        onsets = df['onset']
        source_list = split_dict[event]['onset_source']
        for source in source_list:
            if base.is_number(source):
                onsets = onsets + source
            elif isinstance(source, str):
                source_offsets = df[source]
                onsets = onsets + source_offsets

        add_events['onset'] = onsets

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
                df[column] = np.nan

        add_events['event_type'] = event
        add_events = add_events.dropna(axis='rows', subset=['onset'])
        new_events = new_events.append(add_events)

    df = df.append(new_events)

    if remove_trial_parent:
        df = df.dropna(axis='rows', subset=['event_type'])

    df = df.sort_values('onset').reset_index(drop=True)
    return df


def prep_events(df):
    df = df.replace('n/a', np.nan)
    return df


def rename_columns(df, rename_dict):
    """ Renames columns as specified in event dictionary. """
    return df.rename(columns=rename_dict)


def remove_rows(df, remove_dict):
    """ Removes rows with the values indicated in the columns. """

    for column, drop_list in remove_dict.items():
        for drop_val in drop_list:
            df = df.loc[df[column] != drop_val]
    return df


def order_columns(df, order_dict):
    """ Reorders columns as specified in event dictionary. """
    return df[order_dict]


def add_structure(df, structure_dict):
    dispatch = {'column': structure.add_column_value,
                'event': structure.add_summed_events}

    for element in structure_dict:
        df = dispatch[element.pop('type')](df,
                                           element['column'],
                                           element['marker'],
                                           element['value'])
    return df


def add_numbers(df, number_dict):
    for element in number_dict:
        process = number_dict[element]

        new_column = element + '_number'
        df[new_column] = np.nan

        indices = base.tuple_to_range(
            base.get_indices(df,
                             process['marker']['column'],
                             process['marker']['start_stop']),
            process['marker']['inclusion'])

        for ind, sublist in enumerate(indices):
            df.loc[sublist, new_column] = ind + 1

        within = process.get('within', False)
        if within:
            within_column = within + '_number'
            parent_numbers = base.unique_non_null(df[within_column])
            for number in parent_numbers:
                if number > 1:
                    first = (df.loc[df[within_column] ==
                             number, new_column]).dropna().tolist()[0]
                    first = first - 1
                    df.loc[df[within_column] == number, new_column] = (
                        df.loc[df[within_column] == number, new_column] - first
                                                                       )

    return df
