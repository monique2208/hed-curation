import pandas as pd
import numpy as np
import copy

# pd.set_option('display.max_columns', None)


def is_number(variable):
    '''Checks if a variable is either a float of int.'''
    return ((isinstance(variable, int) or isinstance(variable, float)))


def enhance_events(df, operations_list):
    '''Takes an events dataframe and a list of dictionaries and runs a series
    of operations as specified in the dictionaries.'''

    # string to functions
    dispatch = {'split_events': split_trial_events,
                'derive_columns': derive_columns,
                'rename': rename_columns,
                'remove': remove_columns,
                'order': order_columns
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


def prep_events(df):
    '''Add trial numbers and writes n/a as true nans for easier handeling during
    adaptation.'''

    df['trial_number'] = df.index+1
    # Replace N/A with proper nan
    df = df.replace('n/a', np.NaN)
    return df


def split_trial_events(df, event_dict):
    '''Splits trial row into events based on an event dictionary.'''
    add_column = 'event_type'
    add_column_location = 3
    remove_trial_parent = event_dict.pop('remove_trial_parent')
    print(df.head())

    df.insert(add_column_location, add_column, np.repeat(np.nan, len(df)))
    new_events = pd.DataFrame([], columns=df.columns)

    for event in event_dict:

        add_events = pd.DataFrame([], columns=df.columns)
        print('Adding event %s \n' % event)
        print('')

        if is_number(event_dict[event]['onset_source']):

            add_events['onset'] = (df['onset'] +
                                   event_dict[event]['onset_source'])

        elif isinstance(event_dict[event]['onset_source'], str):
            add_events['onset'] = (df['onset'] +
                                   df[event_dict[event]['onset_source']])

            # remove events if there is no onset (=no response)
            add_events = add_events.dropna(axis='rows', subset=['onset'])

        elif isinstance(event_dict[event]['onset_source'], list):
            add_events['onset'] = (df['onset'] +
                                   df[event_dict[event]['onset_source'][0]] +
                                   event_dict[event]['onset_source'][1])

            add_events = add_events.dropna(axis='rows', subset=['onset'])

        if is_number(event_dict[event]['duration']):
            add_events['duration'] = event_dict[event]['duration']
        elif isinstance(event_dict[event]['duration'], str):
            add_events['duration'] = df[event_dict[event]['duration']]

        if len(event_dict[event]['copy_columns']) > 0:
            for column in event_dict[event]['copy_columns']:
                add_events[column] = df[column]
        add_events['trial_number'] = df['trial_number']

        if len(event_dict[event]['move_columns']) > 0:
            for column in event_dict[event]['move_columns']:
                add_events[column] = df[column]
                df[column] = np.NaN

        add_events['event_type'] = event
        # print('single event additions')
        # print(add_events.head())
        # print('\n')

        new_events = new_events.append(add_events)
        # print('all added events')
        # print(new_events.head())
        # print('\n')

    df = df.append(new_events)
    # print('new dataframe')
    # print(df.head())
    # print('\n')
    if remove_trial_parent:
        df = df.dropna(axis='rows', subset=['event_type'])
    # print('removed trial events')
    # print(df.head())
    # print('\n')
    df = df.sort_values('onset').reset_index(drop=True)
    # print('sorted')
    # print(df.head())
    # print('\n')
    return df


def derive_columns(df, event_dict):
    '''Add a new column based on values in other columns as specified by an
    event dictionary.'''

    for column in event_dict:
        new_column = pd.Series(np.nan, index=range(len(df)))

        combination_index = 0
        indexes_index = 1

        mapping_source = 1
        mapping_value = 0

        for comb in df.groupby(
                event_dict[column]['source_columns']).groups.items():
            for map in event_dict[column]['mapping']:
                # for each key value pair, check if equal to a specific mapping
                if set(comb[combination_index]) == set(map[mapping_source]):
                    entry = map[mapping_value]
                    new_column.iloc[comb[indexes_index]] = entry

        new_column = new_column.dropna(axis=0)
        # print(new_column)
        if column not in df.columns:
            df[column] = np.nan
        df[column].iloc[new_column.index.tolist()] = new_column
    return df


def remove_columns(df, event_dict):
    '''Remove columns as specified in event dictionary'''
    return df.drop(event_dict, axis=1)


def rename_columns(df, event_dict):
    '''Rename columns as specified in event dictionary'''
    return df.rename(columns=event_dict)


def order_columns(df, event_dict):
    '''Reorder columns as specified in event dictionary'''
    return df[event_dict]
