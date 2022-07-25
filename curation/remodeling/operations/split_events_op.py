import numpy as np
import pandas as pd
from hed import HedFileError


def split_events(df, parameters):
    """ Splits a row representing a particular event into multiple rows.

    Args:
        df (DataFrame) - The DataFrame whose rows are to be split.
        parameters (dict) - Dictionary of parameters.

    Raises:
        KeyError if ignore_missing is false and

    Notes:
        - remove_names (list)      The names of the columns to be removed.
        - ignore_missing (boolean) If true, the names in remove_names that are not columns in df should be ignored.

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