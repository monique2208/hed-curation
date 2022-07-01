import numpy as np
import pandas as pd
from curation.remapping.operations import base


def add_summed_events(df, column, marker_dict, value_dict):

    indices = base.tuple_to_range(
        base.get_indices(df, marker_dict['column'], marker_dict['start_stop']),
        marker_dict['inclusion'])

    get_value = (base.return_value if value_dict['type'] == 'label'
                 else base.match_label)

    for ind, sublist in enumerate(indices):
        onset = df.loc[sublist[0], 'onset']
        duration = (df.loc[sublist[-1], 'onset'] -
                    df.loc[sublist[0], 'onset'])

        line = pd.DataFrame([np.repeat(np.nan, len(df.columns))],
                            columns=df.columns, index=[sublist[0]-0.5])
        line['onset'] = onset
        line['duration'] = duration
        line[column] = get_value(df.loc[sublist,
                                        marker_dict['column']].tolist(),
                                 value_dict['content'])
        df = pd.concat([line, df])

    df = df.sort_index().reset_index(drop=True)
    return df


def add_column_value(df, column, marker_dict, value_dict):
    indices = base.tuple_to_range(
        base.get_indices(df, marker_dict['column'], marker_dict['start_stop']),
        marker_dict['inclusion'])

    get_value = (base.return_value if value_dict['type'] == 'label'
                 else base.match_label)

    for ind, sublist in enumerate(indices):
        df.loc[sublist, column] = get_value(df.loc[sublist,
                                            marker_dict['column']].tolist(),
                                            value_dict['content'])
    return df
