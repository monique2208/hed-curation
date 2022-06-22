import numpy as np
import pandas as pd
from . import base


def add_summed_events(df, indexes, column, value, number):
    if number:
        df['trial'] = np.nan

    for ind, sublist in enumerate(indexes):
        onset = df.loc[sublist[0], 'onset']
        duration = (df.loc[sublist[-1], 'onset'] -
                    df.loc[sublist[0], 'onset'])

        line = pd.DataFrame([np.repeat(np.nan, len(df.columns))],
                            columns=df.columns, index=[sublist[0]-0.5])
        line['onset'] = onset
        line['duration'] = duration
        line[column] = (value if isinstance(value, str) else
                        base.match_label(df.loc[sublist, column].tolist(),
                                         value))
        df = pd.concat([line, df])

    df = df.sort_index().reset_index(drop=True)
    return df


def add_column_value(df, indexes, column, value, number):
    for ind, lst in enumerate(indexes):
        label = ind + 1
        df.loc[lst, column] = value + '_%d' % label if number else value
    return df
