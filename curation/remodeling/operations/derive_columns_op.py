import numpy as np
import pandas as pd
from curation.remodeling.operations.base_op import BaseOp

PARAMS = {
    "command": "derive_columns",
    "required_parameters": {
        "column_name": str,
        "remove_values": list
    },
    "optional_parameters": {}
}


class DeriveColumnsOp(BaseOp):

    def __init__(self):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.derive_dict = {}
        print("to here")

    def do_op(self, df):
        """ Derive columns.

        Args:
            df (DataFrame) - The DataFrame whose rows are to be removed.

        Raises:

        """

        for column in self.derive_dict:
            new_column = pd.Series(np.nan, index=range(len(df)))

            combination_index = 0
            indexes_index = 1

            mapping_source = 1
            mapping_value = 0

            for comb in df.groupby(self.derive_dict[column]['source_columns']).groups.items():
                for derive_map in self.derive_dict[column]['mapping']:
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
