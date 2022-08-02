import numpy as np
import pandas as pd
from curation.remodeling.operations.base_op import BaseOp
from hed.tools import KeyMap

PARAMS = {
    "command": "remap_columns",
    "required_parameters": {
        "source_columns": list,
        "destination_columns": list,
        "map_list": list,
        "ignore_missing": bool
    },
    "optional_parameters": {}
}


class RemapColumnsOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.source_columns = parameters['source_columns']
        self.destination_columns = parameters['destination_columns']
        self.map_list = parameters['map_list']
        self.ignore_missing = parameters['ignore_missing']
        if len(self.source_columns) < 1:
            raise ValueError("EmptySourceColumns",
                             f"The source column list {str(self.source_columns)} must be non-empty")

        if len(self.destination_columns) < 1:
            raise ValueError("EmptySourceColumns",
                             f"The source column list {str(self.destination_columns)} must be non-empty")
        entry_len = len(self.source_columns) + len(self.destination_columns)
        for index, item in enumerate(self.map_list):
            if len(item) != entry_len:
                raise ValueError("BadColumnMapEntry",
                                 f"Map list entry {index} has {len(item)} elements, but must have {entry_len} elements")

        self.key_map = self._make_key_map()

    def _make_key_map(self):
        key_df = pd.DataFrame(self.map_list, columns=self.source_columns+self.destination_columns)
        key_map = KeyMap(self.source_columns, target_cols=self.destination_columns, name="remap")
        key_map.update(key_df)
        return key_map

    def do_op(self, df, hed_schema=None, sidecar=None):
        """ Remap new columns from combinations of others.

        Args:
            df (DataFrame) - The DataFrame to be remodeled.
            hed_schema (HedSchema or HedSchemaGroup) Only needed for HED operations.
            sidecar (Sidecar or file-like)   Only needed for HED operations

        Returns:
            Dataframe - a new dataframe after processing.

        """
        df_new, missing = self.key_map.remap(df)
        if missing and not self.ignore_missing:
            raise ValueError("MapSourceValueMissing", f"Missing sources for rows: {str(missing)}")
        return df_new
