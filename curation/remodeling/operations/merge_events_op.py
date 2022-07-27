from curation.remodeling.operations.base_op import BaseOp
from curation.remodeling.util import base

PARAMS = {
    "command": "merge_events",
    "required_parameters": {
        "column_name": str,
        "merge_dict": dict
    },
    "optional_parameters": {}
}


class MergeEventsOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.merge_dict = parameters["merge_dict"]

    def do_op(self, df):
        """ Merge consecutive events of the same type

        Args:
            df (DataFrame) - The DataFrame whose rows are to be removed.

        Raises:

        Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.

        """

        consecutive_list = base.split_consecutive_parts(
            df.index[df[self.merge_dict['column']] == self.merge_dict['value']].tolist())
        for series in consecutive_list:
            df.loc[series[0], 'duration'] = sum(df['duration'][series])
            df.loc[series[1:], 'duration'] = 'to_remove'
        df = df[df.duration != 'to_remove']
        df = df.reset_index(drop=True)
        return df
