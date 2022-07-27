from curation.remodeling.operations.base_op import BaseOp
from curation.remodeling.util import base, structure

PARAMS = {
    "command": "add_structure_events_op",
    "required_parameters": {
        "column_name": str,
        "remove_values": list
    },
    "optional_parameters": {}
}


class AddStructureEventsOp(BaseOp):

    def __init__(self, parameters):
        super().__init__(PARAMS["command"], PARAMS["required_parameters"], PARAMS["optional_parameters"])
        self.check_parameters(parameters)
        self.structure_dict = {}

    def do_op(self, df):
        """ Removes rows with the values indicated in the columns.

        Args:
            df (DataFrame) - The DataFrame whose rows are to be removed.

        Raises:

        Notes:
            - column_name (str)     The name of column to be tested.
            - remove_values (list)  The values to test for row removal.

        If column_name is not a column in df, df is just returned.

        """

        # TODO: This is not written
        dispatch = {'structure_column': structure.add_column_value,
                    'structure_event': structure.add_summed_events}

        for element in self.structure_dict:
            process = self.structure_dict[element]
            indices = base.tuple_to_range(base.get_indices(df, process['marker_column'], process['marker_list']))
            column = (process['new_column'] if 'new_column' in process else process['marker_column'])
            value = (process['label'] if 'label' in process else element)
            number = process['number']
            df = dispatch[process.pop('type')](df, indices, column, value, number)
        return df