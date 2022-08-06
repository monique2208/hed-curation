import numpy as np
import pandas as pd
from curation.remodeling.operations.add_structure_column_op import AddStructureColumnOp
from curation.remodeling.operations.add_structure_events_op import AddStructureEventsOp
from curation.remodeling.operations.add_structure_numbers_op import AddStructureNumbersOp
from curation.remodeling.operations.remap_columns import RemapColumnsOp
from curation.remodeling.operations.factor_column_op import FactorColumnOp
from curation.remodeling.operations.factor_hed_tags_op import FactorHedTagsOp
from curation.remodeling.operations.factor_hed_type_op import FactorHedTypeOp
from curation.remodeling.operations.merge_events_op import MergeEventsOp
from curation.remodeling.operations.remove_columns_op import RemoveColumnsOp
from curation.remodeling.operations.reorder_columns_op import ReorderColumnsOp
from curation.remodeling.operations.remove_rows_op import RemoveRowsOp
from curation.remodeling.operations.rename_columns_op import RenameColumnsOp
from curation.remodeling.operations.split_event_op import SplitEventOp
from hed.errors import HedFileError

dispatch = {
    'add_structure_columns': AddStructureColumnOp,
    'add_structure_events': AddStructureEventsOp,
    'add_structure_numbers': AddStructureNumbersOp,
    'factor_column': FactorColumnOp,
    'factor_hed_tags': FactorHedTagsOp,
    'factor_hed_type': FactorHedTypeOp,
    'merge_events': MergeEventsOp,
    'remap_columns': RemapColumnsOp,
    'remove_columns': RemoveColumnsOp,
    'remove_rows': RemoveRowsOp,
    'rename_columns': RenameColumnsOp,
    'reorder_columns': ReorderColumnsOp,
    'split_event': SplitEventOp
}


class Dispatcher:

    def __init__(self, command_list):
        com_list, errors = self.parse_commands(command_list)
        if errors:
            raise ValueError("InvalidCommandList",
                             f"Cannot create a dispatcher for a list of invalid remodeling commands")
        self.command_list = com_list

    def run_operations(self, df, hed_schema=None, sidecar=None):
        """ Run the dispatcher commands on a dataframe.

        Args:
            df (file-like or DataFrame)      The file or dataframe to be remodeled.
            hed_schema (HedSchema or HedSchemaGroup) Only needed for HED operations.
            sidecar (Sidecar or file-like)   Only needed for HED operations

        """

        # string to functions

        if not isinstance(df, pd.DataFrame):
            try:
                df = pd.read_csv(df, sep='\t')
            except Exception as ex:
                raise HedFileError("BadDataFrameFile",
                                   f"{str(df)} does not correspond to a valid tab-separated value file", "")
        df = self.prep_events(df)
        for operation in self.command_list:
            df = operation.do_op(df, hed_schema=hed_schema, sidecar=sidecar)
        df = df.fillna('n/a')
        return df

    @staticmethod
    def parse_commands(command_list):
        errors = []
        commands = []
        for index, item in enumerate(command_list):
            try:
                if not isinstance(item, dict):
                    raise TypeError("InvalidCommandFormat",
                                    f"Each commands must be a dictionary but command {str(item)} is {type(item)}")
                if "command" not in item:
                    raise KeyError("MissingCommand",
                                   f"Command {str(item)} does not have a command key")
                if "parameters" not in item:
                    raise KeyError("MissingParameters",
                                   f"Command {str(item)} does not have a parameters key")
                new_command = dispatch[item["command"]](item["parameters"])
                commands.append(new_command)
            except Exception as ex:
                errors.append({"index": index, "item": f"{item}", "error_type": type(ex),
                               "error_code": ex.args[0], "error_msg": ex.args[1]})
        if errors:
            return [], errors
        return commands, []

    @staticmethod
    def prep_events(df):
        """ Replace all n/a entries in the data frame by np.NaN for processing.

        Args:
            df (DataFrame) - The DataFrame to be processed.

        """
        df = df.replace('n/a', np.NaN)
        return df

    @staticmethod
    def errors_to_str(messages, title="", sep='\n'):
        error_list = [0]*len(messages)
        for index, message in enumerate(messages):
            error_list[index] = f"Command[{message['index']}] has error:{message['error_type']}" + \
                f" with error code:{message['error_code']} and error msg:{message['error_msg']}"
        errors = sep.join(error_list)
        if title:
            return title + sep + errors
        return errors
