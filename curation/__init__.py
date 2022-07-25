from curation.remodeling.util.base import is_number, get_indices, tuple_to_range, match_label, find_next, \
    split_consecutive_parts
from curation.remodeling.util.bidsfiles import find_task_files,  rename_and_save_new, load_operations
from .remodeling.operations.ops import add_structure_columns, add_structure_numbers, \
    add_structure_events, derive_column, merge_events, prep_events, run_operations
from .remodeling.operations.split_events_op import split_events
from .remodeling.operations.reorder_columns_op import reorder_columns
from .remodeling.operations.remove_columns_op import remove_columns
from .remodeling.operations.rename_columns_op import rename_columns

from . import _version
__version__ = _version.get_versions()['version']
