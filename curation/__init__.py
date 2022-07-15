from .remodeling.operations.base import is_number, get_indices, tuple_to_range, match_label, find_next, \
    split_consecutive_parts
from .remodeling.operations.bidsfiles import find_task_files,  rename_and_save_new, load_operations
from .remodeling.operations.ops import add_structure, add_trial_numbers, derive_columns, merge_consecutive_events, order_columns, \
    prep_events, rename_columns, remove_columns, run_operations, split_trial_events

from . import _version
__version__ = _version.get_versions()['version']
