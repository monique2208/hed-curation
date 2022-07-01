from .operations.base import is_number, get_indices, tuple_to_range, match_label, find_next, \
    split_consecutive_parts
from .operations.bidsfiles import find_task_files, replace_new_with_old, rename_and_save_new, load_operations
from .operations.ops import add_structure, add_numbers, derive_columns, merge_consecutive_events, order_columns, \
    prep_events, rename_columns, remove_columns, run_operations, split_trial_events
