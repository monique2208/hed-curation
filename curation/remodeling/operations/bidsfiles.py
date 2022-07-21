from pathlib import Path
import os
import shutil
import json


def find_task_files(bids_dir, task=None, suffix='*_events.tsv'):
    if task is None:
        return [path for path in Path(bids_dir).rglob(suffix)]
    else:
        return [path for path in Path(bids_dir).rglob(suffix) if task in str(path)]


def load_operations(json_path):
    with open(json_path, encoding='utf-8') as f:
        operations_dict = json.load(f)
    return operations_dict


def rename_and_save_new(df, path):
    old_path = str(path)[:-4] + 'orig' + str(path)[-4:]
    if not os.path.exists(old_path) or not os.path.isfile(old_path):
        os.rename(path, str(path)[:-4] + 'orig' + str(path)[-4:])
    df.to_csv(path, sep='\t', index=False)


def replace_new_with_old(path):
    """ Replaces path with the one whose basename ends in orig.

    Args:
        path (str):  Full path of the file to be replaced.

    Returns:
        bool:  True if successful replacement.

    Notes:
        The _orig file remains in place and can be deleted separately if all place all replacements are successful.

    """
    old_path = str(path)[:-4] + 'orig' + str(path)[-4:]
    if not os.path.exists(old_path) or not os.path.isfile(old_path):
        return False
    try:
        os.remove(path)
        shutil.copy(old_path, path)
    except:
        return False
    return True

def backup_events(path):
    """ Replaces path with the one whose basename ends in orig.

    Args:
        path (str):  Full path of the file to be replaced.

    Returns:
        bool:  True if successful replacement.

    Notes:
        The _orig file remains in place and can be deleted separately if all place all replacements are successful.

    """
    new_path = str(path)[:-4] + 'orig' + str(path)[-4:]
    shutil.copy(path, new_path)

def remove_new_rename_old_deprecated(path):
    os.remove(path)
    os.rename(str(path)[:-4] + 'orig' + str(path)[-4:], path)
