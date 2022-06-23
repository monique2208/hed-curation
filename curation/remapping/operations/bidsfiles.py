from pathlib import Path
import os
import json


def find_task_files(bids_dir, task):
    return [path for path in Path(bids_dir).rglob('*_events.tsv') if task in
            str(path)]


def load_operations(json_path):
    with open(json_path, encoding='utf-8') as f:
        operations_dict = json.load(f)
    return operations_dict


def rename_and_save_new(df, path):
    os.rename(path, str(path)[:-4] + '_orig' + str(path)[-4:])
    df.to_csv(path, sep='\t', index=False)


def remove_new_rename_old(path):
    os.remove(path)
    os.rename(str(path)[:-4] + '_orig' + str(path)[-4:], path)
