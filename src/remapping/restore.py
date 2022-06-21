import os
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(
    description="""Converts event files based on a json file specifying
    operations.""")

parser.add_argument(
    "-d",
    dest="bids_dir",
    help="")

parser.add_argument(
    "-t",
    dest="task_name",
    help="")

args = parser.parse_args()


def find_task_files(bids_dir, task):
    return [path for path in Path(bids_dir).rglob('*_events.tsv') if task
            in str(path)]


def remove_new_rename_old(path):
    os.remove(path)
    os.rename(str(path)[:-4] + '_orig' + str(path)[-4:], path)


all_events_path = find_task_files(args.bids_dir, args.task_name)

for path in all_events_path:
    remove_new_rename_old(path)
