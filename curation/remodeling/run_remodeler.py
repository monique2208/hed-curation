import os
import argparse
import pandas as pd
# import curation.remapping.operations.ops as ops
from curation.remodeling.util.bidsfiles import find_task_files, load_operations
# import .operations.bidsfiles
from curation.remodeling.operations.ops import run_operations

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Converts event files based on a json file specifying operations.")
    parser.add_argument("bids_dir", help="Full path of BIDS root directory")
    parser.add_argument("-m", dest="json_remodel_path", help="Full path of BIDS root directory")
    parser.add_argument("-t", dest="task_name", help="The name of the task to make this enhancement.")
    args = parser.parse_args()

    all_events_paths = find_task_files(args.bids_dir, task=args.task_name, suffix='*_eventsorig.tsv')
    operations_dict = load_operations(args.json_remodel_path)

    for events_file_path in all_events_paths:
        print('Loading file: %s' % events_file_path)
        print('')
        the_path = os.path.realpath(events_file_path)
        events = pd.read_csv(the_path, sep='\t')

        print('Restructuring %s' % events_file_path)
        events_df = run_operations(events, operations_dict)
        new_path = str(the_path)[:-8] + '.tsv'
        print(new_path)
        events_df.to_csv(new_path, sep='\t', index=False)

        # rename_and_save_new(events, events_file_path)
