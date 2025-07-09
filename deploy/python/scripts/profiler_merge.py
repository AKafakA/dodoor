import argparse
import json
import os
import glob
from typing import List, Dict, Any

def merge_profiler_results(input_files: List[str], output_path: str):
    """
    Merges multiple profiler result JSON files into a single file.

    The script combines the 'instanceInfo' from tasks with the same
    'taskTypeId' across all input files.

    Args:
        input_files (List[str]): A list of paths to the input JSON files.
        output_path (str): The path to save the merged JSON output file.
    """
    # Use a dictionary with taskTypeId as the key for efficient merging
    merged_tasks: Dict[str, Any] = {}

    print(f"Reading {len(input_files)} input files...")

    for file_path in input_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                print(f"  - Processing '{file_path}'...")

                for task in data.get('tasks', []):
                    task_id = task.get('taskTypeId')
                    if not task_id:
                        continue

                    # If this is the first time we see this task, add it to our dict
                    if task_id not in merged_tasks:
                        merged_tasks[task_id] = task
                    else:
                        # If the task already exists, merge the instanceInfo
                        existing_instance_info = merged_tasks[task_id].get('instanceInfo', {})
                        new_instance_info = task.get('instanceInfo', {})

                        # The update method neatly merges the dictionaries
                        existing_instance_info.update(new_instance_info)

                        merged_tasks[task_id]['instanceInfo'] = existing_instance_info

        except FileNotFoundError:
            print(f"  - WARNING: Input file not found: '{file_path}'. Skipping.")
        except json.JSONDecodeError:
            print(f"  - WARNING: Could not decode JSON from '{file_path}'. Skipping.")

    # Convert the dictionary of tasks back into a list for the final JSON structure
    final_task_list = list(merged_tasks.values())
    final_output = {"tasks": final_task_list}

    # Write the final merged file
    print(f"\nWriting merged configuration to: {output_path}")
    try:
        with open(output_path, 'w') as f:
            json.dump(final_output, f, indent=4)
        print("Merge complete!")
    except IOError as e:
        print(f"Error writing to output file '{output_path}': {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge all profiler-generated JSON files from a directory into a single configuration file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default="deploy/resources/configuration/profiled_task_config",
        help="Path to the directory containing the JSON profiler results to merge."
    )
    parser.add_argument(
        '--output-path',
        type=str,
        default="deploy/resources/configuration/gen/merged_profiler_config.json",
        help="Path to save the final merged JSON file. Default is 'merged_profiler_config.json'."
    )

    args = parser.parse_args()

    # Check if the provided path is a valid directory
    if not os.path.isdir(args.input_dir):
        print(f"Error: Input path '{args.input_dir}' is not a valid directory.")
        exit(1)

    # Find all .json files in the specified directory
    # The search is not recursive.
    search_path = os.path.join(args.input_dir, '*.json')
    json_files = glob.glob(search_path)

    if len(json_files) < 2:
        print(f"Error: Found fewer than 2 JSON files in '{args.input_dir}'. Nothing to merge.")
    else:
        merge_profiler_results(
            input_files=json_files,
            output_path=args.output_path
        )