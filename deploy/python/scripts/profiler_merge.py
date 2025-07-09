import argparse
import json
import math
import os
import glob
from typing import List, Dict, Any


def merge_profiler_results(input_files: List[str], output_path: str,
                           host_config_path: str,
                           override_num_slots_per_host: int = 2,
                           cores_threshold: int = 2,
                           memory_threshold: int = 1024 * 8,
                           memory_buffer: float = 0.1):
    """
    Merges multiple JSON profiler result files into a single configuration file with CPU cores casting
    """
    # Use a dictionary with taskTypeId as the key for efficient merging
    merged_tasks: Dict[str, Any] = {}

    print(f"Reading {len(input_files)} input files...")

    host_config = json.load(open(host_config_path, 'r'))['nodes']["node.types"]
    node_info = {}
    for node_type in host_config:
        node_info[node_type["node.type"]] = {
            'cores': node_type["system.cores"],
            'memory': node_type['system.memory'],
            'num_slots_per_host': node_type['node_monitor.num_slots']
        }

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
    for task in final_task_list:
        instances_info = task.get('instanceInfo')
        for node_type, instance in instances_info.items():
            # Apply resource casting based on the node type and thresholds
            num_slots = override_num_slots_per_host if override_num_slots_per_host >= 0 \
                else node_info[node_type]['num_slots_per_host']
            resource_vector = instance.get('resourceVector', {})
            casted_cores = []
            casted_memory = []
            for uncasted_core, uncasted_memory in zip(
                resource_vector.get('cores', []),
                resource_vector.get('memory', [])
            ):
                if uncasted_core < cores_threshold:
                    casted_cores.append(int(math.ceil(uncasted_core)))
                else:
                    casted_cores.append(node_info[node_type]['cores'] // num_slots)

                if uncasted_memory < memory_threshold:
                    casted_memory.append(int(math.ceil(uncasted_memory) * (1 + memory_buffer)))
                else:
                    casted_memory.append(int((node_info[node_type]['memory'] // num_slots) * (1 + memory_buffer)))
            # Update the resource vector with the casted values
            resource_vector['cores'] = casted_cores
            resource_vector['memory'] = casted_memory

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
        '--host-config-path',
        type=str,
        default="deploy/resources/configuration/generated_config/host_config.json",
    )

    parser.add_argument(
        '--cores-threshold',
        type=int,
        default=2,
        help="Threshold for the number of cores when apply the host cores / num_slots_per_host "
    )

    parser.add_argument(
        '--memory-threshold',
        type=int,
        default=1024 * 8, # 8 GB
        help="Threshold for the memory in MB when apply the host cores / num_slots_per_host "
    )

    parser.add_argument(
        '--memory-buffer',
        type=float,
        default=0.1,
        help="Buffer percentage for memory allocation. Default is 10%."
    )

    parser.add_argument(
        '--override-num-slots-per-host',
        type=int,
        default=2,
        help="Override the number of slots per host for each node type. Negative value means no override."
    )

    parser.add_argument(
        '--output-path',
        type=str,
        default="deploy/resources/configuration/generated_config/merged_profiler_config.json",
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
            output_path=args.output_path,
            host_config_path=args.host_config_path,
            override_num_slots_per_host=args.override_num_slots_per_host,
            cores_threshold=args.cores_threshold,
            memory_threshold=args.memory_threshold,
            memory_buffer=args.memory_buffer
        )
