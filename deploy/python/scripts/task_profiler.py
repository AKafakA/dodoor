import argparse
import json
import subprocess
import time
import psutil
import threading
import statistics
import os
from typing import List, Dict, Any


# --- Helper Function to Monitor a Process ---
def monitor_process(process: subprocess.Popen, results: Dict[str, Any]):
    """
    Monitors a given process for CPU and memory usage in a separate thread.

    Args:
        process (subprocess.Popen): The process to monitor.
        results (Dict[str, Any]): A dictionary to store the peak usage results.
    """
    p = psutil.Process(process.pid)
    peak_cpu = 0.0
    peak_memory_mb = 0.0

    results['peak_cpu'] = peak_cpu
    results['peak_memory_mb'] = peak_memory_mb

    try:
        # Monitor in a tight loop until the process finishes
        while process.poll() is None:
            with p.oneshot():
                # Get CPU percentage. The first call is inaccurate, but it's fine in a loop.
                cpu_percent = p.cpu_percent(interval=0.1)
                if cpu_percent > peak_cpu:
                    peak_cpu = cpu_percent

                # Get memory usage in Megabytes
                memory_mb = p.memory_info().rss / (1024 * 1024)
                if memory_mb > peak_memory_mb:
                    peak_memory_mb = memory_mb

            # Store the latest peak values
            results['peak_cpu'] = peak_cpu
            results['peak_memory_mb'] = peak_memory_mb

    except (psutil.NoSuchProcess, psutil.AccessDenied):
        # Process might have finished before we could query it again
        pass
    finally:
        # Final check after the loop exits
        if 'peak_cpu' not in results or peak_cpu > results['peak_cpu']:
            results['peak_cpu'] = peak_cpu
        if 'peak_memory_mb' not in results or peak_memory_mb > results['peak_memory_mb']:
            results['peak_memory_mb'] = peak_memory_mb


# --- Main Profiling Logic ---

def profile_tasks(config_path: str, instance_id: str, iterations: int, output_path: str):
    """
    Profiles tasks from a config file and generates a new config with measured data.

    Args:
        config_path (str): Path to the input JSON config file.
        instance_id (str): Identifier for the current machine instance.
        iterations (int): Number of times to run each task.
        output_path (str): Path to save the new JSON output file.
    """
    print(f"Loading configuration from: {config_path}")
    try:
        with open(config_path, 'r') as f:
            original_config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{config_path}'")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'")
        return

    new_tasks_data = []

    print(f"\nStarting profiling for instance: '{instance_id}'")
    print(f"Each task will run {iterations} times.")

    for i, task in enumerate(original_config.get('tasks', [])):
        task_id = task.get('taskTypeId', 'unknown_task')
        exec_path = task.get('taskExecPath')

        if not exec_path:
            print(f"\nSkipping task '{task_id}' due to missing 'taskExecPath'.")
            continue

        # Check if the python script exists before trying to run it
        if not os.path.exists(exec_path):
            print(f"\nWARNING: Script for task '{task_id}' not found at '{exec_path}'. Skipping.")
            continue

        print(f"\n[{i + 1}/{len(original_config['tasks'])}] Profiling Task: {task_id}")

        durations_ms = []
        peak_cpus = []
        peak_memories_mb = []

        for n in range(iterations):
            print(f"  - Iteration {n + 1}/{iterations}...", end='\r')

            start_time = time.perf_counter()

            # Use Popen to run the script as a non-blocking child process
            process = subprocess.Popen(
                ['python3', exec_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Start the monitor thread
            monitor_results = {}
            monitor_thread = threading.Thread(target=monitor_process, args=(process, monitor_results))
            monitor_thread.start()

            # Wait for the process and the monitor to complete
            stdout, stderr = process.communicate()
            monitor_thread.join()

            end_time = time.perf_counter()

            if process.returncode != 0:
                print(f"\n  - ERROR during iteration {n + 1} for task '{task_id}'.")
                print(f"  - Stderr: {stderr.decode().strip()}")
                continue  # Skip this failed iteration

            # Collect results for this iteration
            durations_ms.append((end_time - start_time) * 1000)
            peak_cpus.append(monitor_results.get('peak_cpu', 0.0))
            peak_memories_mb.append(monitor_results.get('peak_memory_mb', 0.0))

        print(f"  - Completed {iterations} iterations.                ")

        # Calculate averages if we have successful runs
        if not durations_ms:
            print(f"  - No successful runs for task '{task_id}'. Cannot generate data.")
            continue

        avg_duration_ms = statistics.mean(durations_ms)
        avg_peak_cpu = statistics.mean(peak_cpus)
        avg_peak_memory_mb = statistics.mean(peak_memories_mb)

        print(f"  - Average Duration: {avg_duration_ms:.2f} ms")
        print(f"  - Average Peak CPU: {avg_peak_cpu:.2f}%")
        print(f"  - Average Peak Memory: {avg_peak_memory_mb:.2f} MB")

        # Build the new JSON structure for this task
        new_task_entry = {
            "taskTypeId": task_id,
            "taskExecPath": exec_path,
            "instanceInfo": {
                instance_id: {
                    "resourceVector": {
                        # Convert CPU % to a core count (e.g., 150% -> 1.5 cores)
                        "cores": round(avg_peak_cpu / 100, 2),
                        "memory": int(round(avg_peak_memory_mb)),  # In MB
                        "disks": 1  # As specified, a constant
                    },
                    "estimatedDuration": int(round(avg_duration_ms))  # In Milliseconds
                }
            }
        }
        new_tasks_data.append(new_task_entry)

    # Final JSON structure
    final_output = {"tasks": new_tasks_data}

    # Write the new JSON file
    print(f"\nWriting new configuration to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(final_output, f, indent=4)

    print("\nProfiling complete!")


# --- Argument Parsing and Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Profile Python scripts defined in a JSON config file "
                    "and generate a new config with measured performance data.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--config-path',
        type=str,
        required=True,
        help="Path to the input JSON configuration file."
    )
    parser.add_argument(
        '--instance-id',
        type=str,
        required=True,
        help="A unique identifier for the machine instance "
             "where this script is running (e.g., 'm5.large', 'local-dev')."
    )
    parser.add_argument(
        '-n', '--iterations',
        type=int,
        default=100,
        help="Number of times to run each task for profiling. Default is 100."
    )
    parser.add_argument(
        '--output-path',
        type=str,
        default="profiler_output.json",
        help="Path to save the generated JSON file. Default is 'profiler_output.json'."
    )

    args = parser.parse_args()

    # Ensure psutil is installed
    try:
        import psutil
    except ImportError:
        print("Error: The 'psutil' library is required. Please install it using 'pip install psutil'")
        exit(1)

    profile_tasks(
        config_path=args.config_path,
        instance_id=args.instance_id,
        iterations=args.iterations,
        output_path=args.output_path
    )
