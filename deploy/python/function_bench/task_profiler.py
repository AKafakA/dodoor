import argparse
import json
import subprocess
import time
from pathlib import Path

import psutil
import threading
import statistics
import os
from typing import List, Dict, Any
home = Path.home()

os.chdir(str(home) + "/dodoor/deploy/python/function_bench")


# --- Helper Function to Monitor a Process (Corrected) ---
def monitor_process(process: subprocess.Popen, results: Dict[str, Any]):
    """
    Monitors a given process for CPU and memory usage in a separate thread.

    This version is corrected to handle short-lived processes accurately.

    Args:
        process (subprocess.Popen): The process to monitor.
        results (Dict[str, Any]): A dictionary to store the peak usage results.
    """
    try:
        p = psutil.Process(process.pid)
        # Initialize peak values in the results dictionary
        results['peak_cpu'] = 0.0
        results['peak_memory_mb'] = 0.0

        # CRITICAL FIX: Call cpu_percent once before the loop.
        # The first call with interval=None is non-blocking and returns 0.0,
        # but it initializes the baseline for subsequent calls.
        p.cpu_percent(interval=None)

        while process.poll() is None:
            # CRITICAL FIX: Call cpu_percent with interval=None in the loop.
            # This is non-blocking and gets CPU usage since the last call.
            cpu_percent = p.cpu_percent(interval=None)
            if cpu_percent > results['peak_cpu']:
                results['peak_cpu'] = cpu_percent

            # Get memory usage in Megabytes
            memory_mb = p.memory_info().rss / (1024 * 1024)
            if memory_mb > results['peak_memory_mb']:
                results['peak_memory_mb'] = memory_mb

            # CRITICAL FIX: Add a short sleep.
            # This prevents the monitoring loop from being a "busy-wait" that
            # would consume high CPU itself and pollute the measurement.
            time.sleep(0.02)

    except (psutil.NoSuchProcess, psutil.AccessDenied):
        # This is an expected condition, as the process will eventually terminate.
        pass


# --- Main Profiling Logic ---

def profile_tasks(config_path: str, instance_id: str, iterations: int, output_path: str,
                  verbose: bool = False):
    """
    Profiles tasks from a config file and generates a new config with measured data.

    Args:
        verbose:
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
        exec_script = task.get('taskExecPath')
        command_list = ["python3", exec_script]

        if not exec_script:
            print(f"\nSkipping task '{task_id}' due to missing 'taskExecPath'.")
            continue

        if "inputs" in task:
            inputs = task.get('inputs', {})
            for key, value in inputs.items():
                command_list.append(f"--{key}={value}")

        print(f"\n[{i + 1}/{len(original_config['tasks'])}] Profiling Task: {task_id}")

        durations_ms = []
        peak_cpus = []
        peak_memories_mb = []

        for n in range(iterations):
            if verbose:
                print(f"  - Iteration {n + 1}/{iterations}...", end='\r')

            start_time = time.perf_counter()

            # Use Popen to run the script as a non-blocking child process
            process = subprocess.Popen(
                command_list,
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
        if verbose:
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
            "taskExecPath": exec_script,
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
        default="config/function_bench_config_template.json",
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
        default="config",
        help="Path to save the generated JSON file. Default is 'profiler_output.json'."
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help="Enable verbose output for detailed profiling information."
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
        output_path=os.path.join(args.output_path, f"unboxed_profiler_config_{args.instance_id}.json"),
        verbose=args.verbose
    )
