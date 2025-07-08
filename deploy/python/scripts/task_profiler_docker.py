import argparse
import json
import subprocess
import time
import re
import threading
import statistics
import os
from typing import List, Dict, Any

# --- Helper Function to Monitor a Docker Container ---
def monitor_container(container_id: str, results: Dict[str, Any]):
    """
    Monitors a given Docker container for CPU and memory usage.

    This version is corrected to handle short-lived processes by attempting
    to grab stats immediately before checking if the container has exited.
    """
    results['peak_cpu'] = 0.0
    results['peak_memory_mb'] = 0.0

    while True:
        # 1. Try to get stats FIRST. This is the crucial change.
        try:
            # Get stats without streaming
            proc = subprocess.run(
                ['docker', 'stats', '--no-stream', '--format', '{{.CPUPerc}},{{.MemUsage}}', container_id],
                capture_output=True, text=True, check=True
            )
            output = proc.stdout.strip()

            # Parse CPU
            cpu_match = re.search(r'(\d+\.\d+)%', output)
            if cpu_match:
                cpu_percent = float(cpu_match.group(1))
                if cpu_percent > results['peak_cpu']:
                    results['peak_cpu'] = cpu_percent

            # Parse Memory
            mem_match = re.search(r'(\d+\.?\d+)\s*MiB', output)
            if mem_match:
                memory_mb = float(mem_match.group(1))
                if memory_mb > results['peak_memory_mb']:
                    results['peak_memory_mb'] = memory_mb

        except (subprocess.CalledProcessError, IndexError):
            # This is now expected for very fast containers. It means the container
            # was gone before we could even run the 'stats' command. We can just exit.
            break

        # 2. NOW, check if the container is still running to decide if we should loop again.
        check_proc = subprocess.run(
            ['docker', 'ps', '-q', '-f', f"id={container_id}"],
            capture_output=True, text=True
        )
        if not check_proc.stdout.strip():
            # It's gone, so we have our last reading. We're done.
            break

        # 3. Sleep before the next poll.
        time.sleep(0.05)


# --- Main Profiling Logic ---

def profile_tasks(config_path: str, instance_id: str, iterations: int, output_path: str,
                  docker_image: str, docker_cpus: float, docker_memory: str,
                  verbose: bool = False):
    """
    Profiles tasks from a config file using Docker and generates a new config with measured data.
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

    print(f"\n--- Profiling Setup ---")
    print(f"Instance ID:         '{instance_id}'")
    print(f"Iterations per task: {iterations}")
    print(f"Docker Image:        {docker_image}")
    print(f"Docker CPU Limit:    {docker_cpus}")
    print(f"Docker Memory Limit: {docker_memory}")
    print(f"-----------------------\n")


    for i, task in enumerate(original_config.get('tasks', [])):
        task_id = task.get('taskTypeId', 'unknown_task')
        exec_path = task.get('taskExecPath')

        if not exec_path:
            print(f"Skipping task '{task_id}' due to missing 'taskExecPath'.")
            continue

        if not os.path.exists(exec_path):
            print(f"WARNING: Script for task '{task_id}' not found at '{exec_path}'. Skipping.")
            continue

        print(f"[{i + 1}/{len(original_config['tasks'])}] Profiling Task: {task_id}")

        durations_ms = []
        peak_cpus = []
        peak_memories_mb = []

        # Get host directory and script name for bind mounting
        host_dir = os.path.dirname(os.path.abspath(exec_path))
        script_name = os.path.basename(exec_path)
        container_script_path = f"/app/{script_name}"

        if "inputs" in task:
            inputs = task.get('inputs', {})
            for key, value in inputs.items():
                # Append input arguments to the command list
                container_script_path += f" --{key}={value}"

        for n in range(iterations):
            if verbose:
                print(f"  - Iteration {n + 1}/{iterations}...", end='\r')

            # Command to run the task in a detached Docker container
            docker_command = [
                'docker', 'run',
                '-d',  # Detached mode to get container ID back
                '--rm', # Automatically remove the container when it exits
                '--cpus', str(docker_cpus),
                '--memory', docker_memory,
                '-v', f"{host_dir}:/app", # Mount script directory
                docker_image,
                'python3', container_script_path
            ]
            try:
                # Start the container and capture its ID
                container_id = subprocess.check_output(docker_command, stderr=subprocess.PIPE).decode('utf-8').strip()
                start_time = time.perf_counter()

                # Start the monitor thread
                monitor_results = {}
                monitor_thread = threading.Thread(target=monitor_container, args=(container_id, monitor_results))
                monitor_thread.start()

                # Wait for the container to finish execution
                subprocess.run(['docker', 'wait', container_id], check=True, capture_output=True)
                end_time = time.perf_counter()

                # Ensure monitor thread has finished
                monitor_thread.join()

                # Collect results
                durations_ms.append((end_time - start_time) * 1000)
                peak_cpus.append(monitor_results.get('peak_cpu', 0.0))
                peak_memories_mb.append(monitor_results.get('peak_memory_mb', 0.0))

            except subprocess.CalledProcessError as e:
                # CORRECTED ERROR HANDLING:
                # If the 'docker run' command fails, 'container_id' is never assigned.
                # The error message is in the exception object 'e' itself.
                print(f"\n  - ERROR: The 'docker run' command failed to start for task '{task_id}'.")
                if e.stderr:
                    print(f"  - Docker Daemon Error: {e.stderr.decode().strip()}")
                continue  # Skip this failed iteration
        if verbose:
            print(f"  - Completed {iterations} iterations.                ")

        if not durations_ms:
            print(f"  - No successful runs for task '{task_id}'. Cannot generate data.")
            continue

        avg_duration_ms = statistics.mean(durations_ms)
        avg_peak_cpu = statistics.mean(peak_cpus)
        avg_peak_memory_mb = statistics.mean(peak_memories_mb)

        print(f"  - Average Duration: {avg_duration_ms:.2f} ms")
        print(f"  - Average Peak CPU: {avg_peak_cpu:.2f}%")
        print(f"  - Average Peak Memory: {avg_peak_memory_mb:.2f} MB")

        new_task_entry = {
            "taskTypeId": task_id,
            "taskExecPath": exec_path,
            "instanceInfo": {
                instance_id: {
                    "resourceVector": {
                        "cores": round(avg_peak_cpu / 100, 3),
                        "memory": int(round(avg_peak_memory_mb)),
                        "disks": 1
                    },
                    "estimatedDuration": int(round(avg_duration_ms))
                }
            }
        }
        new_tasks_data.append(new_task_entry)

    final_output = {"tasks": new_tasks_data}

    print(f"\nWriting new configuration to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(final_output, f, indent=4)

    print("\n✅ Profiling complete!")


# --- Argument Parsing and Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Profile Python scripts inside Docker containers and generate a new config with measured performance data.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # --- Core Arguments ---
    parser.add_argument('--config-path', type=str, required=True, help="Path to the input JSON configuration file.")
    parser.add_argument('--instance-id', type=str, required=True, help="A unique identifier for the machine instance (e.g., 'm5.large').")
    parser.add_argument('-n', '--iterations', type=int, default=100, help="Number of times to run each task. Default is 100.")
    parser.add_argument('--output-path', type=str, default="profiler_output_docker.json", help="Path to save the generated JSON file.")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose output.")

    # --- Docker-specific Arguments ---
    parser.add_argument('--docker-image', type=str, default="python:3.10-slim", help="Docker image to use for profiling.")
    parser.add_argument('--docker-cpus', type=float, default=2.0, help="CPU limit for the Docker container.")
    parser.add_argument('--docker-memory', type=str, default="1000m", help="Memory limit for the Docker container (e.g., '1000m', '2g').")

    args = parser.parse_args()

    profile_tasks(
        config_path=args.config_path,
        instance_id=args.instance_id,
        iterations=args.iterations,
        output_path=args.output_path,
        docker_image=args.docker_image,
        docker_cpus=args.docker_cpus,
        docker_memory=args.docker_memory,
        verbose=args.verbose
    )