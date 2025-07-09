import argparse
import json
import subprocess
import time
import re
import threading
import statistics
import os
from typing import List, Dict, Any
from pathlib import Path
home = Path.home()

os.chdir(str(home) + "/dodoor/deploy/python/function_bench")

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
                  docker_image: str, min_docker_cpus: float, min_docker_memory: int,
                  verbose: bool = False, mode: str = 'long'):
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
    print(f"Minimum Docker CPUs: {min_docker_cpus}")
    print(f"Minimum Docker Memory: {min_docker_memory} MB")
    print(f"Mode:                {mode}")
    print(f"-----------------------\n")

    for i, task in enumerate(original_config.get('tasks', [])):
        task_id = task.get('taskTypeId', 'unknown_task')
        exec_script = task.get('taskExecPath')

        if not exec_script:
            print(f"Skipping task '{task_id}' due to missing 'taskExecPath'.")
            continue

        if not os.path.exists(exec_script):
            print(f"WARNING: Script for task '{task_id}' not found at '{exec_script}'. Skipping.")
            continue

        print(f"[{i + 1}/{len(original_config['tasks'])}] Profiling Task: {task_id}")

        durations_ms = []
        peak_cpus = []
        peak_memories_mb = []

        # Get host directory and script name for bind mounting
        host_dir = os.getcwd()
        container_script_path = f"/app/{exec_script}"

        instance_info = task.get('instanceInfo', {})
        if instance_id not in instance_info:
            print(f"WARNING: Instance ID '{instance_id}' not found in task '{task_id}'. Skipping.")
            continue

        instance_task_info = instance_info[instance_id]
        cpu_limit = max(instance_task_info.get('resourceVector', {}).get('cores', min_docker_cpus), min_docker_cpus)
        memory_limit = max(instance_task_info.get('resourceVector', {}).get('memory', min_docker_memory),
                           min_docker_memory)

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
                '--cpus', str(cpu_limit),
                '--memory', f"{memory_limit * 1.1}m", # Add 10% buffer to memory limit
                '-v', f"{host_dir}:/app",  # Bind mount the script directory
                docker_image,
                'python3', container_script_path,
                mode  # Pass the mode as an argument to the script
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
            "taskExecPath": exec_script,
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
        description="Profile Python scripts inside Docker containers and generate a config "
                    "with measured performance data.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # --- Core Arguments ---
    parser.add_argument('--config-path', type=str,
                        default="config",
                        help="Path to the input JSON configuration file.")
    parser.add_argument('--instance-id', type=str, required=True,
                        help="A unique identifier for the machine instance (e.g., 'm5.large').")
    parser.add_argument('-n', '--iterations', type=int, default=1000,
                        help="Number of times to run each task. Default is 100.")
    parser.add_argument('-o', '--output-path', type=str,
                        default="config",
                        help="Path to save the generated JSON file.")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose output.")

    parser.add_argument('--mode', type=str, choices=['long', 'short'], default='long',
                        help="Mode of operation: 'long' for long task profiling with tens of seconds, "
                             "'short' for short_task "
                             "with ms-level latency "
                             "Default is 'long'."
                        )

    # --- Docker-specific Arguments ---
    parser.add_argument('--docker-image', type=str, default="wd312/dodoor-function-bench",
                        help="Docker image to use for profiling. Please checking setup_docker.sh for more information.")
    parser.add_argument('--min-docker-cpus', type=float, default=1.0,
                        help="CPU limit for the Docker container.")
    parser.add_argument('--min-docker-memory', type=int, default=1,
                        help="Memory limit for Docker container in mb")


    args = parser.parse_args()

    profile_tasks(
        config_path=f"{args.config_path}/merged_profiler_config_{args.mode}.json",
        instance_id=args.instance_id,
        iterations=args.iterations,
        output_path=args.output_path + f"/docker_profiled_config_{args.instance_id}_{args.mode}.json",
        docker_image=args.docker_image,
        min_docker_cpus=args.min_docker_cpus,
        min_docker_memory=args.min_docker_memory,
        verbose=args.verbose,
        mode=args.mode
    )