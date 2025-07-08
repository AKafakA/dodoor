import time
import argparse
import multiprocessing
import os


def cpu_eater():
    """A simple function that burns CPU cycles."""
    while True:
        pass


def memory_eater():
    """
    Attempts to allocate memory up to the container's limit.
    Reads the cgroup memory limit to determine the target.
    """
    memory_limit_bytes = 0
    cgroup_path = '/sys/fs/cgroup/memory/memory.limit_in_bytes'

    try:
        # Read the memory limit from Linux cgroups
        with open(cgroup_path, 'r') as f:
            memory_limit_bytes = int(f.read().strip())

        # Avoid allocating the absolute max to prevent being killed instantly
        # Target 95% of the container's memory limit
        target_allocation_bytes = int(memory_limit_bytes * 0.99)

        # Allocate memory by creating a large byte array
        # This is more memory-efficient than a list of small objects
        large_memory_chunk = bytearray(target_allocation_bytes)

        print("Memory Eater: Memory allocated. Holding...")
        # Keep the memory allocated until the process is terminated
        while True:
            time.sleep(3600)  # Sleep for a long time

    except FileNotFoundError:
        print(
            f"Memory Eater: Could not find cgroup limit file at '{cgroup_path}'. Is this running on Linux in a container?")
    except Exception as e:
        print(f"Memory Eater: An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A simple stress-ng like script for Docker containers.")
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        required=True,
        help="Timeout in milliseconds (ms) for the stress test."
    )
    args = parser.parse_args()

    # Get the number of CPUs available to the container
    try:
        cpu_count = len(os.sched_getaffinity(0))
    except AttributeError:
        cpu_count = os.cpu_count()  # Fallback for non-Linux systems

    processes = []

    # Start CPU-eating processes
    for _ in range(cpu_count):
        p = multiprocessing.Process(target=cpu_eater)
        p.start()
        processes.append(p)

    # Start memory-eating process
    mem_p = multiprocessing.Process(target=memory_eater)
    mem_p.start()
    processes.append(mem_p)

    # Wait for the specified timeout
    sleep_duration_s = args.timeout / 1000.0
    time.sleep(sleep_duration_s)

    # Terminate all processes
    for p in processes:
        p.terminate()
        p.join()