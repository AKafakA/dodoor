#!/usr/bin/env python3
"""
Native profiler for simulated tasks (stress/stress-ng workloads).

This profiler measures execution times WITHOUT Docker overhead,
matching how simulated tasks are executed in online experiments.

Usage:
    python task_profiler_native.py --output profiler_config_simulated.json --iterations 100
"""

import argparse
import json
import subprocess
import time
import statistics
from typing import List, Dict


def profile_simulated_task(cores: float, memory: int, disk: int, duration_ms: int, iterations: int) -> Dict:
    """
    Profile a simulated task using native stress-ng execution.

    Args:
        cores: CPU cores (can be fractional, e.g., 2.5)
        memory: Memory in MB
        disk: Disk in MB
        duration_ms: Expected duration in milliseconds
        iterations: Number of times to run the task

    Returns:
        Dictionary with execution statistics
    """
    durations = []
    duration_sec = max(duration_ms / 1000.0, 1.0)  # Minimum 1 second

    # Generate stress-ng command (matching TaskLauncherService.java:218-252)
    if cores == int(cores):
        # Integer cores - use stress or stress-ng
        int_cores = int(cores)
        if disk <= 0 and memory <= 0:
            cmd = f"stress-ng -c {int_cores} --timeout {duration_sec}"
        elif disk <= 0:
            cmd = f"stress-ng -c {int_cores} --vm 1 --vm-bytes {memory}M --timeout {duration_sec}"
        elif memory <= 0:
            cmd = f"stress-ng -c {int_cores} --hdd 1 --hdd-bytes {disk}M --timeout {duration_sec}"
        else:
            cmd = f"stress-ng -c {int_cores} --vm 1 --vm-bytes {memory}M -d 1 --hdd-bytes {disk}M --timeout {duration_sec}"
    else:
        # Fractional cores - use stress-ng with load percentage
        int_cores = int(cores) + 1
        load = cores / int_cores
        load_percentage = int(load * 100)
        if disk <= 0 and memory <= 0:
            cmd = f"stress-ng -c {int_cores} -l {load_percentage} --timeout {duration_sec}"
        elif disk <= 0:
            cmd = f"stress-ng -c {int_cores} -l {load_percentage} --vm 1 --vm-bytes {memory}M --timeout {duration_sec}"
        elif memory <= 0:
            cmd = f"stress-ng -c {int_cores} -l {load_percentage} --hdd 1 --hdd-bytes {disk}M --timeout {duration_sec}"
        else:
            cmd = f"stress-ng -c {int_cores} -l {load_percentage} --vm 1 --vm-bytes {memory}M -d 1 --hdd-bytes {disk}M --timeout {duration_sec}"

    print(f"  Command: {cmd}")

    for i in range(iterations):
        start = time.perf_counter()
        try:
            subprocess.run(cmd, shell=True, check=True,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL,
                          timeout=duration_sec + 5)  # Add 5s buffer
            end = time.perf_counter()
            exec_time = (end - start) * 1000  # Convert to ms
            durations.append(exec_time)
        except subprocess.TimeoutExpired:
            print(f"    Warning: Iteration {i+1} timed out")
            continue
        except subprocess.CalledProcessError as e:
            print(f"    Warning: Iteration {i+1} failed with exit code {e.returncode}")
            continue

    if not durations:
        return {
            'count': 0,
            'mean': 0,
            'std': 0,
            'min': 0,
            'max': 0
        }

    return {
        'count': len(durations),
        'mean': statistics.mean(durations),
        'std': statistics.stdev(durations) if len(durations) > 1 else 0,
        'min': min(durations),
        'max': max(durations)
    }


def profile_all_configurations(iterations: int, output_path: str):
    """
    Profile common simulated task configurations.

    Creates a profiler config matching merged_profiler_config.json format
    but for simulated tasks instead of FunctionBench tasks.
    """

    # Common configurations from Azure traces
    # Format: (task_mode, cores, memory_mb, disk_mb, expected_duration_ms)
    configurations = {
        'short': [
            (1.0, 512, 100, 200),    # Light CPU, minimal I/O
            (2.0, 1024, 200, 300),   # Medium CPU
            (4.0, 2048, 500, 500),   # Heavy CPU
        ],
        'medium': [
            (2.0, 2048, 500, 1000),  # Medium workload
            (4.0, 4096, 1000, 1500), # Heavy workload
            (8.0, 8192, 2000, 2000), # Very heavy
        ],
        'long': [
            (4.0, 4096, 1000, 5000),  # Long-running
            (8.0, 8192, 2000, 10000), # Very long
            (16.0, 16384, 4000, 15000), # Extreme
        ]
    }

    # Profile each configuration
    results = {}

    for mode, configs in configurations.items():
        print(f"\nProfiling {mode} tasks...")
        for cores, memory, disk, duration in configs:
            config_name = f"simulated_{mode}_{cores}c_{memory}m"
            print(f"  Configuration: {config_name}")

            stats = profile_simulated_task(cores, memory, disk, duration, iterations)

            results[config_name] = {
                'mode': mode,
                'cores': cores,
                'memory_mb': memory,
                'disk_mb': disk,
                'expected_duration_ms': duration,
                'measured_duration_ms': int(round(stats['mean'])),
                'std_ms': round(stats['std'], 2),
                'min_ms': int(round(stats['min'])),
                'max_ms': int(round(stats['max'])),
                'iterations': stats['count']
            }

            print(f"    Expected: {duration}ms, Measured: {stats['mean']:.0f}ms ± {stats['std']:.0f}ms")

    # Save results
    output = {
        'profiler_type': 'native_simulated',
        'description': 'Native stress-ng profiling without Docker overhead',
        'configurations': results
    }

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Profiling complete! Results saved to {output_path}")

    # Print summary
    print("\n" + "="*80)
    print("SUMMARY: Native vs Expected Duration")
    print("="*80)
    print(f"{'Configuration':<30} {'Expected':>12} {'Measured':>12} {'Difference':>12}")
    print("-"*80)

    for config_name, data in sorted(results.items()):
        expected = data['expected_duration_ms']
        measured = data['measured_duration_ms']
        diff = measured - expected
        pct = (diff / expected * 100) if expected > 0 else 0

        print(f"{config_name:<30} {expected:>10}ms {measured:>10}ms {diff:>+10}ms ({pct:+.1f}%)")

    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Profile simulated tasks using native stress-ng execution (no Docker)',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('-o', '--output', type=str,
                       default='profiler_config_simulated_native.json',
                       help='Output path for profiler results (default: profiler_config_simulated_native.json)')

    parser.add_argument('-n', '--iterations', type=int, default=50,
                       help='Number of iterations per configuration (default: 50)')

    args = parser.parse_args()

    print("="*80)
    print("Native Simulated Task Profiler")
    print("="*80)
    print("\nThis profiler measures stress-ng execution WITHOUT Docker overhead.")
    print("Results will match how simulated tasks are executed in online experiments.\n")

    profile_all_configurations(args.iterations, args.output)


if __name__ == '__main__':
    main()
