#!/usr/bin/env python3
"""
Analyze node logs to aggregate task execution times by task type, mode, and node type.

Usage:
    python analyze_task_execution.py --log_dir <path> --trace_file <path> --manifest <path>
"""

import argparse
import json
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
import numpy as np


def parse_manifest(manifest_path: str) -> Dict[str, str]:
    """
    Parse manifest.xml to extract node name to hardware type mapping.

    Args:
        manifest_path: Path to manifest.xml file

    Returns:
        Dictionary mapping node names (e.g., 'amd105') to hardware types (e.g., 'm510')
    """
    tree = ET.parse(manifest_path)
    root = tree.getroot()

    # Define namespace
    ns = {'ns': 'http://www.geni.net/resources/rspec/3',
          'emulab': 'http://www.protogeni.net/resources/rspec/ext/emulab/1'}

    node_type_map = {}

    for node in root.findall('.//ns:node', ns):
        # Get vnode name (e.g., 'amd105')
        vnode = node.find('.//emulab:vnode', ns)
        if vnode is not None:
            node_name = vnode.get('name')

            # Get hardware type (e.g., 'm510')
            hardware = node.find('.//ns:hardware_type', ns)
            if hardware is not None:
                hardware_type = hardware.get('name')
                if node_name and hardware_type:
                    node_type_map[node_name] = hardware_type

    return node_type_map


def parse_trace_file(trace_path: str) -> Dict[int, Tuple[str, str]]:
    """
    Parse Azure trace file to get task ID to (task_type, task_mode) mapping.

    Args:
        trace_path: Path to trace file (CSV format)

    Returns:
        Dictionary mapping task ID to (task_type, task_mode) tuple
    """
    task_info = {}

    with open(trace_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(',')
            if len(parts) >= 8:
                try:
                    task_id = int(parts[0])
                    task_type = parts[6]  # e.g., 'simulated'
                    task_mode = parts[7]  # e.g., 'medium'
                    task_info[task_id] = (task_type, task_mode)
                except (ValueError, IndexError):
                    continue

    return task_info


def parse_node_log(log_path: str) -> List[Tuple[int, float]]:
    """
    Parse a node log file to extract task completions.

    Args:
        log_path: Path to node log file

    Returns:
        List of (task_id, execution_time_ms) tuples
    """
    completions = []

    # Pattern: "Task {task_id} processing completed in {time_ms} ms"
    pattern = re.compile(r'Task (\d+) processing completed in (\d+) ms')

    with open(log_path, 'r') as f:
        for line in f:
            # Skip lines that don't contain our pattern
            if 'processing completed in' not in line:
                continue

            match = pattern.search(line)
            if match:
                task_id = int(match.group(1))
                execution_time = float(match.group(2))
                completions.append((task_id, execution_time))

    return completions


def extract_node_name(log_filename: str) -> str:
    """
    Extract node name from log filename.

    Args:
        log_filename: e.g., 'dodoor_node_service_out_amd105.log'

    Returns:
        Node name, e.g., 'amd105'
    """
    # Pattern: *_out_{node_name}.log
    match = re.search(r'_out_([^.]+)\.log', log_filename)
    if match:
        return match.group(1)
    return None


def analyze_logs(log_dir: str, trace_file: str, manifest_path: str) -> Dict:
    """
    Analyze all node logs in the given directory.

    Args:
        log_dir: Directory containing experiment subdirectories
        trace_file: Path to Azure trace file
        manifest_path: Path to manifest.xml

    Returns:
        Dictionary with aggregated statistics
    """
    # Parse manifest and trace
    node_type_map = parse_manifest(manifest_path)
    task_info_map = parse_trace_file(trace_file)

    # Data structure: {(task_type, task_mode, node_type): [execution_times]}
    stats = defaultdict(list)

    # Walk through all experiment directories
    exp_dirs = sorted([d for d in Path(log_dir).iterdir() if d.is_dir()])
    total_files = 0

    for exp_dir in exp_dirs:
        service_log_dir = exp_dir / 'service_log'
        if not service_log_dir.exists():
            continue

        # Process each node log file
        log_files = list(service_log_dir.glob('*_out_*.log'))
        print(f"Processing {exp_dir.name}: {len(log_files)} log files")

        for log_file in log_files:
            node_name = extract_node_name(log_file.name)
            if not node_name:
                continue

            node_type = node_type_map.get(node_name, 'unknown')

            # Parse the log file
            completions = parse_node_log(str(log_file))
            total_files += 1

            # Aggregate by (task_type, task_mode, node_type)
            for task_id, exec_time in completions:
                if task_id in task_info_map:
                    task_type, task_mode = task_info_map[task_id]
                    key = (task_type, task_mode, node_type)
                    stats[key].append(exec_time)

    print(f"\nProcessed {total_files} log files total")
    return stats


def compute_statistics(times: List[float]) -> Dict:
    """Compute statistics for a list of execution times."""
    if not times:
        return {
            'count': 0,
            'mean': 0,
            'median': 0,
            'std': 0,
            'min': 0,
            'max': 0,
            'p50': 0,
            'p95': 0,
            'p99': 0
        }

    arr = np.array(times)
    return {
        'count': len(times),
        'mean': float(np.mean(arr)),
        'median': float(np.median(arr)),
        'std': float(np.std(arr)),
        'min': float(np.min(arr)),
        'max': float(np.max(arr)),
        'p50': float(np.percentile(arr, 50)),
        'p95': float(np.percentile(arr, 95)),
        'p99': float(np.percentile(arr, 99))
    }


def format_time_ms(ms: float, width: int = 10) -> str:
    """Format milliseconds in a readable way with consistent width."""
    if ms < 1000:
        formatted = f"{ms:.0f}ms"
    elif ms < 60000:
        formatted = f"{ms/1000:.2f}s"
    else:
        formatted = f"{ms/60000:.2f}m"
    return formatted.rjust(width)


def print_results(stats: Dict, output_format: str = 'table'):
    """
    Print aggregated statistics.

    Args:
        stats: Dictionary from analyze_logs
        output_format: 'table' or 'json'
    """
    if output_format == 'json':
        results = {}
        for key, times in sorted(stats.items()):
            task_type, task_mode, node_type = key
            key_str = f"{task_type}_{task_mode}_{node_type}"
            results[key_str] = compute_statistics(times)
        print(json.dumps(results, indent=2))
        return

    # Table format with fixed column widths
    print("\n" + "="*130)
    print(f"{'Task Type':<15} {'Mode':<10} {'Node Type':<12} {'Count':>8} {'Mean':>12} {'Median':>12} {'Std':>12} {'Min':>12} {'Max':>12} {'P95':>12} {'P99':>12}")
    print("="*130)

    for key, times in sorted(stats.items()):
        task_type, task_mode, node_type = key
        s = compute_statistics(times)

        print(f"{task_type:<15} {task_mode:<10} {node_type:<12} "
              f"{s['count']:>8} "
              f"{format_time_ms(s['mean'], 12)} "
              f"{format_time_ms(s['median'], 12)} "
              f"{format_time_ms(s['std'], 12)} "
              f"{format_time_ms(s['min'], 12)} "
              f"{format_time_ms(s['max'], 12)} "
              f"{format_time_ms(s['p95'], 12)} "
              f"{format_time_ms(s['p99'], 12)}")

    print("="*130)
    print(f"Total combinations: {len(stats)}")
    print()


def map_task_mode_to_index(mode: str) -> int:
    """Map task mode (short/medium/long) to array index (0/1/2)."""
    mode_map = {'short': 0, 'medium': 1, 'long': 2}
    return mode_map.get(mode.lower(), -1)


def update_profiler_config(stats: Dict, profiler_config_path: str, output_path: str = None):
    """
    Update merged_profiler_config.json with real execution data from online experiments.

    Args:
        stats: Dictionary from analyze_logs with execution statistics
        profiler_config_path: Path to merged_profiler_config.json
        output_path: Optional output path (defaults to overwriting input)
    """
    if output_path is None:
        output_path = profiler_config_path

    print(f"\nLoading profiler config from: {profiler_config_path}")

    try:
        with open(profiler_config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Profiler config not found at {profiler_config_path}")
        return

    updates_made = 0

    # Process each task in the config
    for task in config.get('tasks', []):
        task_type_id = task.get('taskTypeId')
        instance_info = task.get('instanceInfo', {})

        # Update each node type's estimatedDuration
        for node_type, node_data in instance_info.items():
            estimated_durations = node_data.get('estimatedDuration', [0, 0, 0])

            # Look for matching statistics for each mode (short, medium, long)
            for mode_idx, mode in enumerate(['short', 'medium', 'long']):
                # Find matching stats entry
                key = (task_type_id, mode, node_type)
                if key in stats:
                    times = stats[key]
                    s = compute_statistics(times)
                    # Update with mean execution time (rounded to nearest ms)
                    new_duration = int(round(s['mean']))
                    old_duration = estimated_durations[mode_idx]

                    estimated_durations[mode_idx] = new_duration
                    updates_made += 1

                    print(f"  Updated {task_type_id}/{mode}/{node_type}: "
                          f"{old_duration}ms -> {new_duration}ms "
                          f"(Δ{new_duration - old_duration:+d}ms, "
                          f"{((new_duration/old_duration - 1) * 100):+.1f}%)")

            # Write back updated durations
            node_data['estimatedDuration'] = estimated_durations

    # Save updated config
    print(f"\nWriting updated config to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=4)

    print(f"✅ Updated {updates_made} estimatedDuration values")


def compare_with_profiler(stats: Dict, profiler_config_path: str):
    """
    Compare online experiment results with offline profiler results.

    Args:
        stats: Dictionary from analyze_logs with execution statistics
        profiler_config_path: Path to merged_profiler_config.json
    """
    print(f"\nLoading profiler config from: {profiler_config_path}")

    try:
        with open(profiler_config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Profiler config not found at {profiler_config_path}")
        return

    # Build lookup table from profiler config
    profiler_data = {}
    profiler_task_types = set()
    for task in config.get('tasks', []):
        task_type_id = task.get('taskTypeId')
        profiler_task_types.add(task_type_id)
        instance_info = task.get('instanceInfo', {})

        for node_type, node_data in instance_info.items():
            estimated_durations = node_data.get('estimatedDuration', [0, 0, 0])
            for mode_idx, mode in enumerate(['short', 'medium', 'long']):
                key = (task_type_id, mode, node_type)
                profiler_data[key] = estimated_durations[mode_idx]

    # Check for task type mismatches
    online_task_types = set(task_type for task_type, _, _ in stats.keys())

    if not online_task_types.intersection(profiler_task_types):
        print("\n" + "="*80)
        print("⚠️  WARNING: TASK TYPE MISMATCH DETECTED!")
        print("="*80)
        print(f"\nOnline experiment task types: {sorted(online_task_types)}")
        print(f"Profiler config task types:   {sorted(profiler_task_types)}")
        print("\n❌ NO MATCHING TASK TYPES FOUND!")
        print("\nPossible reasons:")
        print("1. Online uses 'simulated' tasks (native stress-ng, fast)")
        print("   Profiler uses FunctionBench tasks (Docker-based, slow)")
        print("   → This comparison is INVALID (different workloads)")
        print("\n2. Different trace files or config files")
        print("   → Check that trace file and profiler config match")
        print("\nSolutions:")
        print("- For 'simulated' tasks: Create native profiler (no Docker)")
        print("- For FunctionBench tasks: Use matching trace file")
        print(f"\nSee ONLINE_FASTER_ANALYSIS.md for detailed explanation")
        print("="*80)
        print("\nSkipping comparison due to task type mismatch.\n")
        return

    # Compare
    print("\n" + "="*160)
    print(f"{'Task Type':<15} {'Mode':<10} {'Node Type':<12} "
          f"{'Online (mean)':>15} {'Offline (prof)':>15} {'Difference':>15} {'% Diff':>10} {'Count':>8}")
    print("="*160)

    total_comparisons = 0
    total_abs_error = 0
    total_rel_error = 0

    for key, times in sorted(stats.items()):
        task_type, task_mode, node_type = key
        s = compute_statistics(times)
        online_mean = s['mean']

        if key in profiler_data:
            offline_est = profiler_data[key]
            diff = online_mean - offline_est
            pct_diff = ((online_mean / offline_est - 1) * 100) if offline_est > 0 else 0

            total_comparisons += 1
            total_abs_error += abs(diff)
            total_rel_error += abs(pct_diff)

            print(f"{task_type:<15} {task_mode:<10} {node_type:<12} "
                  f"{format_time_ms(online_mean, 15)} "
                  f"{format_time_ms(offline_est, 15)} "
                  f"{format_time_ms(diff, 15)} "
                  f"{pct_diff:>9.1f}% "
                  f"{s['count']:>8}")
        else:
            print(f"{task_type:<15} {task_mode:<10} {node_type:<12} "
                  f"{format_time_ms(online_mean, 15)} "
                  f"{'N/A':>15} "
                  f"{'N/A':>15} "
                  f"{'N/A':>10} "
                  f"{s['count']:>8}")

    print("="*160)

    if total_comparisons > 0:
        avg_abs_error = total_abs_error / total_comparisons
        avg_rel_error = total_rel_error / total_comparisons
        print(f"\nComparison Summary:")
        print(f"  Total comparisons: {total_comparisons}")
        print(f"  Average absolute error: {format_time_ms(avg_abs_error)}")
        print(f"  Average relative error: {avg_rel_error:.1f}%")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Analyze node logs to aggregate task execution times by type, mode, and node type'
    )
    parser.add_argument('--log_dir', required=True,
                        help='Directory containing experiment logs (e.g., deploy/resources/log/node/azure_600)')
    parser.add_argument('--trace_file', required=True,
                        help='Path to Azure trace file (e.g., deploy/resources/data/azure_data/azure_data_600)')
    parser.add_argument('--manifest', required=True,
                        help='Path to manifest.xml (e.g., deploy/resources/configuration/manifest.xml)')
    parser.add_argument('--output_format', default='table', choices=['table', 'json'],
                        help='Output format: table or json (default: table)')
    parser.add_argument('--output_file', default=None,
                        help='Optional: Save output to file instead of stdout')
    parser.add_argument('--update_profiler', default=None,
                        help='Path to merged_profiler_config.json to update with online execution data')
    parser.add_argument('--profiler_output', default=None,
                        help='Output path for updated profiler config (defaults to overwriting input)')
    parser.add_argument('--compare_profiler', default=None,
                        help='Path to merged_profiler_config.json to compare with online data')

    args = parser.parse_args()

    # Validate inputs
    if not os.path.exists(args.log_dir):
        print(f"Error: Log directory not found: {args.log_dir}")
        return 1

    if not os.path.exists(args.trace_file):
        print(f"Error: Trace file not found: {args.trace_file}")
        return 1

    if not os.path.exists(args.manifest):
        print(f"Error: Manifest file not found: {args.manifest}")
        return 1

    print(f"Analyzing logs from: {args.log_dir}")
    print(f"Using trace file: {args.trace_file}")
    print(f"Using manifest: {args.manifest}")

    # Analyze
    stats = analyze_logs(args.log_dir, args.trace_file, args.manifest)

    if not stats:
        print("Warning: No task execution data found!")
        return 1

    # Output results
    if args.output_file:
        import sys
        original_stdout = sys.stdout
        with open(args.output_file, 'w') as f:
            sys.stdout = f
            print_results(stats, args.output_format)
            sys.stdout = original_stdout
        print(f"Results written to: {args.output_file}")
    else:
        print_results(stats, args.output_format)

    # Update profiler config if requested
    if args.update_profiler:
        if not os.path.exists(args.update_profiler):
            print(f"Error: Profiler config not found: {args.update_profiler}")
            return 1
        update_profiler_config(stats, args.update_profiler, args.profiler_output)

    # Compare with profiler if requested
    if args.compare_profiler:
        if not os.path.exists(args.compare_profiler):
            print(f"Error: Profiler config not found: {args.compare_profiler}")
            return 1
        compare_with_profiler(stats, args.compare_profiler)

    return 0


if __name__ == '__main__':
    exit(main())