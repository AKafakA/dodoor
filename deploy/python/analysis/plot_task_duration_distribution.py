#!/usr/bin/env python3
"""
Parse Azure trace dataset and plot the distribution of task durations.

Usage:
    python plot_task_duration_distribution.py --trace_file <path> --output_dir <path>
"""

import argparse
import os
from pathlib import Path
from typing import List, Dict, Tuple
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend


def parse_trace_file(trace_path: str, max_records: int = None) -> Dict[str, List[float]]:
    """
    Parse Azure trace file to extract task durations.

    Args:
        trace_path: Path to trace file (CSV format)
        max_records: Maximum number of records to read (None = read all)

    Returns:
        Dictionary mapping (task_type, task_mode) to list of durations in seconds
    """
    durations = {}
    records_read = 0

    with open(trace_path, 'r') as f:
        for line in f:
            if max_records is not None and records_read >= max_records:
                break

            line = line.strip()
            if not line:
                continue

            parts = line.split(',')
            if len(parts) >= 8:
                try:
                    # Format: task_id,cores,memory,disk,duration_ms,arrival_time,task_type,task_mode
                    duration_ms = float(parts[4])
                    task_type = parts[6]
                    task_mode = parts[7]

                    # Convert to seconds
                    duration_s = duration_ms / 1000.0

                    key = f"{task_type}_{task_mode}"
                    if key not in durations:
                        durations[key] = []
                    durations[key].append(duration_s)

                    records_read += 1

                except (ValueError, IndexError):
                    continue

    return durations


def plot_duration_distribution(durations: Dict[str, List[float]], output_dir: str, trace_name: str):
    """
    Plot histogram of task duration distributions as probability density.

    Args:
        durations: Dictionary mapping task category to list of durations
        output_dir: Directory to save plots
        trace_name: Name of the trace for the plot title
    """
    os.makedirs(output_dir, exist_ok=True)

    # Plot all categories together
    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.Set3(np.linspace(0, 1, len(durations)))

    for (category, times), color in zip(sorted(durations.items()), colors):
        if not times:
            continue

        times_array = np.array(times)

        # Create histogram with probability density
        counts, bins, patches = ax.hist(
            times_array,
            bins=50,
            alpha=0.6,
            label=category,
            color=color,
            density=True,  # Normalize to probability density
            edgecolor='black',
            linewidth=0.5
        )

    ax.set_xlabel('Task Duration (seconds)', fontsize=12)
    ax.set_ylabel('Probability Density', fontsize=12)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')

    plt.tight_layout()
    output_path = os.path.join(output_dir, f'duration_distribution_{trace_name}_combined.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved combined plot: {output_path}")

    # Plot each category separately
    for category, times in sorted(durations.items()):
        if not times:
            continue

        times_array = np.array(times)

        fig, ax = plt.subplots(figsize=(10, 6))

        counts, bins, patches = ax.hist(
            times_array,
            bins=50,
            alpha=0.7,
            color='steelblue',
            density=True,
            edgecolor='black',
            linewidth=0.8
        )

        # Add statistics text
        mean_val = np.mean(times_array)
        median_val = np.median(times_array)
        std_val = np.std(times_array)

        stats_text = f'Count: {len(times)}\n'
        stats_text += f'Mean: {mean_val:.2f}s\n'
        stats_text += f'Median: {median_val:.2f}s\n'
        stats_text += f'Std: {std_val:.2f}s\n'
        stats_text += f'Min: {np.min(times_array):.2f}s\n'
        stats_text += f'Max: {np.max(times_array):.2f}s'

        ax.text(0.97, 0.97, stats_text,
                transform=ax.transAxes,
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                fontsize=10,
                family='monospace')

        ax.set_xlabel('Task Duration (seconds)', fontsize=12)
        ax.set_ylabel('Probability Density', fontsize=12)
        ax.grid(True, alpha=0.3, linestyle='--')

        plt.tight_layout()
        output_path = os.path.join(output_dir, f'duration_distribution_{trace_name}_{category}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved individual plot: {output_path}")


def plot_duration_cdf(durations: Dict[str, List[float]], output_dir: str, trace_name: str):
    """
    Plot cumulative distribution function (CDF) of task durations.

    Args:
        durations: Dictionary mapping task category to list of durations
        output_dir: Directory to save plots
        trace_name: Name of the trace for the plot title
    """
    os.makedirs(output_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.Set3(np.linspace(0, 1, len(durations)))

    for (category, times), color in zip(sorted(durations.items()), colors):
        if not times:
            continue

        times_sorted = np.sort(times)
        probabilities = np.arange(1, len(times_sorted) + 1) / len(times_sorted)

        ax.plot(times_sorted, probabilities, label=category, color=color, linewidth=2)

    ax.set_xlabel('Task Duration (seconds)', fontsize=12)
    ax.set_ylabel('Cumulative Probability', fontsize=12)
    ax.legend(loc='lower right', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_ylim([0, 1])

    plt.tight_layout()
    output_path = os.path.join(output_dir, f'duration_cdf_{trace_name}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved CDF plot: {output_path}")


def print_statistics(durations: Dict[str, List[float]]):
    """Print summary statistics for each category."""
    print("\n" + "="*100)
    print(f"{'Category':<20} {'Count':<10} {'Mean (s)':<12} {'Median (s)':<12} {'Std (s)':<12} {'Min (s)':<12} {'Max (s)':<12}")
    print("="*100)

    for category, times in sorted(durations.items()):
        if not times:
            continue

        times_array = np.array(times)
        print(f"{category:<20} "
              f"{len(times):<10} "
              f"{np.mean(times_array):<12.2f} "
              f"{np.median(times_array):<12.2f} "
              f"{np.std(times_array):<12.2f} "
              f"{np.min(times_array):<12.2f} "
              f"{np.max(times_array):<12.2f}")

    print("="*100)
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Parse Azure trace and plot task duration distribution'
    )
    parser.add_argument('--trace_file', required=True,
                        help='Path to Azure trace file (e.g., deploy/resources/data/azure_data/azure_data_600)')
    parser.add_argument('--output_dir', required=True,
                        help='Directory to save output plots')
    parser.add_argument('--plot_type', default='both', choices=['histogram', 'cdf', 'both'],
                        help='Type of plot to generate (default: both)')
    parser.add_argument('--max_records', type=int, default=4000,
                        help='Maximum number of records to read from trace file (default: 4000)')

    args = parser.parse_args()

    # Validate inputs
    if not os.path.exists(args.trace_file):
        print(f"Error: Trace file not found: {args.trace_file}")
        return 1

    # Extract trace name from path
    trace_name = Path(args.trace_file).stem

    print(f"Parsing trace file: {args.trace_file}")
    print(f"Max records: {args.max_records if args.max_records else 'all'}")
    print(f"Output directory: {args.output_dir}")

    # Parse trace file
    durations = parse_trace_file(args.trace_file, args.max_records)

    if not durations:
        print("Error: No duration data found in trace file!")
        return 1

    # Print statistics
    print_statistics(durations)

    # Generate plots
    if args.plot_type in ['histogram', 'both']:
        print("\nGenerating histogram plots...")
        plot_duration_distribution(durations, args.output_dir, trace_name)

    if args.plot_type in ['cdf', 'both']:
        print("\nGenerating CDF plot...")
        plot_duration_cdf(durations, args.output_dir, trace_name)

    print(f"\nAll plots saved to: {args.output_dir}")

    return 0


if __name__ == '__main__':
    exit(main())
