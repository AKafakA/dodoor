import argparse
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patches as mpatches
import numpy as np

from deploy.python.analysis.scheduler_metrics import SchedulerMetrics


def parse_parameter_from_name(dir_name):
    """
    Extract parameters from directory name like 'dodoor_batch_25_beta_1.0_cpu_2.0_duration_0.5_qps_5'
    Returns dict with parameter values and QPS
    """
    parts = dir_name.split('_')
    params = {}

    try:
        # Find scheduler name
        params['scheduler'] = parts[0]

        # Find QPS
        qps_idx = parts.index('qps')
        params['qps'] = int(float(parts[qps_idx + 1]))

        # Extract other parameters
        if 'batch' in parts:
            batch_idx = parts.index('batch')
            params['batch_size'] = float(parts[batch_idx + 1])

        if 'beta' in parts:
            beta_idx = parts.index('beta')
            params['beta'] = float(parts[beta_idx + 1])

        if 'cpu' in parts:
            cpu_idx = parts.index('cpu')
            params['cpu_weight'] = float(parts[cpu_idx + 1])

        if 'duration' in parts:
            duration_idx = parts.index('duration')
            params['duration_weight'] = float(parts[duration_idx + 1])

    except (ValueError, IndexError) as e:
        print(f"Error parsing parameters from {dir_name}: {e}")
        return None

    return params


def detect_tuning_parameter(experiment_name):
    """
    Detect which parameter is being tuned based on experiment name.
    Returns parameter name (e.g., 'batch_size', 'duration_weight')
    """
    if 'tune_batch' in experiment_name:
        return 'batch_size'
    elif 'tune_duration_weight' in experiment_name:
        return 'duration_weight'
    elif 'tune_cpu' in experiment_name or 'tune_cpu_weight' in experiment_name:
        return 'cpu_weight'
    elif 'tune_beta' in experiment_name:
        return 'beta'
    else:
        return None


def get_display_name(parameter_name):
    """
    Get display name for parameter (for use in plots/paper).
    Maps internal names to paper names.
    """
    name_mapping = {
        'duration_weight': 'alpha',
        'batch_size': 'batch_size',
        'cpu_weight': 'cpu_weight',
        'beta': 'beta'
    }
    return name_mapping.get(parameter_name, parameter_name)


def parse_service_log_per_task_metrics(service_log_path):
    """
    Parse service log file to extract per-task scheduling and makespan latencies.

    Returns:
        dict with 'scheduling_latencies' and 'makespan_latencies' lists
    """
    scheduling_latencies = []
    makespan_latencies = []

    if not os.path.exists(service_log_path):
        print(f"Service log not found: {service_log_path}")
        return {'scheduling_latencies': scheduling_latencies, 'makespan_latencies': makespan_latencies}

    # Regex patterns
    scheduling_pattern = re.compile(r'Task\s+(-?\d+)\s+scheduled,\s+end-to-end\s+latency:\s+(\d+)\s+ms')
    makespan_pattern = re.compile(r'Task\s+(-?\d+)\s+finished,\s+makespan:\s+(\d+)\s+ms')

    with open(service_log_path, 'r') as f:
        for line in f:
            # Parse scheduling latency
            sched_match = scheduling_pattern.search(line)
            if sched_match:
                latency = int(sched_match.group(2))
                scheduling_latencies.append(latency)

            # Parse makespan latency
            makespan_match = makespan_pattern.search(line)
            if makespan_match:
                makespan = int(makespan_match.group(2))
                makespan_latencies.append(makespan)

    print(f"  Parsed {len(scheduling_latencies)} scheduling latencies, {len(makespan_latencies)} makespan latencies")
    return {
        'scheduling_latencies': scheduling_latencies,
        'makespan_latencies': makespan_latencies
    }


def parse_experiment_data(experiment_log_dir, experiment_name, parameter_name):
    """
    Parse all scheduler configuration directories in a tuning experiment.
    Returns aggregated metrics DataFrame and per-task metrics by parameter value.
    """
    aggregated_data = []
    per_task_data = {}  # {parameter_value: {'scheduling': [], 'makespan': []}}

    for config_dir in os.listdir(experiment_log_dir):
        config_dir_path = os.path.join(experiment_log_dir, config_dir)

        if not os.path.isdir(config_dir_path):
            continue

        try:
            params = parse_parameter_from_name(config_dir)
            if not params or parameter_name not in params:
                continue

            param_value = params[parameter_name]
            print(f"Processing config: {config_dir} ({parameter_name}={param_value})")

            # Parse aggregated metrics from metrics log
            metrics_path = os.path.join(config_dir_path, 'metrics')
            if os.path.exists(metrics_path):
                log_files = [f for f in os.listdir(metrics_path) if f.endswith('.log')]
                if log_files:
                    log_file_path = os.path.join(metrics_path, log_files[0])
                    scheduler_metrics = SchedulerMetrics(log_file_path)

                    metrics = {
                        'scheduler': params['scheduler'],
                        'qps': params['qps'],
                        'parameter_name': parameter_name,
                        'parameter_value': param_value,
                        'rpc_count': scheduler_metrics.metrics['num_messages'][-1] if scheduler_metrics.metrics['num_messages'] else 0,
                        'makespan_latency_mean': scheduler_metrics.metrics['task_makespan_duration_avg'][-1] if scheduler_metrics.metrics['task_makespan_duration_avg'] else 0,
                        'makespan_latency_p95': scheduler_metrics.metrics['task_makespan_duration_P95'][-1] if scheduler_metrics.metrics['task_makespan_duration_P95'] else 0,
                        'scheduling_latency_mean': scheduler_metrics.metrics['e2e_latency_avg'][-1] if scheduler_metrics.metrics['e2e_latency_avg'] else 0,
                        'scheduling_latency_p95': scheduler_metrics.metrics['e2e_latency_p95'][-1] if scheduler_metrics.metrics['e2e_latency_p95'] else 0,
                        'finished_tasks': scheduler_metrics.metrics['finished_tasks'][-1] if scheduler_metrics.metrics['finished_tasks'] else 0,
                        'throughput': scheduler_metrics.metrics['throughput'][-1] if scheduler_metrics.metrics['throughput'] else 0,
                    }
                    aggregated_data.append(metrics)

            # Parse per-task metrics from service log
            service_log_dir = os.path.join(config_dir_path, 'service_log')
            if os.path.exists(service_log_dir):
                service_log_files = [f for f in os.listdir(service_log_dir) if f.endswith('.log')]
                if service_log_files:
                    service_log_path = os.path.join(service_log_dir, service_log_files[0])
                    per_task_metrics = parse_service_log_per_task_metrics(service_log_path)

                    if param_value not in per_task_data:
                        per_task_data[param_value] = {
                            'scheduling': [],
                            'makespan': []
                        }

                    per_task_data[param_value]['scheduling'].extend(per_task_metrics['scheduling_latencies'])
                    per_task_data[param_value]['makespan'].extend(per_task_metrics['makespan_latencies'])

        except Exception as e:
            print(f"Error processing {config_dir}: {e}")

    return pd.DataFrame(aggregated_data), per_task_data


def plot_aggregated_metrics(df, parameter_name, experiment_name, output_dir):
    """
    Plot aggregated metrics (mean/p95) for different parameter values.
    """
    if df.empty:
        print("No aggregated data to plot.")
        return

    # Get display name for parameter
    display_name = get_display_name(parameter_name)

    plt.style.use('seaborn-v0_8-whitegrid')

    plot_config = {
        'rpc_count': ('RPC Count', 'RPC Messages', False),
        'throughput': ('Throughput', 'Throughput', True),
        'makespan_latency_mean': ('Mean Makespan (ms)', 'Mean Makespan Latency', False),
        'makespan_latency_p95': ('P95 Makespan (ms)', 'P95 Makespan Latency', False),
        'scheduling_latency_mean': ('Mean Scheduling (ms)', 'Mean Scheduling Latency', False),
        'scheduling_latency_p95': ('P95 Scheduling (ms)', 'P95 Scheduling Latency', False),
    }

    num_metrics = len(plot_config)
    num_cols = 3
    num_rows = 2
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(18, 12))
    axes = axes.flatten()

    unique_param_values = sorted(df['parameter_value'].unique())
    palette = sns.color_palette('viridis', n_colors=len(unique_param_values))
    color_map = {param: color for param, color in zip(unique_param_values, palette)}

    for idx, (metric, (ylabel, title, higher_is_better)) in enumerate(plot_config.items()):
        ax = axes[idx]

        # Group by parameter value
        for param_value in unique_param_values:
            param_df = df[df['parameter_value'] == param_value]
            if not param_df.empty:
                ax.bar(param_value, param_df[metric].iloc[0],
                      color=color_map[param_value], alpha=0.8,
                      edgecolor='black', linewidth=0.6)

        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_xlabel(display_name, fontsize=11)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_facecolor('#f7f7f7')

    plt.tight_layout()

    output_path = os.path.join(output_dir, f'aggregated_metrics_{parameter_name}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    print(f"✅ Aggregated metrics plot saved to {output_path}")


def plot_cdf(data_dict, metric_name, parameter_name, experiment_name, output_dir, aggregated_df=None):
    """
    Plot CDF for a specific metric (scheduling or makespan latency) across parameter values.

    Args:
        data_dict: {parameter_value: [latencies]}
        metric_name: 'scheduling' or 'makespan'
        parameter_name: e.g., 'batch_size', 'duration_weight'
        experiment_name: e.g., 'function_100k_tune_batch_100-0-0'
        output_dir: output directory
        aggregated_df: DataFrame with aggregated metrics (for throughput/RPC counts)
    """
    if not data_dict:
        print(f"No data for {metric_name} CDF")
        return

    # Get display name for parameter
    display_name = get_display_name(parameter_name)

    fig, ax = plt.subplots(figsize=(12, 7))

    # Sort parameter values for consistent coloring
    sorted_params = sorted(data_dict.keys())
    palette = sns.color_palette('viridis', n_colors=len(sorted_params))

    # Calculate statistics for each parameter value
    stats_text = []
    for idx, param_value in enumerate(sorted_params):
        latencies = data_dict[param_value]
        if not latencies:
            continue

        # Sort latencies for CDF
        sorted_latencies = np.sort(latencies)
        cdf = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)

        ax.plot(sorted_latencies, cdf, label=f'{display_name}={param_value}',
                linewidth=2.5, color=palette[idx])

        # Calculate statistics
        mean_val = np.mean(latencies)
        p95_val = np.percentile(latencies, 95)
        p999_val = np.percentile(latencies, 99.9)
        max_val = np.max(latencies)
        std_val = np.std(latencies)

        # Build stats text based on metric type (split into 2 lines)
        if metric_name == 'makespan':
            # For makespan: mean, P95, throughput, std (use scientific notation)
            throughput_val = 'N/A'
            if aggregated_df is not None and not aggregated_df.empty:
                param_row = aggregated_df[aggregated_df['parameter_value'] == param_value]
                if not param_row.empty:
                    tp = param_row['throughput'].iloc[0]
                    throughput_val = f"{tp:.2e}"

            # Use scientific notation for all values - split into 2 lines
            stat_line1 = f"{display_name}={param_value}: mean={mean_val:.2e}ms, P95={p95_val:.2e}ms,"
            stat_line2 = f"  std={std_val:.2e}ms, throughput={throughput_val}"
            stats_text.append(stat_line1)
            stats_text.append(stat_line2)
        else:
            # For scheduling: mean, max, P99.9, RPC count - split into 2 lines
            rpc_count = 'N/A'
            if aggregated_df is not None and not aggregated_df.empty:
                param_row = aggregated_df[aggregated_df['parameter_value'] == param_value]
                if not param_row.empty:
                    rpc = int(param_row['rpc_count'].iloc[0])
                    rpc_count = f"{rpc:.2e}"

            stat_line1 = f"{display_name}={param_value}: mean={mean_val:.1f}ms, max={max_val:.1f}ms,"
            stat_line2 = f"  P99.9={p999_val:.1f}ms, RPC={rpc_count}"
            stats_text.append(stat_line1)
            stats_text.append(stat_line2)

    ax.set_xlabel(f'{metric_name.capitalize()} Latency (ms)', fontsize=13)
    ax.set_ylabel('CDF', fontsize=13)
    ax.grid(True, alpha=0.3, linestyle='--')

    # Position legend based on metric type
    if metric_name == 'makespan':
        # For makespan: legend at upper left
        ax.legend(fontsize=11, loc='upper left')
    else:
        # For scheduling: legend at lower right
        ax.legend(fontsize=11, loc='lower right')

    # Add statistics text box
    if stats_text:
        textstr = '\n'.join(stats_text)
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.85)

        if metric_name == 'makespan':
            # For makespan: text at right bottom
            ax.text(0.98, 0.02, textstr, transform=ax.transAxes, fontsize=9,
                    verticalalignment='bottom', horizontalalignment='right',
                    bbox=props, family='monospace')
        else:
            # For scheduling: text higher up to avoid overlapping with legend at lower right
            ax.text(0.98, 0.35, textstr, transform=ax.transAxes, fontsize=9,
                    verticalalignment='bottom', horizontalalignment='right',
                    bbox=props, family='monospace')

    plt.tight_layout()

    output_path = os.path.join(output_dir, f'cdf_{metric_name}_latency_{parameter_name}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"✅ {metric_name.capitalize()} CDF plot saved to {output_path}")


def plot_cdf_on_axis(ax, data_dict, metric_name, parameter_name, aggregated_df=None):
    """
    Plot CDF on a given axis (helper function for both individual and merged plots).
    """
    if not data_dict:
        return

    # Get display name for parameter
    display_name = get_display_name(parameter_name)

    # Sort parameter values for consistent coloring
    sorted_params = sorted(data_dict.keys())
    palette = sns.color_palette('viridis', n_colors=len(sorted_params))

    # Calculate statistics for each parameter value
    stats_text = []
    for idx, param_value in enumerate(sorted_params):
        latencies = data_dict[param_value]
        if not latencies:
            continue

        # Sort latencies for CDF
        sorted_latencies = np.sort(latencies)
        cdf = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)

        ax.plot(sorted_latencies, cdf, label=f'{display_name}={param_value}',
                linewidth=2.5, color=palette[idx])

        # Calculate statistics
        mean_val = np.mean(latencies)
        p95_val = np.percentile(latencies, 95)
        p999_val = np.percentile(latencies, 99.9)
        max_val = np.max(latencies)
        std_val = np.std(latencies)

        display_name = display_name.replace('_', ' ')

        # Build stats text based on metric type
        if metric_name == 'makespan':
            throughput_val = 'N/A'
            if aggregated_df is not None and not aggregated_df.empty:
                param_row = aggregated_df[aggregated_df['parameter_value'] == param_value]
                if not param_row.empty:
                    tp = param_row['throughput'].iloc[0]
                    throughput_val = f"{tp:.1f}"

            stat_line = (f"{display_name}={param_value}: "
                        f"mean={mean_val:.1f}, P95={p95_val:.1f}")
            stat_line_2 = f"std={std_val:.1f}, throughput={throughput_val} \n"
            stats_text.append(stat_line)
            stats_text.append(stat_line_2)

        else:
            rpc_count = 'N/A'
            if aggregated_df is not None and not aggregated_df.empty:
                param_row = aggregated_df[aggregated_df['parameter_value'] == param_value]
                if not param_row.empty:
                    rpc = int(param_row['rpc_count'].iloc[0])
                    rpc_count = f"{rpc:.1f}"

            # stat_line = (f"{display_name}={param_value}: "
            #             f"mean={mean_val:.1f}ms, max={max_val:.1f}ms, \n"
            #             f"P99.9={p999_val:.1f}ms, RPC={rpc_count}")
            stat_line = (f"{display_name}={param_value}: "
                        f"mean={mean_val:.1f}, max={max_val:.1f} RPC={rpc_count} \n")
            stats_text.append(stat_line)


    ax.set_xlabel(f'{metric_name.capitalize()} Latency (ms)', fontsize=18)
    ax.set_ylabel('CDF', fontsize=18)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.tick_params(axis='both', which='major', labelsize=16)

    # Position legend based on metric type with larger font
    if metric_name == 'makespan':
        ax.legend(fontsize=16, loc='upper left')
    else:
        ax.legend(fontsize=16, loc='lower right')

    # Add statistics text box with larger font
    if stats_text:
        textstr = '\n'.join(stats_text)
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.85)

        if metric_name == 'makespan':
            ax.text(0.98, 0.02, textstr, transform=ax.transAxes, fontsize=13,
                    verticalalignment='bottom', horizontalalignment='right',
                    bbox=props, family='monospace')
        else:
            ax.text(0.98, 0.35, textstr, transform=ax.transAxes, fontsize=13,
                    verticalalignment='bottom', horizontalalignment='right',
                    bbox=props, family='monospace')


def plot_merged_cdfs(all_experiments_data, output_dir):
    """
    Create a merged figure with all CDFs in a 2x2 grid.

    Args:
        all_experiments_data: list of tuples (experiment_name, parameter_name, scheduling_data, makespan_data, aggregated_df)
        output_dir: base output directory
    """
    if not all_experiments_data or len(all_experiments_data) != 2:
        print("Warning: Need exactly 2 experiments for merged CDF plot. Skipping.")
        return

    fig, axes = plt.subplots(2, 2, figsize=(20, 14))

    # Experiment 1 (row 0): batch_size
    exp1_name, param1, sched1_data, makespan1_data, agg1_df = all_experiments_data[0]
    plot_cdf_on_axis(axes[0, 0], sched1_data, 'scheduling', param1, agg1_df)
    plot_cdf_on_axis(axes[0, 1], makespan1_data, 'makespan', param1, agg1_df)

    # Experiment 2 (row 1): duration_weight
    exp2_name, param2, sched2_data, makespan2_data, agg2_df = all_experiments_data[1]
    plot_cdf_on_axis(axes[1, 0], sched2_data, 'scheduling', param2, agg2_df)
    plot_cdf_on_axis(axes[1, 1], makespan2_data, 'makespan', param2, agg2_df)

    plt.tight_layout()

    output_path = os.path.join(output_dir, 'merged_cdf_all_experiments.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    print(f"✅ Merged CDF figure saved to {output_path}")


def process_tuning_experiment(experiment_name, experiment_log_dir, output_base_dir):
    """
    Process a single tuning experiment: generate all plots.
    Returns data for merged plot generation.
    """
    print(f"\n{'='*60}")
    print(f"Processing tuning experiment: {experiment_name}")
    print(f"{'='*60}")

    # Detect parameter being tuned
    parameter_name = detect_tuning_parameter(experiment_name)
    if not parameter_name:
        print(f"Warning: Could not detect tuning parameter from experiment name: {experiment_name}")
        return None

    print(f"Detected tuning parameter: {parameter_name}")

    # Parse data
    aggregated_df, per_task_data = parse_experiment_data(experiment_log_dir, experiment_name, parameter_name)

    # Create output directory for this experiment
    output_dir = os.path.join(output_base_dir, experiment_name)
    os.makedirs(output_dir, exist_ok=True)

    # Plot aggregated metrics
    if not aggregated_df.empty:
        plot_aggregated_metrics(aggregated_df, parameter_name, experiment_name, output_dir)
    else:
        print("Warning: No aggregated data found")

    # Plot CDFs for scheduling latency
    scheduling_data = {param: data['scheduling'] for param, data in per_task_data.items()}
    plot_cdf(scheduling_data, 'scheduling', parameter_name, experiment_name, output_dir, aggregated_df)

    # Plot CDFs for makespan latency
    makespan_data = {param: data['makespan'] for param, data in per_task_data.items()}
    plot_cdf(makespan_data, 'makespan', parameter_name, experiment_name, output_dir, aggregated_df)

    # Return data for merged plot
    return (experiment_name, parameter_name, scheduling_data, makespan_data, aggregated_df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze and plot parameter tuning experiments.')
    parser.add_argument('--log_dir', type=str,
                        default='deploy/resources/log/scheduler',
                        help='Parent directory containing scheduler log subdirectories')
    parser.add_argument('--output_dir', type=str,
                        default='deploy/plots/parameter_tune',
                        help='Base directory to save generated plots')

    args = parser.parse_args()

    # Find all tuning experiments
    if not os.path.exists(args.log_dir):
        print(f"Error: Log directory {args.log_dir} does not exist")
        exit(1)

    experiment_dirs = [d for d in os.listdir(args.log_dir)
                      if os.path.isdir(os.path.join(args.log_dir, d)) and '_tune_' in d]

    if not experiment_dirs:
        print(f"No tuning experiments found in {args.log_dir}")
        print("Looking for directories with '_tune_' in the name")
        exit(1)

    print(f"Found {len(experiment_dirs)} tuning experiment(s): {experiment_dirs}")

    # Process each tuning experiment and collect data for merged plot
    all_experiments_data = []
    for experiment_name in experiment_dirs:
        experiment_log_dir = os.path.join(args.log_dir, experiment_name)
        experiment_data = process_tuning_experiment(experiment_name, experiment_log_dir, args.output_dir)
        if experiment_data:
            all_experiments_data.append(experiment_data)

    # Generate merged CDF figure
    if all_experiments_data:
        print(f"\n{'='*60}")
        print(f"Generating merged CDF figure...")
        print(f"{'='*60}")
        plot_merged_cdfs(all_experiments_data, args.output_dir)

    print(f"\n{'='*60}")
    print(f"✅ All tuning experiments processed. Plots saved to {args.output_dir}")
    print(f"{'='*60}")
