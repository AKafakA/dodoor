import argparse
import os
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
        params['qps'] = int(parts[qps_idx + 1])

        # Extract other parameters
        if 'batch' in parts:
            batch_idx = parts.index('batch')
            params['batch_size'] = float(parts[batch_idx + 1])

        if 'cpu' in parts:
            cpu_idx = parts.index('cpu')
            params['cpu_weight'] = float(parts[cpu_idx + 1])

        if 'beta' in parts:
            beta_idx = parts.index('beta')
            params['beta'] = float(parts[beta_idx + 1])

        if 'duration' in parts:
            duration_idx = parts.index('duration')
            params['duration_weight'] = float(parts[duration_idx + 1])

    except (ValueError, IndexError):
        return None

    return params


def parse_dodoor_log_data(log_dir):
    """
    Parses only Dodoor scheduler log files to extract performance metrics
    and parameter values for comparison analysis.
    """
    data = []
    for scheduler_dir in os.listdir(log_dir):
        scheduler_dir_path = os.path.join(log_dir, scheduler_dir)
        if os.path.isdir(scheduler_dir_path):
            try:
                # Parse parameters from directory name
                params = parse_parameter_from_name(scheduler_dir)
                if not params or params['scheduler'] != 'dodoor':
                    continue

                print(f"Processing Dodoor directory: {scheduler_dir_path}")
                scheduler_dir_metrics_path = scheduler_dir_path + '/metrics'

                # Find log files in metrics directory or fallback
                if os.path.exists(scheduler_dir_metrics_path):
                    log_files = os.listdir(scheduler_dir_metrics_path)
                    base_path = scheduler_dir_metrics_path
                else:
                    log_files = os.listdir(scheduler_dir_path)
                    base_path = scheduler_dir_path

                log_files = [f for f in log_files if f.endswith('.log')]
                if not log_files:
                    print(f"No log files found in {scheduler_dir_path}")
                    continue

                log_file_name = log_files[0]  # assume only one file in the directory
                log_file_path = os.path.join(base_path, log_file_name)

                if os.path.exists(log_file_path):
                    scheduler_metrics = SchedulerMetrics(log_file_path)

                    # Calculate RPC rate instead of total messages
                    total_messages = scheduler_metrics.metrics['num_messages'][-1] if scheduler_metrics.metrics['num_messages'] else 0
                    # total_makespan is in 10ms units (t += 10 per measurement), convert to seconds
                    total_makespan_seconds = scheduler_metrics.metrics.get('total_makespan', 0) / 100.0 if scheduler_metrics.metrics.get('total_makespan', 0) > 0 else 1.0
                    rpc_rate = total_messages / total_makespan_seconds if total_makespan_seconds > 0 else 0

                    # Initialize metrics dictionary with parameter values
                    metrics = {
                        'cpu_weight': params.get('cpu_weight', 1.0),
                        'batch_size': params.get('batch_size', 50.0),
                        'beta': params.get('beta', 0.6),
                        'duration_weight': params.get('duration_weight', 0.5),
                        'qps': params['qps'],
                        'rpc_rate': rpc_rate,
                        'makespan_latency_mean': scheduler_metrics.metrics['task_makespan_duration_avg'][-1] if
                        scheduler_metrics.metrics['task_makespan_duration_avg'] else 0,
                        'makespan_latency_p99': scheduler_metrics.metrics['task_makespan_duration_P99'][-1] if
                        scheduler_metrics.metrics['task_makespan_duration_P99'] else 0,
                        'scheduling_latency_mean': scheduler_metrics.metrics['e2e_latency_avg'][-1] if
                        scheduler_metrics.metrics['e2e_latency_avg'] else 0,
                        'scheduling_latency_p99': scheduler_metrics.metrics['e2e_latency_p99'][-1] if
                        scheduler_metrics.metrics['e2e_latency_p99'] else 0,
                        "finished_tasks": scheduler_metrics.metrics['finished_tasks'][-1] if
                        scheduler_metrics.metrics['finished_tasks'] else 0,
                        'throughput': scheduler_metrics.metrics['throughput'][-1] if
                        scheduler_metrics.metrics['throughput'] else 0,
                    }

                    data.append(metrics)
            except (ValueError, IndexError) as e:
                print(f"Could not parse directory name: {scheduler_dir}. Skipping. Error: {e}")
            except FileNotFoundError:
                print(f"Log file not found for directory: {scheduler_dir}. Skipping.")

    return pd.DataFrame(data)


def plot_parameter_comparison(df, output_dir):
    """
    Generate comparison plots similar to the original scheduler comparison but for different parameter values.
    """
    if df.empty:
        print("Data is empty. Cannot generate parameter comparison plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    plot_config = {
        'rpc_rate': ('RPC Rate (messages/sec)', 'RPC Messages per Second', False),
        'makespan_latency_mean': ('Mean Makespan Latency (µs)', 'Mean E2E Makespan Latency', False),
        'makespan_latency_p99': ('P99 Makespan Latency (µs)', '99th Percentile E2E Makespan Latency', False),
        'scheduling_latency_mean': ('Mean Scheduling Latency (µs)', 'Mean Scheduling Latency', False),
        'scheduling_latency_p99': ('P99 Scheduling Latency (µs)', '99th Percentile Scheduling Latency', False),
        'throughput': ('throughput as number of requests / wall time', 'throughput', True),
    }

    num_metrics = len(plot_config)
    num_metrics_per_row = 3
    height_per_metric = 4
    num_rows = 2
    fig, axes = plt.subplots(num_rows, num_metrics_per_row, sharex=True)
    fig.set_size_inches(num_metrics_per_row * 6, height_per_metric * num_rows)

    axes = axes.flatten() if num_metrics > 1 else [axes]

    # Create parameter combination labels for legend
    df['param_label'] = df.apply(lambda row: f"CPU:{row['cpu_weight']}, Batch:{row['batch_size']}", axis=1)
    unique_params = df['param_label'].unique()
    palette = sns.color_palette('tab10', n_colors=len(unique_params))
    color_map = {param: color for param, color in zip(unique_params, palette)}

    for ax, (metric, (ylabel, title, higher_is_better)) in zip(axes, plot_config.items()):
        qps_values = sorted(df['qps'].unique())
        x_coords = np.arange(len(qps_values))

        bar_width = 0.8 / len(unique_params)

        # Plot bars for each parameter combination
        for i, param_combo in enumerate(unique_params):
            param_data = []
            for qps in qps_values:
                qps_param_df = df[(df['qps'] == qps) & (df['param_label'] == param_combo)]
                if not qps_param_df.empty:
                    param_data.append(qps_param_df[metric].iloc[0])
                else:
                    param_data.append(0)

            bar_x = x_coords + (i - len(unique_params)/2) * bar_width + bar_width/2
            ax.bar(bar_x, param_data, width=bar_width,
                   color=color_map[param_combo], label=param_combo,
                   edgecolor='black', linewidth=0.6)

        ax.set_title(title, fontsize=14, pad=15)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_xticks(x_coords)
        ax.set_xticklabels(qps_values)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_facecolor('#f7f7f7')

    axes[4].set_xlabel('Queries Per Second (QPS)', fontsize=12)

    # Create legend
    handles = [mpatches.Patch(color=color, label=param) for param, color in color_map.items()]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=min(len(handles), 4), fontsize=12, title_fontsize=13, title='Parameter Combinations')

    fig.suptitle('Dodoor Scheduler Parameter Tuning Performance', fontsize=20, y=1.06)
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(f"{output_dir}/dodoor_scheduler_parameter_comparison.png", dpi=300)
    plt.close(fig)

    print(f"✅ Scheduler parameter comparison saved to '{output_dir}'.")


def generate_scheduler_summary_table(df, output_dir):
    """
    Generate a summary table showing best parameter combinations for each metric.
    """
    if df.empty:
        print("Data is empty. Cannot generate scheduler summary.")
        return

    metrics = {
        'makespan_latency_mean': ('Mean Makespan Latency', False),
        'makespan_latency_p99': ('P99 Makespan Latency', False),
        'scheduling_latency_mean': ('Mean Scheduling Latency', False),
        'scheduling_latency_p99': ('P99 Scheduling Latency', False),
        'throughput': ('Throughput', True),
        'rpc_rate': ('RPC Rate', False)
    }

    summary_data = []

    for qps in sorted(df['qps'].unique()):
        qps_df = df[df['qps'] == qps].copy()
        if qps_df.empty:
            continue

        qps_summary = {'QPS': qps}

        for metric, (name, higher_is_better) in metrics.items():
            if metric not in qps_df.columns:
                continue

            if higher_is_better:
                best_row = qps_df.loc[qps_df[metric].idxmax()]
            else:
                best_row = qps_df.loc[qps_df[metric].idxmin()]

            qps_summary[f'Best {name}'] = f"CPU:{best_row['cpu_weight']}, Batch:{best_row['batch_size']} ({best_row[metric]:.2f})"

        summary_data.append(qps_summary)

    summary_df = pd.DataFrame(summary_data)
    summary_path = os.path.join(output_dir, 'dodoor_scheduler_parameter_summary.csv')
    summary_df.to_csv(summary_path, index=False)

    print(f"✅ Scheduler parameter summary saved to {summary_path}")
    print("\nScheduler Parameter Tuning Summary:")
    print(summary_df.to_string(index=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze Dodoor scheduler performance across different parameters.')
    parser.add_argument('--log_dir', type=str,
                        default='deploy/resources/log/scheduler/azure_cpu_weight_tuning_600',
                        help='Directory containing Dodoor scheduler log files')
    parser.add_argument('--output_dir', type=str, default='deploy/plots/parameter_tune',
                        help='Directory to save the generated plots')

    args = parser.parse_args()

    scheduler_data = parse_dodoor_log_data(args.log_dir)

    if scheduler_data.empty:
        print("No Dodoor scheduler data found. Please check the log directory.")
        exit(1)

    print(f"Found {len(scheduler_data)} Dodoor scheduler experiments")
    print("Parameter combinations found:")
    param_summary = scheduler_data.groupby(['cpu_weight', 'batch_size']).size().reset_index(name='count')
    print(param_summary.to_string(index=False))

    plot_parameter_comparison(scheduler_data, args.output_dir)
    generate_scheduler_summary_table(scheduler_data, args.output_dir)