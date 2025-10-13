import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.patches as mpatches
from scipy.ndimage import gaussian_filter1d

from deploy.python.analysis.node_metrics import NodeMetrics


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


def parse_dodoor_node_logs(log_dir, use_smoothing=False):
    """
    Parses all node logs for Dodoor experiments and aggregates time-series data
    with parameter information for comparison analysis.
    """
    all_data = []

    experiment_dirs = [d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d))]

    for scheduler_dir in experiment_dirs:
        try:
            # Parse parameters from directory name
            params = parse_parameter_from_name(scheduler_dir)
            if not params or params['scheduler'] != 'dodoor':
                continue

            scheduler_name = params['scheduler']
            qps = params['qps']
            cpu_weight = params.get('cpu_weight', 1.0)
            batch_size = params.get('batch_size', 50.0)
            beta = params.get('beta', 0.6)
            duration_weight = params.get('duration_weight', 0.5)

            print(f"Processing Dodoor node experiment: {scheduler_name} at {qps} QPS (CPU:{cpu_weight}, Batch:{batch_size})...")

            experiment_path = os.path.join(log_dir, scheduler_dir)
            # Check for metrics subdirectory first, fallback to experiment_path if not found
            metrics_path = os.path.join(experiment_path, 'metrics')
            if os.path.exists(metrics_path):
                node_log_files = [f for f in os.listdir(metrics_path) if f.endswith('.log')]
                base_path = metrics_path
            else:
                node_log_files = [f for f in os.listdir(experiment_path) if f.endswith('.log')]
                base_path = experiment_path

            # --- Parse all node logs for this experiment ---
            node_metrics_list = []
            for log_file in node_log_files:
                node_id = log_file.split('.')[0]
                metrics = NodeMetrics(os.path.join(base_path, log_file), node_id)
                metrics.parse()
                node_metrics_list.append(metrics)

            # --- Aggregate metrics across all nodes ---
            if not node_metrics_list:
                continue

            # Find the common length for time series
            min_len = min(m.length for m in node_metrics_list)

            for t in range(min_len):  # Iterate through each timestamp
                utilizations = []
                waiting_tasks = []
                for metrics_obj in node_metrics_list:
                    # Resource utilization = (CPU + Memory) / 2
                    util = (metrics_obj.metrics['cpu_usage'][t] + metrics_obj.metrics['mem_usage'][t]) / 2
                    utilizations.append(util)
                    waiting_tasks.append(metrics_obj.metrics['num_waiting_tasks'][t])

                # Calculate aggregated metrics for this timestamp
                all_data.append({
                    'scheduler': scheduler_name,
                    'qps': qps,
                    'cpu_weight': cpu_weight,
                    'batch_size': batch_size,
                    'beta': beta,
                    'duration_weight': duration_weight,
                    'timestamp': t,
                    'avg_utilization': np.mean(utilizations),
                    'var_utilization': np.var(utilizations),
                    'avg_waiting_tasks': np.mean(waiting_tasks),
                    'var_waiting_tasks': np.var(waiting_tasks),
                })
        except Exception as e:
            print(f"Could not parse directory {scheduler_dir}. Skipping. Error: {e}")

    if all_data and use_smoothing:
        # Group by parameter combinations for smoothing
        for combo in set((d['cpu_weight'], d['batch_size'], d['qps']) for d in all_data):
            cpu_weight, batch_size, qps = combo
            subset = [d for d in all_data
                     if d['cpu_weight'] == cpu_weight and d['batch_size'] == batch_size and d['qps'] == qps]
            for metric in ['avg_utilization', 'var_utilization']:
                unsmoothed = [d[metric] for d in subset]
                smoothed = gaussian_filter1d(unsmoothed, sigma=2)
                for i, d in enumerate(subset):
                    d[metric] = smoothed[i]

    return pd.DataFrame(all_data)


def calculate_node_summary_metrics(df):
    """
    Calculate summary metrics for each parameter combination.
    """
    if df.empty:
        return pd.DataFrame()

    summary_metrics = []

    # Group by parameter combinations
    param_groups = df.groupby(['cpu_weight', 'batch_size', 'qps'])

    for (cpu_weight, batch_size, qps), group in param_groups:
        summary = {
            'cpu_weight': cpu_weight,
            'batch_size': batch_size,
            'qps': qps,
            'avg_utilization_mean': group['avg_utilization'].mean(),
            'avg_utilization_std': group['avg_utilization'].std(),
            'var_utilization_mean': group['var_utilization'].mean(),
            'var_utilization_std': group['var_utilization'].std(),
            'avg_waiting_tasks_mean': group['avg_waiting_tasks'].mean(),
            'avg_waiting_tasks_std': group['avg_waiting_tasks'].std(),
            'var_waiting_tasks_mean': group['var_waiting_tasks'].mean(),
            'var_waiting_tasks_std': group['var_waiting_tasks'].std(),
        }
        summary_metrics.append(summary)

    return pd.DataFrame(summary_metrics)


def plot_node_parameter_timeseries(df, output_dir):
    """
    Generate time-series plots showing node metrics over time for different parameter combinations.
    """
    if df.empty:
        print("Data is empty. Cannot generate node time-series plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    # Metrics to plot
    node_metrics = {
        'avg_utilization': 'Avg. Resource Utilization',
        'var_utilization': 'Variance in Resource Utilization',
        'avg_waiting_tasks': 'Avg. Waiting Tasks per Node',
        'var_waiting_tasks': 'Variance in Waiting Tasks'
    }

    # Create parameter combination labels
    df['param_label'] = df.apply(lambda row: f"CPU:{row['cpu_weight']}, Batch:{row['batch_size']}", axis=1)
    unique_params = df['param_label'].unique()
    palette = sns.color_palette('tab10', n_colors=len(unique_params))
    color_map = {param: color for param, color in zip(unique_params, palette)}

    qps_values = sorted(df['qps'].unique())

    for qps in qps_values:
        qps_df = df[df['qps'] == qps]

        if qps_df.empty:
            continue

        print(f"Generating node time-series plots for QPS {qps}")

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        axes = axes.flatten()

        for idx, (metric, ylabel) in enumerate(node_metrics.items()):
            if idx >= len(axes):
                break

            ax = axes[idx]

            # Plot time series for each parameter combination
            for param_combo in unique_params:
                param_data = qps_df[qps_df['param_label'] == param_combo]
                if not param_data.empty:
                    ax.plot(param_data['timestamp'], param_data[metric],
                           color=color_map[param_combo], label=param_combo,
                           linewidth=2, alpha=0.8)

            ax.set_title(ylabel, fontsize=14)
            ax.set_ylabel(ylabel, fontsize=12)
            ax.set_xlabel('Time (seconds)', fontsize=12)
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=9)
            ax.spines[['top', 'right']].set_visible(False)

        plt.suptitle(f'Dodoor Node Performance Time Series - QPS {qps}', fontsize=16, y=0.98)
        plt.tight_layout()

        plt.savefig(f'{output_dir}/dodoor_node_timeseries_qps_{qps}.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✅ Node time-series plot for QPS {qps} saved to {output_dir}")


def plot_node_parameter_comparison(summary_df, output_dir):
    """
    Generate comparison plots for node metrics across parameter combinations.
    """
    if summary_df.empty:
        print("Data is empty. Cannot generate node parameter comparison plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    plot_config = {
        'avg_utilization_mean': ('Average Resource Utilization', 'Avg. Utilization', True),
        'var_utilization_mean': ('Variance in Resource Utilization', 'Utilization Variance', False),
        'avg_waiting_tasks_mean': ('Average Waiting Tasks per Node', 'Avg. Waiting Tasks', False),
        'var_waiting_tasks_mean': ('Variance in Waiting Tasks', 'Waiting Tasks Variance', False),
    }

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    axes = axes.flatten()

    # Create parameter combination labels for legend
    summary_df['param_label'] = summary_df.apply(lambda row: f"CPU:{row['cpu_weight']}, Batch:{row['batch_size']}", axis=1)
    unique_params = summary_df['param_label'].unique()
    palette = sns.color_palette('tab10', n_colors=len(unique_params))
    color_map = {param: color for param, color in zip(unique_params, palette)}

    for idx, (metric, (ylabel, title, higher_is_better)) in enumerate(plot_config.items()):
        if idx >= len(axes):
            break

        ax = axes[idx]
        qps_values = sorted(summary_df['qps'].unique())
        x_coords = np.arange(len(qps_values))

        bar_width = 0.8 / len(unique_params)

        # Plot bars for each parameter combination
        for i, param_combo in enumerate(unique_params):
            param_data = []
            for qps in qps_values:
                qps_param_df = summary_df[(summary_df['qps'] == qps) & (summary_df['param_label'] == param_combo)]
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

    # Create legend
    handles = [mpatches.Patch(color=color, label=param) for param, color in color_map.items()]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=min(len(handles), 4), fontsize=12, title_fontsize=13, title='Parameter Combinations')

    fig.suptitle('Dodoor Node Parameter Tuning Performance', fontsize=20, y=1.06)
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(f"{output_dir}/dodoor_node_parameter_comparison.png", dpi=300)
    plt.close(fig)

    print(f"✅ Node parameter comparison saved to '{output_dir}'.")


def generate_node_summary_table(summary_df, output_dir):
    """
    Generate a summary table showing best parameter combinations for each node metric.
    """
    if summary_df.empty:
        print("Data is empty. Cannot generate node summary.")
        return

    metrics = {
        'avg_utilization_mean': ('Average Utilization', True),
        'var_utilization_mean': ('Utilization Variance', False),
        'avg_waiting_tasks_mean': ('Average Waiting Tasks', False),
        'var_waiting_tasks_mean': ('Waiting Tasks Variance', False)
    }

    summary_data = []

    for qps in sorted(summary_df['qps'].unique()):
        qps_df = summary_df[summary_df['qps'] == qps].copy()
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

            qps_summary[f'Best {name}'] = f"CPU:{best_row['cpu_weight']}, Batch:{best_row['batch_size']} ({best_row[metric]:.3f})"

        summary_data.append(qps_summary)

    summary_df_final = pd.DataFrame(summary_data)
    summary_path = os.path.join(output_dir, 'dodoor_node_parameter_summary.csv')
    summary_df_final.to_csv(summary_path, index=False)

    print(f"✅ Node parameter summary saved to {summary_path}")
    print("\nNode Parameter Tuning Summary:")
    print(summary_df_final.to_string(index=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze Dodoor node performance across different parameters.')
    parser.add_argument('--log_dir', type=str, required=True,
                        help='Directory containing Dodoor node log files')
    parser.add_argument('--output_dir', type=str, default='deploy/plots/parameter_tune',
                        help='Directory to save the generated plots')
    parser.add_argument("--apply_smoothing", action='store_true',
                        help='Apply smoothing to time series data')

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    print("Parsing Dodoor node experiment data...")
    node_data = parse_dodoor_node_logs(args.log_dir, use_smoothing=args.apply_smoothing)

    if node_data.empty:
        print("No Dodoor node data found. Please check the log directory.")
        exit(1)

    print(f"Found {len(node_data)} Dodoor node measurement points")
    print("Parameter combinations found:")
    param_summary = node_data.groupby(['cpu_weight', 'batch_size']).size().reset_index(name='count')
    print(param_summary.to_string(index=False))

    # Calculate summary metrics
    summary_metrics = calculate_node_summary_metrics(node_data)

    # Generate plots
    plot_node_parameter_timeseries(node_data, args.output_dir)
    plot_node_parameter_comparison(summary_metrics, args.output_dir)
    generate_node_summary_table(summary_metrics, args.output_dir)