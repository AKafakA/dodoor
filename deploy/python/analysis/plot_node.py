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
        params['qps'] = int(float(parts[qps_idx + 1]))
        
        # Extract other parameters
        if 'batch' in parts:
            batch_idx = parts.index('batch')
            params['batch_size'] = float(parts[batch_idx + 1])
            
        if 'cpu' in parts:
            cpu_idx = parts.index('cpu')
            params['cpu_weight'] = float(parts[cpu_idx + 1])
            
    except (ValueError, IndexError):
        return None
        
    return params


def parse_node_logs(log_dir, use_smoothing=False, cpu_weight=None, batch_size=None):
    """
    Parses all node logs for each scheduler experiment and aggregates time-series data.
    For dodoor scheduler, can filter by specific parameter values.
    """
    all_data = []

    experiment_dirs = [d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d))]

    for scheduler_dir in experiment_dirs:
        try:
            # Parse parameters from directory name
            params = parse_parameter_from_name(scheduler_dir)
            if not params:
                # Fallback to old parsing for non-parameterized experiments
                parts = scheduler_dir.split('_')
                scheduler_name = parts[0]
                qps = int(parts[parts.index('qps') + 1])
                params = {'scheduler': scheduler_name, 'qps': qps}
            
            scheduler_name = params['scheduler']
            qps = params['qps']

            print(f"Processing experiment: {scheduler_name} at {qps} QPS...")

            experiment_path = os.path.join(log_dir, scheduler_dir)
            # Check for metrics subdirectory first, fallback to experiment_path if not found
            metrics_path = os.path.join(experiment_path, 'metrics')
            if os.path.exists(metrics_path):
                # Exclude scheduler.log; only parse per-node logs
                node_log_files = [
                    f for f in os.listdir(metrics_path)
                    if f.endswith('.log') and not f.startswith('scheduler')
                ]
                base_path = metrics_path
            else:
                node_log_files = [
                    f for f in os.listdir(experiment_path)
                    if f.endswith('.log') and not f.startswith('scheduler')
                ]
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
                    'timestamp': t,
                    'avg_utilization': np.mean(utilizations),
                    'var_utilization': np.var(utilizations),
                    'avg_waiting_tasks': np.mean(waiting_tasks),
                    'var_waiting_tasks': np.var(waiting_tasks),
                })
        except Exception as e:
            print(f"Could not parse directory {scheduler_dir}. Skipping. Error: {e}")

    if all_data and use_smoothing:
        for scheduler in set(d['scheduler'] for d in all_data):
            for qps in set(d['qps'] for d in all_data if d['scheduler'] == scheduler):
                subset = [d for d in all_data if d['scheduler'] == scheduler and d['qps'] == qps]
                for metric in ['avg_utilization', 'var_utilization']:
                    unsmoothed = [d[metric] for d in subset]
                    smoothed = gaussian_filter1d(unsmoothed, sigma=2)
                    for i, d in enumerate(subset):
                        d[metric] = smoothed[i]

    return pd.DataFrame(all_data)


def plot_metric_category(df, category_name, plot_config, output_dir):
    """
    Helper function to generate a single grid figure for a specific category of metrics.
    """
    unique_qps = sorted(df['qps'].unique())
    num_metrics = len(plot_config)
    num_qps = len(unique_qps)

    if num_qps == 0: return

    fig, axes = plt.subplots(num_metrics, num_qps,
                             figsize=(7 * num_qps, 6 * num_metrics),
                             sharex=True, sharey='row')

    if num_metrics == 1 and num_qps == 1:
        axes = np.array([[axes]])
    elif num_metrics == 1:
        axes = np.array([axes])
    elif num_qps == 1:
        axes = np.array([[ax] for ax in axes])

    # Filter out sparrow (has implementation issues)
    df = df[df['scheduler'] != 'sparrow'].copy()

    # Define fixed scheduler order (matches plot_scheduler.py)
    desired_order = ['random', 'powerOfTwo', 'prequal', 'dodoor']
    all_schedulers = df['scheduler'].unique()
    scheduler_order = [s for s in desired_order if s in all_schedulers]

    palette = sns.color_palette('tab10', n_colors=len(scheduler_order))
    color_map = {scheduler: color for scheduler, color in zip(scheduler_order, palette)}

    for row_idx, (metric, ylabel) in enumerate(plot_config.items()):
        for col_idx, qps in enumerate(unique_qps):
            ax = axes[row_idx, col_idx]
            qps_df = df[df['qps'] == qps]

            sns.lineplot(data=qps_df, x='timestamp', y=metric, hue='scheduler',
                         ax=ax, palette=color_map, linewidth=2.5, legend=False)

            if row_idx == 0: ax.set_title(f'{qps} QPS', fontsize=25, pad=20)
            if col_idx == 0:
                ax.set_ylabel(ylabel, fontsize=25)
            else:
                ax.set_ylabel('')

            if row_idx == num_metrics - 1:
                ax.set_xlabel('Time (seconds)', fontsize=25)
            else:
                ax.set_xlabel('')

            ax.tick_params(axis='both', which='major', labelsize=12)
            ax.grid(True, which='both', linestyle='--', linewidth=0.5)
            ax.spines[['top', 'right']].set_visible(False)

    handles = [mpatches.Patch(color=color_map[scheduler], label=scheduler) for scheduler in scheduler_order]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=len(handles), fontsize=25, title='Scheduler', title_fontsize=25)

    fig.suptitle(f'Node-Level {category_name} Comparison Across Workloads', fontsize=24, y=1.08)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(f"{output_dir}/node_metrics_{category_name.lower().replace(' ', '_')}.png", dpi=300)
    plt.close(fig)


def plot_node_metrics(df, output_dir):
    """
    Generates two separate figures: one for resource utilization and one for waiting tasks.
    """
    if df.empty:
        print("Data is empty. Cannot generate plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    # --- Figure 1: Resource Utilization ---
    resource_config = {
        'avg_utilization': 'Avg. Utilization',
        'var_utilization': 'Variance in Utilization'
    }
    plot_metric_category(df, "Resource Utilization", resource_config, output_dir)
    print("✅ Resource Utilization figure saved.")

    # --- Figure 2: Waiting Tasks ---
    waiting_tasks_config = {
        'avg_waiting_tasks': 'Avg. Waiting Tasks per Node',
        'var_waiting_tasks': 'Variance in Waiting Tasks'
    }
    plot_metric_category(df, "Waiting Tasks", waiting_tasks_config, output_dir)
    print("✅ Waiting Tasks figure saved.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse node logs and generate time-series performance plots.')
    parser.add_argument('--log_dir', type=str,
                        default='deploy/resources/log/node',
                        help='Parent directory containing experiment subdirectories')
    parser.add_argument('--output_dir', type=str,
                        default='deploy/plots',
                        help='Directory to save the generated plots')
    parser.add_argument('--max_qps', type=int, default=10,
                        help='The maximum QPS value to process')
    parser.add_argument("--apply_smoothing", action='store_true',)
    parser.add_argument('--cpu_weight', type=float, default=1.0,
                        help='CPU weight parameter for dodoor scheduler (default: 1.0)')
    parser.add_argument('--batch_size', type=float, default=5.0,
                        help='Batch size parameter for dodoor scheduler (default: 100.0)')

    args = parser.parse_args()

    # Get all experiment subdirectories (exclude parameter tuning experiments)
    if not os.path.exists(args.log_dir):
        print(f"Error: Log directory {args.log_dir} does not exist")
        exit(1)

    experiment_dirs = [d for d in os.listdir(args.log_dir)
                      if os.path.isdir(os.path.join(args.log_dir, d)) and '_tune_' not in d]

    if not experiment_dirs:
        print(f"No experiment directories found in {args.log_dir}")
        exit(1)

    print(f"Found {len(experiment_dirs)} experiment(s): {experiment_dirs}")

    # Process each experiment
    for experiment_name in experiment_dirs:
        print(f"\n{'='*60}")
        print(f"Processing experiment: {experiment_name}")
        print(f"{'='*60}")

        experiment_log_dir = os.path.join(args.log_dir, experiment_name)
        experiment_output_dir = os.path.join(args.output_dir, experiment_name)

        # Parse and plot data for this experiment
        node_data = parse_node_logs(experiment_log_dir,
                                   use_smoothing=True,
                                   cpu_weight=args.cpu_weight,
                                   batch_size=args.batch_size)

        if not node_data.empty:
            plot_node_metrics(node_data, experiment_output_dir)
        else:
            print(f"Warning: No data found for experiment {experiment_name}")

    print(f"\n{'='*60}")
    print(f"✅ All experiments processed. Plots saved to {args.output_dir}")
    print(f"{'='*60}")
