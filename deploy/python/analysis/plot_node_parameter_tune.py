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


def parse_node_logs_with_parameters(log_dir, target_scheduler='dodoor', parameter='cpu_weight', use_smoothing=False):
    """
    Parses node logs for parameter tuning analysis focusing on a specific scheduler and parameter.
    """
    all_data = []
    experiment_dirs = [d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d))]

    for scheduler_dir in experiment_dirs:
        try:
            params = parse_parameter_from_name(scheduler_dir)
            if not params or params['scheduler'] != target_scheduler or parameter not in params:
                continue
                
            print(f"Processing experiment: {scheduler_dir}...")

            experiment_path = os.path.join(log_dir, scheduler_dir)
            # Check for metrics subdirectory first, fallback to experiment_path if not found
            metrics_path = os.path.join(experiment_path, 'metrics')
            if os.path.exists(metrics_path):
                node_log_files = [f for f in os.listdir(metrics_path) if f.endswith('.log')]
                base_path = metrics_path
            else:
                node_log_files = [f for f in os.listdir(experiment_path) if f.endswith('.log')]
                base_path = experiment_path
                
            # Parse all node logs for this experiment
            node_metrics_list = []
            for log_file in node_log_files:
                node_id = log_file.split('.')[0]
                metrics = NodeMetrics(os.path.join(base_path, log_file), node_id)
                metrics.parse()
                node_metrics_list.append(metrics)

            # Aggregate metrics across all nodes
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
                    'scheduler': params['scheduler'],
                    'qps': params['qps'],
                    'parameter_name': parameter,
                    'parameter_value': params[parameter],
                    'timestamp': t,
                    'avg_utilization': np.mean(utilizations),
                    'var_utilization': np.var(utilizations),
                    'avg_waiting_tasks': np.mean(waiting_tasks),
                    'var_waiting_tasks': np.var(waiting_tasks),
                    'batch_size': params.get('batch_size', 'N/A'),
                    'beta': params.get('beta', 'N/A'),
                    'cpu_weight': params.get('cpu_weight', 'N/A'),
                    'duration_weight': params.get('duration_weight', 'N/A')
                })
        except Exception as e:
            print(f"Could not parse directory {scheduler_dir}. Skipping. Error: {e}")

    if all_data and use_smoothing:
        grouped_data = {}
        for d in all_data:
            key = (d['qps'], d['parameter_value'])
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(d)
            
        for key, subset in grouped_data.items():
            for metric in ['avg_utilization', 'var_utilization']:
                unsmoothed = [d[metric] for d in subset]
                if len(unsmoothed) > 1:
                    smoothed = gaussian_filter1d(unsmoothed, sigma=2)
                    for i, d in enumerate(subset):
                        d[metric] = smoothed[i]

    return pd.DataFrame(all_data)


def plot_parameter_metric_category(df, category_name, plot_config, parameter_name, output_dir):
    """
    Helper function to generate a single grid figure for a specific category of metrics,
    comparing different parameter values across QPS levels.
    """
    unique_qps = sorted(df['qps'].unique())
    unique_param_values = sorted(df['parameter_value'].unique())
    
    num_metrics = len(plot_config)
    num_qps = len(unique_qps)

    if num_qps == 0 or len(unique_param_values) == 0:
        print(f"No data available for {parameter_name}")
        return

    fig, axes = plt.subplots(num_metrics, num_qps,
                             figsize=(7 * num_qps, 6 * num_metrics),
                             sharex=True, sharey='row')

    if num_metrics == 1 and num_qps == 1:
        axes = np.array([[axes]])
    elif num_metrics == 1:
        axes = np.array([axes])
    elif num_qps == 1:
        axes = np.array([[ax] for ax in axes])

    palette = sns.color_palette('viridis', n_colors=len(unique_param_values))
    color_map = {param: color for param, color in zip(unique_param_values, palette)}

    for row_idx, (metric, ylabel) in enumerate(plot_config.items()):
        for col_idx, qps in enumerate(unique_qps):
            ax = axes[row_idx, col_idx]
            qps_df = df[df['qps'] == qps]

            for param_value in unique_param_values:
                param_df = qps_df[qps_df['parameter_value'] == param_value]
                if not param_df.empty:
                    ax.plot(param_df['timestamp'], param_df[metric], 
                           color=color_map[param_value], linewidth=2.5,
                           label=f'{parameter_name}={param_value}')

            if row_idx == 0: 
                ax.set_title(f'{qps} QPS', fontsize=18, pad=20)
            if col_idx == 0:
                ax.set_ylabel(ylabel, fontsize=15)
            else:
                ax.set_ylabel('')

            if row_idx == num_metrics - 1:
                ax.set_xlabel('Time (seconds)', fontsize=15)
            else:
                ax.set_xlabel('')

            ax.tick_params(axis='both', which='major', labelsize=12)
            ax.grid(True, which='both', linestyle='--', linewidth=0.5)
            ax.spines[['top', 'right']].set_visible(False)

    # Create legend for parameter values
    handles = [mpatches.Patch(color=color, label=f'{parameter_name}={param}') 
              for param, color in color_map.items()]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=min(len(handles), 5), fontsize=15, title=f'{parameter_name} Values', 
               title_fontsize=16)

    fig.suptitle(f'Node-Level {category_name} - {parameter_name} Parameter Tuning', 
                 fontsize=24, y=1.08)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    
    output_filename = f"{output_dir}/node_metrics_{category_name.lower().replace(' ', '_')}_{parameter_name}.png"
    plt.savefig(output_filename, dpi=300)
    plt.close(fig)
    print(f"✅ {category_name} figure saved to {output_filename}")


def plot_node_parameter_metrics(df, parameter_name, output_dir):
    """
    Generates parameter tuning plots for node metrics.
    """
    if df.empty:
        print("Data is empty. Cannot generate plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    # Figure 1: Resource Utilization
    resource_config = {
        'avg_utilization': 'Avg. Utilization',
        'var_utilization': 'Variance in Utilization'
    }
    plot_parameter_metric_category(df, "Resource Utilization", resource_config, 
                                 parameter_name, output_dir)

    # Figure 2: Waiting Tasks
    waiting_tasks_config = {
        'avg_waiting_tasks': 'Avg. Waiting Tasks per Node',
        'var_waiting_tasks': 'Variance in Waiting Tasks'
    }
    plot_parameter_metric_category(df, "Waiting Tasks", waiting_tasks_config, 
                                 parameter_name, output_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse node logs for parameter tuning analysis.')
    parser.add_argument('--log_dir', type=str,
                        default='deploy/resources/log/node/azure_600_timeout_30_full',
                        help='Directory containing experiment logs')
    parser.add_argument('--output_dir', type=str,
                        default='deploy/plots/parameter_tune',
                        help='Directory to save the generated plots')
    parser.add_argument('--scheduler', type=str, default='dodoor',
                        help='Scheduler to analyze (default: dodoor)')
    parser.add_argument('--parameter', type=str, default='cpu_weight',
                        choices=['cpu_weight', 'batch_size', 'beta', 'duration_weight'],
                        help='Parameter to analyze')
    parser.add_argument("--apply_smoothing", action='store_true',
                        help='Apply smoothing to time series data')

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    node_data = parse_node_logs_with_parameters(args.log_dir, args.scheduler, 
                                              args.parameter, args.apply_smoothing)
    
    if not node_data.empty:
        plot_node_parameter_metrics(node_data, args.parameter, args.output_dir)
        print(f"Parameter tuning analysis complete for {args.parameter}")
    else:
        print(f"No data found for scheduler '{args.scheduler}' with parameter '{args.parameter}'")