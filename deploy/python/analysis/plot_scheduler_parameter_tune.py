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


def parse_log_data_with_parameters(log_dir, target_scheduler='dodoor', parameter='cpu_weight'):
    """
    Parses scheduler log files for parameter tuning analysis.
    """
    data = []
    
    for scheduler_dir in os.listdir(log_dir):
        scheduler_dir_path = os.path.join(log_dir, scheduler_dir)
        
        if not os.path.isdir(scheduler_dir_path):
            continue
            
        try:
            params = parse_parameter_from_name(scheduler_dir)
            if not params or params['scheduler'] != target_scheduler or parameter not in params:
                continue
                
            print(f"Processing directory: {scheduler_dir_path}")
            scheduler_dir_metrics_path = os.path.join(scheduler_dir_path, 'metrics')
            
            # Find log file
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
                
            log_file_name = log_files[0]  # Assume only one file in the directory
            log_file_path = os.path.join(base_path, log_file_name)
            
            if os.path.exists(log_file_path):
                scheduler_metrics = SchedulerMetrics(log_file_path)

                # Calculate RPC rate instead of total messages
                total_messages = scheduler_metrics.metrics['num_messages'][-1] if scheduler_metrics.metrics['num_messages'] else 0
                # total_makespan is in 10ms units (t += 10 per measurement), convert to seconds
                total_makespan_seconds = scheduler_metrics.metrics.get('total_makespan', 0) / 100.0 if scheduler_metrics.metrics.get('total_makespan', 0) > 0 else 1.0
                rpc_rate = total_messages / total_makespan_seconds if total_makespan_seconds > 0 else 0
                
                # Initialize metrics dictionary
                metrics = {
                    'scheduler': params['scheduler'],
                    'qps': params['qps'],
                    'parameter_name': parameter,
                    'parameter_value': params[parameter],
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
                    'batch_size': params.get('batch_size', 'N/A'),
                    'beta': params.get('beta', 'N/A'),
                    'cpu_weight': params.get('cpu_weight', 'N/A'),
                    'duration_weight': params.get('duration_weight', 'N/A')
                }

                data.append(metrics)
        except (ValueError, IndexError) as e:
            print(f"Could not parse directory name: {scheduler_dir}. Skipping. Error: {e}")
        except FileNotFoundError:
            print(f"Log file not found for directory: {scheduler_dir}. Skipping.")

    return pd.DataFrame(data)


def plot_parameter_metrics(df, parameter_name, output_dir):
    """
    Generates a consolidated figure comparing parameter values across different QPS levels
    for scheduler performance metrics.
    """
    if df.empty:
        print("Data is empty. Cannot generate plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    plot_config = {
        'rpc_rate': ('RPC Rate (messages/sec)', 'RPC Messages per Second', False),
        'makespan_latency_mean': ('Mean Makespan Latency (µs)', 'Mean E2E Makespan Latency', False),
        'makespan_latency_p99': ('P99 Makespan Latency (µs)', '99th Percentile E2E Makespan Latency', False),
        'scheduling_latency_mean': ('Mean Scheduling Latency (µs)', 'Mean Scheduling Latency', False),
        'scheduling_latency_p99': ('P99 Scheduling Latency (µs)', '99th Percentile Scheduling Latency', False),
        'finished_tasks': ('Number of Finished Tasks', 'Finished Tasks', True),
    }

    num_metrics = len(plot_config)
    num_metrics_per_row = 3
    height_per_metric = 4
    num_rows = 2
    fig, axes = plt.subplots(num_rows, num_metrics_per_row, sharex=True)
    fig.set_size_inches(num_metrics_per_row * 6, height_per_metric * num_rows)
    
    if num_metrics == 1:
        axes = [axes]
    axes = axes.flatten() if num_metrics > 1 else [axes]

    unique_param_values = sorted(df['parameter_value'].unique())
    palette = sns.color_palette('viridis', n_colors=len(unique_param_values))
    color_map = {param: color for param, color in zip(unique_param_values, palette)}

    for ax, (metric, (ylabel, title, higher_is_better)) in zip(axes, plot_config.items()):
        qps_values = sorted(df['qps'].unique())
        x_coords = np.arange(len(qps_values))

        bar_width = 0.8 / len(unique_param_values)

        # Plot bars for each parameter value
        for i, param_value in enumerate(unique_param_values):
            param_df = df[df['parameter_value'] == param_value]
            
            y_values = []
            for qps in qps_values:
                qps_param_df = param_df[param_df['qps'] == qps]
                if not qps_param_df.empty:
                    y_values.append(qps_param_df[metric].iloc[0])
                else:
                    y_values.append(0)
            
            bar_positions = x_coords - (len(unique_param_values) / 2 * bar_width) + (i * bar_width) + bar_width / 2
            
            ax.bar(bar_positions, y_values, width=bar_width, 
                  color=color_map[param_value], alpha=0.8,
                  label=f'{parameter_name}={param_value}',
                  edgecolor='black', linewidth=0.6)

        ax.set_title(title, fontsize=14, pad=15)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_xticks(x_coords)
        ax.set_xticklabels(qps_values)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_facecolor('#f7f7f7')

    # Set x-label for bottom row
    axes[4].set_xlabel('Queries Per Second (QPS)', fontsize=12)

    # Create legend
    handles = [mpatches.Patch(color=color, label=f'{parameter_name}={param}') 
              for param, color in color_map.items()]
    
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=min(len(handles), 5), fontsize=12, title_fontsize=13, 
               title=f'{parameter_name} Values')

    fig.suptitle(f'Scheduler Performance - {parameter_name} Parameter Tuning vs. QPS', 
                 fontsize=20, y=1.06)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_filename = f"{output_dir}/scheduler_performance_{parameter_name}.png"
    plt.savefig(output_filename, dpi=300)
    plt.close(fig)

    print(f"✅ Parameter tuning figure saved to '{output_filename}'.")


def generate_parameter_summary_table(df, parameter_name, output_dir):
    """
    Generate a summary table showing parameter configurations and key metrics.
    """
    if df.empty:
        return
        
    # Create summary table
    summary_cols = ['qps', 'parameter_value', 'makespan_latency_mean', 'scheduling_latency_mean', 
                   'finished_tasks', 'throughput', 'batch_size', 'beta', 'cpu_weight', 'duration_weight']
    
    summary_df = df[summary_cols].copy()
    summary_df = summary_df.round(2)
    
    # Save as CSV
    summary_filename = f"{output_dir}/parameter_summary_{parameter_name}.csv"
    summary_df.to_csv(summary_filename, index=False)
    print(f"✅ Parameter summary table saved to '{summary_filename}'.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse scheduler logs for parameter tuning analysis.')
    parser.add_argument('--log_dir', type=str, 
                        default='deploy/resources/log/scheduler/azure_600_timeout_30_full',
                        help='Directory containing scheduler log files')
    parser.add_argument('--output_dir', type=str, 
                        default='deploy/plots/parameter_tune',
                        help='Directory to save the generated plots')
    parser.add_argument('--scheduler', type=str, default='dodoor',
                        help='Scheduler to analyze (default: dodoor)')
    parser.add_argument('--parameter', type=str, default='batch_size',
                        choices=['cpu_weight', 'batch_size', 'beta', 'duration_weight'],
                        help='Parameter to analyze')

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    scheduler_data = parse_log_data_with_parameters(args.log_dir, args.scheduler, args.parameter)
    
    if not scheduler_data.empty:
        plot_parameter_metrics(scheduler_data, args.parameter, args.output_dir)
        generate_parameter_summary_table(scheduler_data, args.parameter, args.output_dir)
        print(f"Parameter tuning analysis complete for {args.parameter}")
    else:
        print(f"No data found for scheduler '{args.scheduler}' with parameter '{args.parameter}'")