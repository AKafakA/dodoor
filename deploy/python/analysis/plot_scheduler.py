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


def parse_log_data(log_dir, cpu_weight=None, batch_size=None):
    """
    Parses scheduler log files in a given directory to extract performance metrics.
    For dodoor scheduler, can filter by specific parameter values.
    """
    data = []
    for scheduler_dir in os.listdir(log_dir):
        scheduler_dir_path = os.path.join(log_dir, scheduler_dir)
        print(f"Processing directory: {scheduler_dir_path}")
        scheduler_dir_metrics_path = scheduler_dir_path + '/metrics'
        if os.path.isdir(scheduler_dir_path):
            try:
                # Parse parameters from directory name
                params = parse_parameter_from_name(scheduler_dir)
                if not params:
                    # Fallback to old parsing for non-parameterized experiments
                    parts = scheduler_dir.split('_')
                    scheduler_name = parts[0]
                    qps_index = parts.index('qps')
                    qps = int(parts[qps_index + 1])
                    if qps <= 5:
                        continue
                    params = {'scheduler': scheduler_name, 'qps': qps}
                
                scheduler_name = params['scheduler']
                qps = params['qps']
                
                # Filter dodoor experiments by parameters if specified
                if scheduler_name == 'dodoor':
                    if cpu_weight is not None and params.get('cpu_weight') != cpu_weight:
                        continue
                    if batch_size is not None and params.get('batch_size') != batch_size:
                        continue
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
                    rpc_count = total_messages
                    
                    # Initialize metrics dictionary
                    metrics = {
                        'scheduler': scheduler_name,
                        'qps': qps,
                        'rpc_count': rpc_count,
                        'makespan_latency_mean': scheduler_metrics.metrics['task_makespan_duration_avg'][-1] if
                        scheduler_metrics.metrics['task_makespan_duration_avg'] else 0,
                        'makespan_latency_p99': scheduler_metrics.metrics['task_makespan_duration_P95'][-1] if
                        scheduler_metrics.metrics['task_makespan_duration_P95'] else 0,
                        'scheduling_latency_mean': scheduler_metrics.metrics['e2e_latency_avg'][-1] if
                        scheduler_metrics.metrics['e2e_latency_avg'] else 0,
                        'scheduling_latency_p99': scheduler_metrics.metrics['e2e_latency_p95'][-1] if
                        scheduler_metrics.metrics['e2e_latency_p95'] else 0,
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


def plot_metrics(df, output_dir):
    """
    Generates a single, consolidated figure with fixed scheduler ordering across all metrics.
    Shows performance improvement and cost of dodoor compared to the best baseline scheduler.
    """
    if df.empty:
        print("Data is empty. Cannot generate plots.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')

    plot_config = {
        'rpc_count': ('RPC count', 'RPC Messages handled by scheduler', False),
        'throughput': ('Throughput', 'Throughput', True),
        'makespan_latency_mean': ('Mean Makespan Latency (ms)', 'Mean E2E Makespan Latency', False),
        'makespan_latency_p99': ('P95 Makespan Latency (ms)', '95th Percentile E2E Makespan Latency', False),
        'scheduling_latency_mean': ('Mean Scheduling Latency (ms)', 'Mean Scheduling Latency', False),
        'scheduling_latency_p99': ('P95 Scheduling Latency (ms)', '95th Percentile Scheduling Latency', False),
    }

    num_metrics = len(plot_config)
    num_metrics_per_row = 2
    height_per_metric = 4  # Height for each subplot
    num_rows = 3
    fig, axes = plt.subplots(num_rows, num_metrics_per_row, sharex=True)
    fig.set_size_inches(num_metrics_per_row * 8, height_per_metric * num_rows)  # Adjust size based on number of rows
    # Ensure axes is always an array, even if there's only one subplot
    if num_metrics == 1:
        axes = [axes]
    axes = axes.flatten() if num_metrics > 1 else [axes]

    improvement_shown_globally = False
    cost_shown_globally = False

    # Filter out sparrow (has implementation issues)
    df = df[df['scheduler'] != 'sparrow'].copy()

    # Define fixed scheduler order
    desired_order = ['random', 'powerOfTwo', 'prequal', 'dodoor']
    all_schedulers = df['scheduler'].unique()
    scheduler_order = [s for s in desired_order if s in all_schedulers]

    palette = sns.color_palette('tab10', n_colors=len(scheduler_order))
    color_map = {scheduler: color for scheduler, color in zip(scheduler_order, palette)}

    for ax, (metric, (ylabel, title, higher_is_better)) in zip(axes, plot_config.items()):

        qps_values = sorted(df['qps'].unique())
        x_coords = np.arange(len(qps_values))  # The center of each QPS group

        bar_width = 0.8 / len(scheduler_order)  # Adjust bar width based on number of schedulers

        # --- Per-QPS Plotting Loop ---
        for i, qps in enumerate(qps_values):
            qps_df = df[df['qps'] == qps].copy()

            # Use fixed scheduler order (no per-QPS ranking)
            for j, scheduler in enumerate(scheduler_order):
                if scheduler not in qps_df['scheduler'].values:
                    continue

                row = qps_df[qps_df['scheduler'] == scheduler].iloc[0]
                bar_x = x_coords[i] - (len(scheduler_order) / 2 * bar_width) + (j * bar_width) + bar_width / 2
                ax.bar(bar_x, row[metric], width=bar_width, color=color_map[scheduler],
                       edgecolor='black', linewidth=0.6)

            # --- Annotation Logic: Show cost and improvement ---
            if 'dodoor' in qps_df['scheduler'].values and len(qps_df) >= 2:
                dodoor_row = qps_df[qps_df['scheduler'] == 'dodoor'].iloc[0]
                dodoor_y = dodoor_row[metric]
                dodoor_idx = scheduler_order.index('dodoor')

                # Find best and second-best baseline schedulers (excluding dodoor)
                baseline_df = qps_df[qps_df['scheduler'] != 'dodoor'].copy()
                if higher_is_better:
                    baseline_df = baseline_df.sort_values(metric, ascending=False)
                else:
                    baseline_df = baseline_df.sort_values(metric, ascending=True)

                # Find best baseline that doesn't tie with dodoor (for meaningful comparison)
                best_baseline = None
                best_baseline_y = None
                best_baseline_idx = None

                for idx in range(len(baseline_df)):
                    candidate = baseline_df.iloc[idx]
                    if abs(candidate[metric] - dodoor_y) > 1e-6:  # Not a tie (allow small floating point tolerance)
                        best_baseline = candidate['scheduler']
                        best_baseline_y = candidate[metric]
                        best_baseline_idx = scheduler_order.index(best_baseline)
                        break

                # If all baselines tie with dodoor, skip annotations
                if best_baseline is None:
                    continue

                # Determine if dodoor is better or worse than best baseline
                if higher_is_better:
                    dodoor_is_best = dodoor_y > best_baseline_y
                else:
                    dodoor_is_best = dodoor_y < best_baseline_y

                # RED ZONE: Cost - if dodoor is worse than best baseline
                if not dodoor_is_best:
                    cost_shown_globally = True
                    if best_baseline_y != 0:
                        cost_pct = abs((dodoor_y - best_baseline_y) / best_baseline_y) * 100
                    else:
                        cost_pct = 0

                    if higher_is_better:
                        # Draw red zone on dodoor bar (at top) showing how much it falls short
                        dodoor_bar_x = x_coords[i] - (len(scheduler_order) / 2 * bar_width) + (dodoor_idx * bar_width) + bar_width / 2
                        red_box_y = dodoor_y
                        red_box_height = best_baseline_y - dodoor_y
                        red_bar_x = dodoor_bar_x
                    else:
                        # Draw red zone on best baseline bar showing how much dodoor exceeds it
                        best_bar_x = x_coords[i] - (len(scheduler_order) / 2 * bar_width) + (best_baseline_idx * bar_width) + bar_width / 2
                        red_box_y = best_baseline_y
                        red_box_height = dodoor_y - best_baseline_y
                        red_bar_x = best_bar_x

                    rect = mpatches.Rectangle((red_bar_x - bar_width / 2, red_box_y), bar_width, red_box_height,
                                              facecolor='#d62728', alpha=0.4, ec='#d62728', lw=1.5, ls='--')
                    ax.add_patch(rect)
                    ax.text(red_bar_x, red_box_y + red_box_height * 1.05, f'{cost_pct:.1f}%',
                            ha='center', va='bottom', color='white', fontsize=10, fontweight='bold',
                            bbox=dict(boxstyle='round,pad=0.3', facecolor='#d62728', alpha=0.8, edgecolor='#d62728'))

                # GREEN ZONE: Improvement - compare dodoor with next-worse baseline
                # For lower-is-better: draw on dodoor bar
                # For higher-is-better: draw on baseline bar to avoid overlap
                if len(baseline_df) >= 1:
                    # Find baseline that is closest to dodoor but worse (and not tied)
                    if dodoor_is_best:
                        # Dodoor is best, compare with best baseline (second overall)
                        comparison_baseline = best_baseline
                        comparison_baseline_y = best_baseline_y
                        comparison_baseline_idx = best_baseline_idx
                    else:
                        # Find baseline worse than dodoor (excluding ties)
                        comparison_baseline = None
                        for idx in range(len(baseline_df)):
                            candidate = baseline_df.iloc[idx]
                            candidate_y = candidate[metric]
                            # Check if worse AND not tied
                            is_worse = (candidate_y > dodoor_y) if not higher_is_better else (candidate_y < dodoor_y)
                            is_not_tied = abs(candidate_y - dodoor_y) > 1e-6
                            if is_worse and is_not_tied:
                                comparison_baseline = candidate['scheduler']
                                comparison_baseline_y = candidate_y
                                comparison_baseline_idx = scheduler_order.index(comparison_baseline)
                                break

                    if comparison_baseline:
                        improvement_shown_globally = True
                        if comparison_baseline_y != 0:
                            improvement_pct = abs((comparison_baseline_y - dodoor_y) / comparison_baseline_y) * 100
                        else:
                            improvement_pct = 0

                        if higher_is_better:
                            # Draw green zone on baseline bar to avoid overlap with dodoor
                            comparison_bar_x = x_coords[i] - (len(scheduler_order) / 2 * bar_width) + (comparison_baseline_idx * bar_width) + bar_width / 2
                            green_box_y = comparison_baseline_y
                            green_box_height = dodoor_y - comparison_baseline_y
                            green_bar_x = comparison_bar_x
                        else:
                            # Draw green zone on dodoor bar
                            dodoor_bar_x = x_coords[i] - (len(scheduler_order) / 2 * bar_width) + (dodoor_idx * bar_width) + bar_width / 2
                            green_box_y = dodoor_y
                            green_box_height = comparison_baseline_y - dodoor_y
                            green_bar_x = dodoor_bar_x

                        if green_box_height > 0:  # Only draw if there's actual improvement
                            rect = mpatches.Rectangle((green_bar_x - bar_width / 2, green_box_y), bar_width, green_box_height,
                                                      facecolor='#2ca02c', alpha=0.4, ec='#2ca02c', lw=1.5, ls='--')
                            ax.add_patch(rect)
                            ax.text(green_bar_x, green_box_y + green_box_height * 1.05, f'{improvement_pct:.1f}%',
                                    ha='center', va='bottom', color='white', fontsize=10, fontweight='bold',
                                    bbox=dict(boxstyle='round,pad=0.3', facecolor='#2ca02c', alpha=0.8, edgecolor='#2ca02c'))

        ax.set_title(title, fontsize=20, pad=15)
        ax.set_ylabel(ylabel, fontsize=15)
        ax.set_xticks(x_coords)
        ax.set_xticklabels(qps_values, fontsize=13)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.spines[['top', 'right']].set_visible(False)
        ax.set_facecolor('#f7f7f7')

    # --- Final Touches on the Combined Figure ---
    axes[4].set_xlabel('Queries Per Second (QPS)', fontsize=15)

    handles = [mpatches.Patch(color=color_map[scheduler], label=scheduler) for scheduler in scheduler_order]
    if improvement_shown_globally:
        handles.append(mpatches.Patch(facecolor='#2ca02c', alpha=0.4, edgecolor='#2ca02c',
                                     label='Dodoor Improvement (vs next-worse baseline)'))
    if cost_shown_globally:
        handles.append(mpatches.Patch(facecolor='#d62728', alpha=0.4, edgecolor='#d62728',
                                     label='Dodoor Cost (vs best baseline)'))

    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=len(handles), fontsize=15, title_fontsize=15, title='Scheduler')

    fig.suptitle('Scheduler Performance Comparison vs. QPS (Fixed Order)', fontsize=20, y=1.06)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(f"{output_dir}/scheduler_performance_figure.png", dpi=300)
    plt.close(fig)

    print(f"✅ Fixed-order comparison figure with dodoor vs best baseline saved to '{output_dir}'.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse scheduler logs and generate performance plots.')
    parser.add_argument('--log_dir', type=str, default='deploy/resources/log/scheduler',
                        help='Parent directory containing experiment subdirectories')
    parser.add_argument('--output_dir', type=str, default='deploy/plots',
                        help='Directory to save the generated plots')
    parser.add_argument('--cpu_weight', type=float, default=1.0,
                        help='CPU weight parameter for dodoor scheduler (default: 2.0)')
    parser.add_argument('--batch_size', type=float, default=50.0,
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
        scheduler_data = parse_log_data(experiment_log_dir,
                                       cpu_weight=args.cpu_weight,
                                       batch_size=args.batch_size)

        if not scheduler_data.empty:
            plot_metrics(scheduler_data, experiment_output_dir)
        else:
            print(f"Warning: No data found for experiment {experiment_name}")

    print(f"\n{'='*60}")
    print(f"✅ All experiments processed. Plots saved to {args.output_dir}")
    print(f"{'='*60}")
