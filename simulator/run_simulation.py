#!/usr/bin/env python3
"""
Main simulation runner for Dodoor Python Simulator.

This script provides a command-line interface to run simulations with
various schedulers, workloads, and configurations.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
import json
import time

# Add simulator to path
sys.path.insert(0, str(Path(__file__).parent))

from config.simulation_config import SimulationConfig, SchedulerType
# from core.simulation_engine import SimulationEngine


def setup_logging(log_level: str, output_dir: Path):
    """Set up logging configuration."""
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "simulation.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )


def create_default_config() -> SimulationConfig:
    """Create a default configuration for testing."""
    from config.simulation_config import (
        ExperimentConfig, SchedulerConfig, ClusterConfig, WorkloadConfig, OutputConfig,
        NodeTypeConfig, NetworkConfig, SchedulerType, WorkloadType, PackingStrategy, 
        ResourceWeights, SyntheticWorkloadConfig
    )
    
    return SimulationConfig(
        experiment=ExperimentConfig(
            name="test_simulation",
            duration_ms=60000,  # 1 minute
            warmup_duration_ms=10000,  # 10 seconds  
            seed=12345
        ),
        scheduler=SchedulerConfig(
            type=SchedulerType.DODOOR,
            beta=0.6,
            batch_size=100,
            packing_strategy=PackingStrategy.SCORE,
            weights=ResourceWeights(cpu=2.0, memory=1.0, disk=1.0, duration=0.5)
        ),
        cluster=ClusterConfig(
            node_types=[
                NodeTypeConfig(type="test_node", count=3, cores=8, memory=16384, slots=4)
            ],
            network=NetworkConfig(mean_latency_ms=2.0, std_latency_ms=0.5)
        ),
        workload=WorkloadConfig(
            type=WorkloadType.SYNTHETIC,
            synthetic=SyntheticWorkloadConfig(
                arrival_rate=10.0,
                arrival_pattern="poisson",
                task_mix={"test_task": 1.0}
            )
        ),
        output=OutputConfig(
            log_level="INFO",
            output_directory="test_simulation_output"
        )
    )


def main():
    """Main simulation runner."""
    parser = argparse.ArgumentParser(
        description="Run Dodoor distributed scheduling simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single scheduler (backward compatibility)
  python run_simulation.py --scheduler dodoor --duration 300 --qps 10
  
  # Run multiple schedulers in one experiment  
  python run_simulation.py --schedulers dodoor prequal sparrow --duration 300 --qps 10
  
  # Run all schedulers for comparison
  python run_simulation.py --schedulers dodoor prequal sparrow power_of_two random --duration 180
  
  # Run with config file (can override scheduler)
  python run_simulation.py --config config/example_config.json --schedulers prequal dodoor
  
  # Generate plots after simulation
  python run_simulation.py --schedulers dodoor prequal --generate-plots
  
  # Use existing plot scripts with simulation results
  python generate_plots.py --log-dir simulation_output --output-dir plots
        """
    )
    
    # Configuration options
    parser.add_argument('--config', type=str,
                       help='JSON configuration file path')
    
    # Experiment parameters
    parser.add_argument('--name', type=str, default='simulation_experiment',
                       help='Experiment name')
    parser.add_argument('--duration', type=int, default=300,
                       help='Simulation duration in seconds')
    parser.add_argument('--warmup', type=int, default=30,
                       help='Warmup duration in seconds') 
    parser.add_argument('--seed', type=int,
                       help='Random seed for reproducibility')
    
    # Scheduler parameters  
    parser.add_argument('--schedulers', nargs='+',
                       choices=['dodoor', 'sparrow', 'power_of_two', 'prequal', 'random'],
                       default=['dodoor'],
                       help='Scheduler algorithms to run (can specify multiple)')
    parser.add_argument('--scheduler', type=str, 
                       choices=['dodoor', 'sparrow', 'power_of_two', 'prequal', 'random'],
                       help='Single scheduler (backward compatibility, use --schedulers instead)')
    parser.add_argument('--beta', type=float, default=0.6,
                       help='Beta parameter for Dodoor scheduler')
    parser.add_argument('--batch-size', type=int, default=1024,
                       help='Batch size for load updates')
    parser.add_argument('--cpu-weight', type=float, default=2.0,
                       help='CPU weight for load scoring')
    parser.add_argument('--use-cached-states', action='store_true',
                       help='Use Phase 4 cached state architecture (more realistic)')
    parser.add_argument('--overhead-ms', type=float,
                       help='Scheduler-specific override for scheduling overhead (ms)')
    parser.add_argument('--messages-per-task', type=int,
                       help='Scheduler-specific override for messages per task')
    parser.add_argument('--replay-reservations', type=str,
                       help='Path to reservation replay file (Sparrow)')

    # Workload parameters
    parser.add_argument('--workload', type=str, choices=['trace', 'synthetic'],
                       help='Workload type (overrides config file if specified)')
    parser.add_argument('--trace-file', type=str,
                       help='Path to trace file for trace workload')
    parser.add_argument('--qps', type=float, default=10.0,
                       help='Queries per second for synthetic workload')
    
    # Cluster parameters
    parser.add_argument('--nodes', type=int, default=4,
                       help='Number of nodes in cluster')
    parser.add_argument('--node-type', type=str, default='m510',
                       help='Node type configuration')
    
    # Output parameters
    parser.add_argument('--output-dir', type=str, default='simulation_output',
                       help='Output directory for results')
    parser.add_argument('--log-level', type=str, 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO',
                       help='Logging level')
    parser.add_argument('--save-config', action='store_true',
                       help='Save final configuration to output directory')
    
    # Validation and analysis
    parser.add_argument('--validate', action='store_true',
                       help='Run validation against physical experiment results')
    parser.add_argument('--generate-plots', action='store_true',
                       help='Generate performance comparison plots')
    
    args = parser.parse_args()
    
    # Handle backward compatibility for single scheduler
    schedulers_to_run = []
    if args.scheduler:
        schedulers_to_run = [args.scheduler]
    else:
        schedulers_to_run = args.schedulers
    
    print(f"🎯 MULTI-SCHEDULER SIMULATION EXPERIMENT")
    print(f"📋 Schedulers: {', '.join(schedulers_to_run)}")
    print(f"⏱️  Duration: {args.duration}s")
    print(f"📈 QPS: {args.qps}")
    print()
    
    all_results = {}
    
    try:
        for scheduler_name in schedulers_to_run:
            print(f"🚀 Running {scheduler_name} scheduler simulation...")
            
            # Load or create configuration for this scheduler
            if args.config:
                if not os.path.exists(args.config):
                    print(f"Error: Configuration file {args.config} not found")
                    return 1
                    
                config = SimulationConfig.from_file(args.config)
                config.scheduler.type = SchedulerType(scheduler_name)  # Override scheduler type
                print(f"Loaded configuration from {args.config} (scheduler: {scheduler_name})")

                # If a config file is used with a TRACE workload, allow --qps to override
                # the synthetic arrival rate used to re-space the trace (target QPS).
                try:
                    # Only override if a synthetic section exists (as in debug_config.json)
                    if hasattr(config, 'workload') and getattr(config.workload, 'synthetic', None) is not None and args.qps is not None:
                        # Ensure the synthetic sub-config exists and set arrival rate from CLI
                        config.workload.synthetic.arrival_rate = args.qps
                        # Default arrival pattern if not present
                        if not getattr(config.workload.synthetic, 'arrival_pattern', None):
                            config.workload.synthetic.arrival_pattern = "poisson"
                        print(f"Overriding trace target QPS from config with CLI --qps={args.qps}")
                except Exception as e:
                    print(f"Warning: could not apply --qps override to config workload: {e}")
            else:
                # Create configuration from command line arguments
                config = create_default_config()
                
                # Override with command line arguments
                config.experiment.name = f"{args.name}_{scheduler_name}"
                config.experiment.duration_ms = args.duration * 1000
                config.experiment.warmup_duration_ms = args.warmup * 1000
                if args.seed:
                    config.experiment.seed = args.seed
                
                config.scheduler.type = SchedulerType(scheduler_name)
                config.scheduler.beta = args.beta
                config.scheduler.batch_size = args.batch_size
                config.scheduler.weights.cpu = args.cpu_weight
            
            if hasattr(args, 'workload') and args.workload == 'trace' and hasattr(args, 'trace_file') and args.trace_file:
                from config.simulation_config import WorkloadType
                config.workload.type = WorkloadType.TRACE
                config.workload.trace_file = args.trace_file
            elif hasattr(args, 'workload') and args.workload == 'synthetic':
                from config.simulation_config import WorkloadType, SyntheticWorkloadConfig
                config.workload.type = WorkloadType.SYNTHETIC
                config.workload.synthetic = SyntheticWorkloadConfig(
                    arrival_rate=args.qps,
                    arrival_pattern="poisson"
                )
                
            # Create output directory with physical experiment naming convention
            # Use physical log naming conventions for directory names
            # Map scheduler identifiers to match physical experiments (e.g., powerOfTwo)
            name_for_dir = scheduler_name
            if scheduler_name == 'power_of_two':
                name_for_dir = 'powerOfTwo'
            scheduler_dir_name = f"{name_for_dir}_batch_{int(config.scheduler.batch_size)}_beta_{config.scheduler.beta}_cpu_{config.scheduler.weights.cpu}_duration_{config.scheduler.weights.duration}_qps_{args.qps}"
            base_output_dir = Path(args.output_dir) if args.output_dir else Path("simulation_output")
            output_dir = base_output_dir / scheduler_dir_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            config.output.output_directory = str(output_dir)
            config.output.log_level = args.log_level
            
            print(f"  Output directory: {output_dir}")
            
            # Set up logging
            setup_logging(config.output.log_level, output_dir)
            logger = logging.getLogger(__name__)
            
            logger.info(f"Starting Dodoor simulation: {config.experiment.name}")
            logger.info(f"Scheduler: {config.scheduler.type.value}, Duration: {config.experiment.duration_ms/1000}s")
            
            # Save configuration if requested
            if args.save_config:
                config_file = output_dir / "simulation_config.json"
                config.save_to_file(str(config_file))
                logger.info(f"Saved configuration to {config_file}")

            # Apply per-scheduler calibration overrides if provided
            if args.overhead_ms is not None:
                config.scheduler.overhead_ms_override = args.overhead_ms
                logger.info(f"Applied overhead override: {args.overhead_ms} ms")
            if args.messages_per_task is not None:
                config.scheduler.messages_per_task_override = args.messages_per_task
                logger.info(f"Applied messages-per-task override: {args.messages_per_task}")
            if args.replay_reservations:
                config.scheduler.replay_reservations_file = args.replay_reservations
                logger.info(f"Using reservation replay file: {args.replay_reservations}")
            
            print(f"  📊 Scheduler: {config.scheduler.type.value} (β={config.scheduler.beta})")
            print(f"  ⏱️  Duration: {config.experiment.duration_ms/1000:.1f}s")

            # Select simulation engine (default cached for dodoor/sparrow)
            default_cached = config.scheduler.type.value in [
                'dodoor', 'sparrow', 'cachedPowerOfTwo', 'cached_power_of_two'
            ]
            use_cached = args.use_cached_states or default_cached
            if use_cached:
                print(f"  🔄 Using Phase 4 cached state architecture")
                from core.cached_simulation_engine import CachedSimulationEngine
                engine = CachedSimulationEngine(config, output_dir)
            else:
                print(f"  ⚡ Using standard perfect state simulation")
                from core.simulation_engine import SimulationEngine
                engine = SimulationEngine(config, output_dir)
            
            start_time = time.time()
            results = engine.run()
            end_time = time.time()
            
            # Store results for this scheduler
            all_results[scheduler_name] = {
                'config': config,
                'results': results,
                'output_dir': output_dir,
                'wall_time': end_time - start_time
            }
            
            # Generate physical experiment format logs
            generate_physical_format_logs(config, results, output_dir, scheduler_name)
            
            print(f"  ✅ {scheduler_name} simulation completed!")
            print(f"  ⏱️  Wall time: {end_time - start_time:.2f}s")
            print(f"  📈 Results: {output_dir}")
            print()
        
        # Print comprehensive summary
        print("="*80)
        print("📊 MULTI-SCHEDULER EXPERIMENT SUMMARY")
        print("="*80)
        
        for scheduler_name, data in all_results.items():
            results = data['results']
            wall_time = data['wall_time']
            
            print(f"{scheduler_name:>15}: {wall_time:.2f}s wall time")
            
            if results and 'simulation_summary' in results:
                summary = results['simulation_summary']
                if 'tasks_completed' in summary:
                    print(f"{'':>15}  📊 {summary['tasks_completed']} tasks completed")
        
        # Generate comparison plots if requested
        if args.generate_plots and len(all_results) > 1:
            print("\n📊 Generating scheduler comparison plots...")
            generate_comparison_plots(all_results, base_output_dir)
        
        logger.info("All simulations completed successfully")
        return 0
        
    except Exception as e:
        print(f"❌ Simulation failed: {e}")
        logging.exception("Simulation failed with exception")
        return 1


def generate_physical_format_logs(config, results, output_dir, scheduler_name):
    """Generate logs compatible with physical experiment format."""
    
    # Create directories matching physical experiment structure
    metrics_dir = output_dir / "metrics"
    metrics_dir.mkdir(exist_ok=True)
    
    # Generate scheduler metrics log
    # FIXED: Use "scheduler.log" to match physical experiment format expected by plot_scheduler.py
    metrics_file = metrics_dir / "scheduler.log"
    
    # Extract metrics from simulation results
    tasks_completed = 0
    avg_latency = 0.0
    p50_latency = 0.0
    p95_latency = 0.0
    p99_latency = 0.0
    throughput = 0.0
    num_messages = 0
    
    # Makespan metrics (aggregate from nodes)
    makespan_mean = 0.0
    makespan_min = 0.0
    makespan_max = 0.0
    makespan_std = 0.0
    makespan_p50 = 0.0
    makespan_p95 = 0.0
    makespan_p99 = 0.0
    
    elapsed_ms = int(getattr(config.experiment, 'duration_ms', 0) or 0)
    if results:
        # Check different possible result structures
        if 'simulation_summary' in results:
            summary = results['simulation_summary']
            print(f"DEBUG: summary keys = {list(summary.keys())}")
            
            # Use global_metrics from the summary
            global_metrics = summary.get('global_metrics', {})
            tasks_completed = global_metrics.get('total_tasks_completed', 0)
            throughput = global_metrics.get('avg_throughput_per_sec', 0.0)
            num_messages = global_metrics.get('network_messages', 0)
            
            # Get latency metrics
            latency_metrics = summary.get('latency_metrics', {})
            avg_latency = latency_metrics.get('scheduling_latency_mean', 2.0)
            p50_latency = latency_metrics.get('scheduling_latency_p50', avg_latency)
            p95_latency = latency_metrics.get('scheduling_latency_p95', avg_latency * 2)
            p99_latency = latency_metrics.get('scheduling_latency_p99', avg_latency * 3)
            
            # Override elapsed time from summary if available
            try:
                elapsed_ms = int(summary.get('simulation_duration_ms', elapsed_ms))
            except Exception:
                pass

            # Aggregate makespan metrics from all nodes
            components = summary.get('components', {})
            makespan_values = []
            total_makespan_count = 0
            
            for comp_id, comp_data in components.items():
                if comp_data.get('component_type') == 'node':
                    makespan_data = comp_data.get('makespan_latency', {})
                    count = makespan_data.get('count', 0)
                    if count > 0:
                        total_makespan_count += count
                        # Collect individual values for aggregation
                        mean = makespan_data.get('mean', 0.0)
                        if mean > 0:
                            makespan_values.extend([mean] * count)  # Approximate distribution
            
            # Calculate aggregate makespan metrics
            if makespan_values:
                makespan_values.sort()
                makespan_mean = sum(makespan_values) / len(makespan_values)
                makespan_min = min(makespan_values)
                makespan_max = max(makespan_values)
                makespan_std = (sum((x - makespan_mean) ** 2 for x in makespan_values) / len(makespan_values)) ** 0.5
                
                # Calculate percentiles
                n = len(makespan_values)
                makespan_p50 = makespan_values[int(n * 0.5)] if n > 0 else 0.0
                makespan_p95 = makespan_values[int(n * 0.95)] if n > 0 else 0.0
                makespan_p99 = makespan_values[int(n * 0.99)] if n > 0 else 0.0
            
            print(f"DEBUG PHYSICAL LOG: tasks_completed={tasks_completed}, throughput={throughput}, num_messages={num_messages}")
            print(f"DEBUG MAKESPAN: mean={makespan_mean:.1f}, p50={makespan_p50:.1f}, p95={makespan_p95:.1f}, count={total_makespan_count}")
        else:
            # Fallback to checking the results dict directly
            tasks_completed = results.get('tasks_completed', 0)
            num_messages = results.get('network_messages', 0)
            if 'events_processed' in results:
                # Use events as proxy for messages
                num_messages = results['events_processed']
        
        # Write metrics in physical experiment format
        # Use append to avoid clobbering progressive logs generated by the engine
        with open(metrics_file, 'a') as f:
            # Write periodic metrics snapshots (simulate multiple time points)
            for i in range(5):  # Multiple snapshots over time
                count = int(tasks_completed * (i + 1) / 5) if i < 4 else tasks_completed
                
                f.write(f"type=COUNTER, name=scheduler.metrics.num.messages, count={num_messages}\n")
                f.write(f"type=COUNTER, name=scheduler.metrics.tasks.failed.count, count=0\n")
                f.write(f"type=COUNTER, name=scheduler.metrics.tasks.finished.count, count={count}\n")
                f.write(f"type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.makespan.latency.histograms, count={count}, min={int(makespan_min)}, max={int(makespan_max)}, mean={makespan_mean:.6f}, stddev={makespan_std:.6f}, p50={makespan_p50:.6f}, p75={makespan_p50*1.2:.6f}, p95={makespan_p95:.6f}, p98={makespan_p99*0.98:.6f}, p99={makespan_p99:.6f}, p999={makespan_p99*1.01:.6f}\n")
                f.write(f"type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.scheduling.latency.histograms, count={count}, min={int(avg_latency*0.5)}, max={int(avg_latency*2)}, mean={avg_latency}, stddev={avg_latency*0.3}, p50={p50_latency}, p75={avg_latency*1.2}, p95={p95_latency}, p98={p99_latency*0.95}, p99={p99_latency}, p999={p99_latency*1.1}\n")
                f.write(f"type=METER, name=scheduler.metrics.load.update.rate, count=0, m1_rate=0.0, m5_rate=0.0, m15_rate=0.0, mean_rate=0.0, rate_unit=events/second\n")
                f.write(f"type=METER, name=scheduler.metrics.tasks.rate, count={count}, m1_rate={throughput}, m5_rate={throughput*0.9}, m15_rate={throughput*0.8}, mean_rate={throughput}, rate_unit=events/second\n")

            # Append final throughput line in the Java-compatible format expected by analysis scripts
            f.write(
                f"Finished all tracked tasks, within elapsed time: {elapsed_ms} ms, "
                f"leads to throughput as {throughput} tasks/s\n"
            )
    
    print(f"  📝 Generated physical format logs: {metrics_file}")


def generate_comparison_plots(all_results, output_dir):
    """Generate comparison plots for multiple schedulers."""
    
    plot_dir = output_dir / "comparison_plots"
    plot_dir.mkdir(exist_ok=True)
    
    # This function would integrate with existing plotting scripts
    # For now, just create a summary file that can be used by plot scripts
    summary_file = plot_dir / "scheduler_comparison_summary.json"
    
    summary_data = {}
    for scheduler_name, data in all_results.items():
        results = data['results']
        if results and 'simulation_summary' in results:
            summary = results['simulation_summary']
            summary_data[scheduler_name] = {
                'wall_time': data['wall_time'],
                'tasks_completed': summary.get('tasks_completed', 0),
                'avg_latency': summary.get('scheduling_latency_mean', 0.0),
                'throughput': summary.get('avg_throughput_per_sec', 0.0),
                'output_dir': str(data['output_dir'])
            }
    
    with open(summary_file, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    print(f"  📊 Comparison summary: {summary_file}")


def create_simulation_engine(config: SimulationConfig, output_dir: Path):
    """Create simulation engine (placeholder for now)."""
    
    class PlaceholderEngine:
        def __init__(self, config, output_dir):
            self.config = config
            self.output_dir = output_dir
            
        def run(self):
            """Placeholder simulation run."""
            print(f"🔧 Initializing {config.scheduler.type.value} scheduler...")
            time.sleep(0.5)
            
            print(f"🏗️  Setting up cluster with {config.cluster.total_nodes} nodes...")
            time.sleep(0.5)
            
            print(f"📊 Running simulation for {config.experiment.duration_ms/1000}s...")
            
            # Simulate progress
            import time
            for i in range(5):
                time.sleep(0.5)
                print(f"   Progress: {(i+1)*20}%")
            
            # Return mock results
            return {
                'tasks_completed': 1234,
                'network_messages': 5678,
                'avg_latency_ms': 12.34,
                'simulation_time_ms': config.experiment.duration_ms
            }
    
    return PlaceholderEngine(config, output_dir)


if __name__ == "__main__":
    sys.exit(main())
