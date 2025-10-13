# Dodoor Python Simulator

A high-fidelity discrete event simulator for evaluating distributed task scheduling algorithms, focusing on the Dodoor system and its comparison with state-of-the-art schedulers.

## Overview

This simulator replicates the behavior of the physical Dodoor distributed scheduling system with precise event ordering, network modeling, and scheduler algorithm implementations. It supports all major scheduling strategies and can process real workload traces for realistic performance evaluation.

### Supported Schedulers

- **Dodoor**: (1+β)-choice with cached load balancing and batched updates
- **Sparrow**: Late-binding distributed scheduling with proactive cancellation
- **Prequal**: Google's centralized scheduler with probe pool management
- **Power-of-Two**: Classic load balancing with runtime probing
- **Random**: Baseline random task placement

### Key Features

- **Physical System Compatibility**: Reads same configuration files and generates compatible log formats as the Java implementation
- **High-Fidelity Simulation**: Discrete event simulation with precise task lifecycle modeling
- **Network Modeling**: Configurable network delays and communication overhead simulation
- **Real Workload Support**: Azure VM traces and configurable synthetic workloads
- **Heterogeneous Clusters**: Support for multi-node-type clusters with different resource capacities
- **Performance Metrics**: Comprehensive scheduling latency, throughput, and resource utilization metrics

## Quick Start

### Basic Usage

Run a single scheduler experiment:
```bash
# Basic simulation with debug.sh parameters
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json

# Specify different scheduler
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json --scheduler sparrow

# Multiple schedulers comparison
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json --schedulers dodoor sparrow prequal power_of_two
```

### Debug.sh Physical Experiment Replication

The simulator includes a `debug_config.json` that exactly replicates the physical experiments from `deploy/script/end_to_end_exp/debug.sh`:

```bash
# Run exact debug.sh experiment
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json --scheduler dodoor

# Compare all schedulers with debug.sh parameters
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json --schedulers dodoor sparrow prequal power_of_two --output-dir simulation_output/debug_comparison
```

### Generate Performance Plots

After running experiments, use the existing analysis scripts to generate plots:

```bash
# Generate scheduler performance comparison plots
PYTHONPATH=. python deploy/python/analysis/plot_scheduler.py --log_dir simulation_output/debug_comparison --output_dir plots/

# Generate node utilization plots
PYTHONPATH=. python deploy/python/analysis/plot_node.py --log_dir simulation_output/debug_comparison --output_dir plots/
```

## Configuration

### Configuration File Structure

The simulator uses JSON configuration files with the following structure:

```json
{
  "experiment": {
    "name": "experiment_name",
    "duration_ms": 600000,
    "timeout_ms": 1800000,
    "target_completed_tasks": 2000,
    "seed": 12345
  },
  "scheduler": {
    "type": "dodoor",
    "beta": 1.0,
    "batch_size": 50,
    "weights": {
      "cpu": 1.0,
      "memory": 1.0,
      "disk": 0.0,
      "duration": 0.5
    }
  },
  "cluster": {
    "node_types": [
      {
        "type": "m510",
        "count": 40,
        "cores": 8,
        "memory": 65536,
        "slots": 4
      }
    ],
    "network": {
      "mean_latency_ms": 2.0,
      "std_latency_ms": 0.5
    }
  },
  "workload": {
    "type": "TRACE",
    "trace_file": "deploy/resources/data/azure_data/azure_data_600"
  },
  "output": {
    "output_directory": "simulation_output/experiment",
    "log_level": "INFO",
    "enable_detailed_metrics": true
  }
}
```

### Key Parameters

#### Scheduler Parameters
- **beta**: Power-of-two probability for Dodoor (0.0-1.0)
- **batch_size**: Batching size for load updates (typical: 50-1024)
- **weights**: Resource weighting (cpu, memory, disk, duration)

#### Cluster Configuration
- **node_types**: Heterogeneous node specifications (type, count, cores, memory, slots)
- **network**: Network latency modeling (mean, std deviation)

#### Workload Types
- **TRACE**: Real workload traces (Azure VM data)
- **SYNTHETIC**: Generated synthetic workloads (configurable)

## Scheduler Algorithms

### Dodoor Scheduler

Implements the core Dodoor algorithm with:
- **(1+β)-choice selection**: Probabilistic choice between cached best node and power-of-two sampling
- **Cached load scoring**: Multi-dimensional resource load calculation
- **Batched updates**: Efficient state synchronization with configurable batch sizes
- **Load score formula**: `RL(task,node) = (task_resources^T · (node_load · weights)) / Σweights`

### Physical System Alignment

The simulator maintains high fidelity to the Java implementation:

#### Task Placement Logic
```python
# Matches TaskPlacer.createTaskPlacer() exactly
if scheduler_type == "dodoor":
    placer = CachedTaskPlacer(beta=1.0, packing=SCORE)
elif scheduler_type == "sparrow":
    placer = CachedTaskPlacer(beta=-2.0, packing=NONE)
elif scheduler_type == "power_of_two":
    placer = RunTimeProbeTaskPlacer(beta=1.0, packing=RIF)
elif scheduler_type == "prequal":
    placer = PrequalTaskPlacer(beta=1.0)
```

#### Resource Management
- **Slot-based execution**: Tasks assigned to discrete node slots
- **Capacity tracking**: Real-time resource utilization monitoring
- **Load calculation**: Multi-dimensional scoring matching Java LoadScore.java

#### Communication Patterns
- **Batched state updates**: Scheduler ↔ DataStore communication
- **Network delays**: Configurable latency modeling for all RPC calls
- **Message counting**: Exact tracking of scheduler messages for overhead analysis

## Output and Metrics

### Log File Structure

The simulator generates logs compatible with existing analysis tools:

```
simulation_output/
├── experiment_name/
│   ├── logs/
│   │   └── simulation.log          # High-level simulation events
│   ├── metrics/
│   │   ├── scheduler.log           # Scheduler metrics (compatible with SchedulerMetrics.java)
│   │   ├── node_000.log           # Per-node execution logs
│   │   └── node_001.log
│   └── simulation_metrics.json     # Summary metrics
```

### Performance Metrics

#### Scheduler Metrics
- **Scheduling latency**: Time from task arrival to placement decision
- **RPC message rate**: Communication overhead between components
- **Task completion rate**: Throughput and success rate
- **Load balancing effectiveness**: Resource utilization distribution

#### Task Metrics
- **End-to-end latency**: Full task lifecycle timing
- **Makespan duration**: Task execution time
- **Queue waiting time**: Scheduling and execution delays

#### Node Metrics
- **Resource utilization**: CPU, memory, disk usage over time
- **Task execution patterns**: Local scheduling and completion rates
- **Load distribution**: Fairness across heterogeneous nodes

### Throughput Log Format

Scheduler logs include final throughput entries matching the Java system format:
```
Finished all tracked tasks, within elapsed time: 600000 ms, and lead to throughput of 3.043 requests/seconds
```

This format ensures compatibility with existing plot generation scripts in `deploy/python/analysis/`.

## Advanced Usage

### Custom Scheduler Implementation

To implement a new scheduler:

1. Create a new scheduler class in `schedulers/`:
```python
from .base_scheduler import BaseScheduler

class MyScheduler(BaseScheduler):
    def __init__(self, scheduler_config, cluster_config):
        super().__init__(scheduler_config, cluster_config)

    def schedule_task(self, task, current_time):
        # Implement scheduling logic
        return selected_node_id
```

2. Register in `scheduler_config.py`:
```python
class SchedulerType(Enum):
    MY_SCHEDULER = "my_scheduler"
```

3. Add to scheduler factory in `unified_scheduler.py`

### Custom Workload Generation

For synthetic workloads, modify `workload/` components:

```python
def generate_synthetic_tasks(config):
    # Generate task stream with specified:
    # - Arrival rate (Poisson process)
    # - Resource requirements
    # - Execution duration distribution
    return task_stream
```

### Validation Against Physical System

To validate simulator accuracy:

1. **Run physical experiments**: Use `deploy/script/end_to_end_exp/debug.sh`
2. **Run simulator experiments**: Use `config/debug_config.json`
3. **Compare results**: Use analysis scripts in `deploy/python/analysis/`

Expected performance ranking: `Dodoor > Prequal > Sparrow > PowerOfTwo`

## Architecture

### Core Components

- **simulation_engine.py**: Main discrete event simulation loop
- **unified_scheduler.py**: Scheduler implementations and task placement logic
- **node_executor.py**: Worker node task execution and resource management
- **datastore_service.py**: Centralized state management and load aggregation
- **metrics.py**: Performance measurement and logging
- **network.py**: Network delay and communication modeling

### Scheduler Implementations

- **cached_task_placer.py**: Dodoor and Sparrow algorithms with cached load scoring
- **runtime_probe_task_placer.py**: Power-of-two with real-time probing
- **prequal_task_placer.py**: Google Prequal with probe pool management

### Workload Processing

- **azure_trace_reader.py**: Azure VM trace parsing and task generation
- **trace_reader.py**: Generic trace file processing

## Performance Considerations

### Simulation Speed
- **Event optimization**: Efficient priority queue for discrete events
- **Batch processing**: Grouped state updates reduce simulation overhead
- **Memory management**: Careful object lifecycle management for large experiments

### Accuracy vs Speed Tradeoffs
- **Network modeling**: Balance between realistic delays and simulation speed
- **Event granularity**: Essential events only (no fictional probe protocols)
- **State synchronization**: Match physical system batching behavior

### Scaling Guidelines
- **Node count**: Tested up to 100 heterogeneous nodes
- **Task rate**: Supports up to 30 tasks/second arrival rate
- **Duration**: Validated for 30-minute experiment durations
- **Memory usage**: ~1GB RAM for full-scale experiments

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure `PYTHONPATH=.` when running from project root
2. **Trace file not found**: Check Azure data path in configuration
3. **Empty plots**: Verify scheduler.log files contain throughput entries
4. **Performance differences**: Compare configuration parameters with physical system

### Debug Mode

Enable detailed logging:
```bash
PYTHONPATH=. python simulator/run_simulation.py --config config.json --debug
```

### Validation

Verify simulator correctness:
```bash
# Check scheduler algorithm implementation
python -c "from core.cached_task_placer import CachedTaskPlacer; print('Dodoor algorithm loaded')"

# Validate configuration parsing
python -c "from config.simulation_config import SimulationConfig; print('Config system working')"

# Test trace reading
python -c "from workload.azure_trace_reader import AzureTraceReader; print('Trace reader functional')"
```

## Compatibility

- **Python Version**: 3.8+
- **Dependencies**: Standard library only (no external packages required)
- **Platform**: Cross-platform (Linux, macOS, Windows)
- **Java System**: Compatible with Dodoor Java implementation configuration files and log formats

## Contributing

When modifying the simulator:

1. **Maintain physical system alignment**: Changes should match Java implementation behavior
2. **Preserve log compatibility**: Ensure existing analysis scripts continue to work
3. **Test scheduler rankings**: Validate that performance order matches expectations
4. **Update documentation**: Keep this README current with any architectural changes

For questions about the simulator implementation or validation against physical experiments, refer to the main project documentation and experimental results in `deploy/`.