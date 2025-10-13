# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Development Commands

### Building the Project
```bash
# Build project (generates Thrift code and compiles)
./rebuild.sh

# Manual build steps
mvn clean package
mvn compile
mvn package -Dmaven.test.skip=true
```

### Running System Components
```bash
# Start DataStore
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.datastore.DataStore --config deploy/resources/configuration/example_dodoor_configuration.conf

# Start Scheduler
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.scheduler.Scheduler --config deploy/resources/configuration/example_dodoor_configuration.conf

# Start Worker Nodes
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.node.NodeImpl --config deploy/resources/configuration/example_dodoor_configuration.conf
```

### Running Simulator
```bash
# Quick test with debug configuration
PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json --schedulers dodoor sparrow prequal power_of_two

# Generate performance plots
PYTHONPATH=. python deploy/python/analysis/plot_scheduler.py --log_dir simulation_output/debug_comparison --output_dir plots/
```

## Core Architecture

Dodoor is a distributed task scheduling system implementing cached load balancing to reduce probing overhead in heterogeneous clusters.

### Key Components

**Scheduler** (`src/main/java/edu/cam/dodoor/scheduler/SchedulerImpl.java`)
- Central placement decisions using cached node states
- Supports multiple scheduling strategies via TaskPlacer interface

**DataStore** (`src/main/java/edu/cam/dodoor/datastore/`)
- Centralized state management for cluster resources
- Aggregates and distributes load information in batches

**Node Monitor** (`src/main/java/edu/cam/dodoor/node/NodeImpl.java`)  
- Worker nodes executing tasks and reporting resource usage
- Local task scheduling with FIFO or late-binding queues

**Task Placer Strategies** (`src/main/java/edu/cam/dodoor/scheduler/taskplacer/`)
- `CachedTaskPlacer.java`: Dodoor algorithm with (1+β)-choice
- `RunTimeProbeTaskPlacer.java`: Power-of-two with runtime probing  
- `PrequalTaskPlacer.java`: Google Prequal scheduler implementation

### Communication Architecture
```
Client → Scheduler → DataStore ← Nodes
         ↓
    TaskPlacer → Nodes (task assignment)
```

All communication uses Apache Thrift RPC with configurable thread pools.

## Scheduling Algorithms

### Dodoor Scheduler (Primary Innovation)
- **Algorithm**: (1+β)-choice with cached loads and batched updates
- **Load Metric**: Multi-dimensional resource load score combining CPU/memory/disk
- **Key Parameters**: 
  - β (beta): Power-of-two probability (default 0.6)
  - α (alpha): Resource vs duration balance weight
  - Batch size for cache updates (default 1024)

### Resource Load Scoring
```java
RL(task, server) = (task_resources^T · (server_load · weights)) / Σweights
Final_Load = (RL / Σ_RL) * (1-α) + (duration / Σ_duration) * α
```

### Other Implemented Schedulers
- **Prequal**: Google's centralized scheduler with probe pool management
- **Sparrow**: Late-binding with proactive cancellation
- **Power-of-Two**: Runtime probing vs cached variants
- **Random**: Baseline random placement

## Configuration Files

**System Configuration**: `deploy/resources/configuration/example_dodoor_configuration.conf`
- Node endpoints, resource capacities, Thrift ports
- Scheduler parameters (beta, batch_size, thread counts)

**Host Configuration**: `deploy/resources/configuration/host_config_template.json`
- Node specifications and resource profiles

**Task Configurations**: `deploy/resources/configuration/generated_config/`
- Workload definitions, resource requirements, execution parameters

## Code Organization

### Core Scheduling Logic
- `SchedulerImpl.java`: Main scheduling loop and job submission handling
- `taskplacer/`: Pluggable scheduling strategy implementations
- `SchedulerThrift.java`: Thrift RPC interface for client communication

### Node Management  
- `NodeImpl.java`: Worker node lifecycle, resource monitoring, task execution
- `TaskScheduler.java`: Local task queuing (FIFO vs late-binding)
- `TaskLauncherService.java`: Task execution management

### Data Store
- `DataStoreThrift.java`: Centralized cluster state management
- Handles batched load updates from schedulers
- Distributes updated resource maps to all schedulers

### Utilities
- `ConfigUtil.java`: Configuration file parsing
- `TClients.java`, `TServers.java`: Thrift client/server abstractions
- `Resources.java`: Resource vector operations and calculations

## Experimental Setup

### Workload Types
1. **Azure VM Traces**: Real datacenter workload patterns
2. **Huawei Serverless**: Function-as-a-service execution traces  
3. **Function Bench**: ML training, image processing, cryptographic tasks

### Task Execution Modes
- **Simulated**: CPU/memory/disk stress using `stress-ng`
- **Docker**: Real containerized workloads with resource isolation
- **Configurable**: JSON-defined resource profiles and execution patterns

### Key Metrics
- Scheduling latency, task response time, resource utilization
- Message overhead, load balancing effectiveness
- Compared against Sparrow, Power-of-Two, random baselines

## Extension Points

1. **New Scheduling Strategies**: Implement `TaskPlacer` interface
2. **Custom Load Metrics**: Extend `LoadScore.java` with new scoring functions
3. **Additional Task Types**: Add to `TaskTypeID` enum and execution logic
4. **Resource Types**: Extend `TResourceVector` beyond CPU/memory/disk

## Python Simulation Framework

The `simulator/` directory contains a complete Python-based simulation framework that complements the physical Java implementation:

### Key Features
- **Discrete Event Simulation**: High-fidelity simulation with precise event ordering
- **Complete Scheduler Suite**: Dodoor, Prequal, Sparrow, Power-of-Two, Random implementations
- **Physical Compatibility**: Reads same configuration files and generates comparable metrics
- **Network Modeling**: Configurable network delays and communication overhead simulation

### Quick Start
```bash
# Physical experiment validation (matches debug.sh parameters exactly)
PYTHONPATH=. python simulator/run_physical_validation.py

# Individual scheduler testing
PYTHONPATH=. python simulator/run_simulation.py --config simulator/physical_experiment_config.json --schedulers dodoor

# Parallel experiment execution (recommended for speed)
PYTHONPATH=. python simulator/run_parallel_validation.py

# Generate performance plots after experiments
PYTHONPATH=. python deploy/python/analysis/plot_scheduler.py --log_dir simulation_output/physical_validation --output_dir deploy/plots/simulation
PYTHONPATH=. python deploy/python/analysis/plot_node.py --log_dir simulation_output/physical_validation --output_dir deploy/plots/simulation
```

### Scheduler Implementations
- **Dodoor**: (1+β)-choice with cached load scoring
- **Prequal**: Probe pool management with quantile-based selection
- **Sparrow**: Late-binding with distributed queuing
- **Power-of-Two**: Classic load balancing with sampling
- **Random**: Baseline for comparison

## Python Simulator - Implementation Status

### ✅ Completed Core Features
- **Slot-based node execution** matching Java `NodeImpl.java`
- **FIFO task queuing** with head-of-line blocking for resource constraints
- **Task acceptance behavior**: All tasks accepted and queued (no rejection)
- **Resource management**: Multi-dimensional load scoring (CPU/memory/disk)
- **Configuration alignment**: All defaults match Java `DodoorConf.java`
- **Timeout handling**: Supports infinite runtime with `timeout_ms: null`
- **Messaging metrics**: Reports cumulative network latency in milliseconds
- **Azure trace support**: Reads real Azure VM workload traces

### Scheduler Implementations (Matching Java)
```java
// From TaskPlacer.createTaskPlacer()
Dodoor:     CachedTaskPlacer(beta=1.0, PackingStrategy.SCORE)
Sparrow:    CachedTaskPlacer(beta=-2.0, PackingStrategy.NONE) + late-binding
PowerOfTwo: RunTimeProbeTaskPlacer(beta=1.0, PackingStrategy.RIF)
Prequal:    PrequalTaskPlacer(beta=1.0)
```

### Physical Experiment Parameters
```bash
# From deploy/script/end_to_end_exp/debug.sh
BATCH_SIZES="50"           # DataStore broadcasts every 50 tasks
BETA_VALS="1.0"            # (1+β)-choice parameter
CPU_WEIGHTS="1.0"          # CPU weight in load scoring
DURATION_WEIGHTS="0.5"     # Duration weight (α parameter)
```

## ⚠️ CRITICAL ISSUE: Sparrow Performance Inversion

**Status**: UNDER ACTIVE INVESTIGATION - REQUIRES FIX

### Problem Statement
Python simulator shows **opposite performance ranking** vs physical experiments:

**Physical Experiments** (azure_600, QPS=10):
1. **Sparrow**: 1166.8s mean makespan (BEST) ✅
2. **Dodoor**: 1173.2s mean makespan
3. **Prequal**: 1331.1s mean makespan
4. **PowerOfTwo**: 1399.3s mean makespan (WORST)

**Python Simulator** (INCORRECT):
1. **Dodoor**: 912.1s mean makespan (BEST)
2. **PowerOfTwo**: 918.3s mean makespan
3. **Prequal**: 1016.0s mean makespan
4. **Sparrow**: 7342.1s mean makespan (WORST) ❌ **6.3x SLOWER**

**Task Completion**: Sparrow completes only 3945/4000 tasks (98.6%), missing 55 tasks

### Bugs Fixed

1. **✅ Timeout Handling** (simulator/core/simulation_engine.py:134-174)
   - Fixed infinite runtime support with `timeout_ms: null`
   - Priority: `timeout_ms > target_completed_tasks > duration_ms`

2. **✅ Messaging Metrics** (simulator/core/metrics.py)
   - Changed from message count to cumulative latency (milliseconds)
   - Tracks both `network_messages` (latency) and `network_messages_count` (count)

3. **⚠️ Sparrow Confirmation Race** (PARTIALLY FIXED)
   - Removed immediate confirmation in `_handle_task_scheduled` to avoid double-confirm race
   - Improved from 3935/4000 (98.4%) to 3996/4000 (99.9%) completion
   - **Issue**: 4 tasks (0.1%) still stuck - requires further investigation

### Current Debugging - Sparrow Confirmation Logic

**Symptom**: 4 tasks stuck at 99.9% completion
**Root Cause**: Task confirmation bookkeeping issue

**Problem Pattern**:
```python
# Line ~419: Store preserved nodes when scheduling task
self._preserved_nodes[task.task_id] = preserved_nodes

# Later during confirmation event:
preserved_nodes = self._preserved_nodes.get(task_id)  # Returns None!
# Result: Confirmation rejected → task stuck in queue
```

**Debug Evidence**:
```
DEBUG CONFIRM: task=azure_0_loop0, node=m510_031, preserved=None, has_entry=False
DEBUG CONFIRM REJECT: task=azure_0_loop0, node=m510_031, reason=missing_reservation
```

**Next Steps**:
1. Investigate why `_preserved_nodes` cleared before all confirms processed
2. Fix race condition in confirmation bookkeeping
3. Achieve 100% task completion for Sparrow
4. Validate Sparrow performance matches physical experiments (~1167s mean makespan)

## Key Files Reference

**Core Simulation**:
- `simulator/core/simulation_engine.py` - Main discrete event loop, task lifecycle
- `simulator/core/node_executor.py` - Slot-based execution, FIFO queuing
- `simulator/core/metrics.py` - Performance metrics collection
- `simulator/core/events.py` - Event types and scheduling

**Schedulers**:
- `simulator/schedulers/unified_scheduler.py` - All 4 scheduler implementations

**Configuration**:
- `simulator/config/debug_config.json` - Debug/test configuration
- `simulator/config/simulation_config.py` - Configuration schema

**Workload**:
- `simulator/workload/azure_trace_reader.py` - Azure VM trace processing
