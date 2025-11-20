# Dodoor: Decentralized Scheduling with Cached Load and Heterogeneous Tasks

Dodoor is a research prototype for decentralized task scheduling on heterogeneous clusters. It makes placement decisions using cached server load snapshots that are refreshed in batches, avoiding per-decision runtime probing. For heterogeneous tasks, Dodoor uses a load score that captures resource alignment (CPU, memory, disk) and anticipated wait time, following the approach described in the accompanying paper at [link](https://arxiv.org/abs/2510.12889)

## Architecture

The system is composed of the following main components:

*   **Scheduler**: The central component responsible for making scheduling decisions. It receives job submissions from clients and assigns tasks to worker nodes based on the selected scheduling policy.
*   **Data Store**: A service that maintains the state of the cluster, including information about the available nodes and their resources.
*   **Worker Nodes**: The nodes that execute the tasks. Each worker node runs a `NodeMonitorService` to report its status and a `NodeEnqueueService` to receive tasks from the scheduler.
*   **Client**: A client that submits jobs to the scheduler.

The communication between these components is done using [Apache Thrift](https://thrift.apache.org/), a software framework for scalable cross-language services development.

### Scheduling Policies

The following policies are implemented and used in experiments (see paper-draft.tex):

*   **Random**: Places tasks uniformly at random without probing.
*   **Power of Two (PoT)**: Probes two random workers and selects the one with the lower queue length.
*   **Prequal (NSDI’24)**: Maintains a pool of recent probe results to estimate queue distributions and place tasks under RIF and latency constraints.
*   **Dodoor (This work)**: Makes decisions from cached load snapshots pushed by a data store; uses a heterogeneous load score with tunable weights and batch updates.

Note: The late-binding (Sparrow) variant is experimental and currently excluded from this README pending duplication execution issue resolution.

## Getting Started

### Prerequisites

*   [Java 17](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)
*   [Apache Maven](https://maven.apache.org/)
*   [Apache Thrift](https://thrift.apache.org/download)

### Building the Project

To build the project, run the following command from the root directory:

```bash
./rebuild.sh
```

This will generate the Thrift code, compile the Java source code, and create a JAR file in the `target` directory.

### Running the System

Dodoor runs three services: Scheduler, Data Store, and Node. Use `ServiceDaemon` to launch any combination on a host. You need:

- `static` config: e.g., `deploy/resources/configuration/example_dodoor_configuration.conf`
- `host` config JSON: generated per testbed (see `deploy/instruction/configuration-generation.md`)
- `task` config JSON: profiled tasks (see the same instruction doc)

Examples (adjust paths/ports for your environment):

- Start Node on a worker
  ```bash
  java -cp target/dodoor-1.0-SNAPSHOT.jar \
    edu.cam.dodoor.ServiceDaemon \
    -c deploy/resources/configuration/example_dodoor_configuration.conf \
    -hc deploy/resources/host_addresses/cloud_lab/host_config.json \
    -tc deploy/resources/configuration/generated_config/merged_profiler_config.json \
    -d false -s false -n true
  ```

- Start Scheduler and Data Store on the control node
  ```bash
  java -cp target/dodoor-1.0-SNAPSHOT.jar \
    edu.cam.dodoor.ServiceDaemon \
    -c deploy/resources/configuration/example_dodoor_configuration.conf \
    -hc deploy/resources/host_addresses/cloud_lab/host_config.json \
    -tc deploy/resources/configuration/generated_config/merged_profiler_config.json \
    -d true -s true -n false
  ```

### Running Experiments

- Config generation and profiling: see `deploy/instruction/configuration-generation.md`.
- CloudLab multi-host orchestration and collection: `deploy/script/single_exp.sh` and `deploy/script/test_cloudlab.sh`.
- Python evaluation tools and workloads live under `deploy/python` (analysis, data generation, and function benchmarks).

## User Guide

### Submitting a Job

Submit via the `SchedulerService` Thrift API. A job is a list of `TTaskSpec` items with resource requirements, duration, and type. See `src/main/java/edu/cam/dodoor/client/DodoorClient.java` and `src/main/java/edu/cam/dodoor/client/TaskTracePlayer.java` for examples (trace replay, Poisson/QPS replay).

### Monitoring the System

`DataStoreService` exposes current node states and can be polled by schedulers and tools. Metrics logs are emitted per service and can be aggregated for analysis.

### Extending the System

- Add a policy: implement `edu.cam.dodoor.scheduler.Scheduler` and wire in `SchedulerImpl`/`SchedulerThrift`.
- Add workloads: define tasks and resource shapes in the task config; server backends support Linux `stress-ng` and Docker-executed Python functions.

### Bibetex
If you feel this useful, please consider to cite our paper 
```
@misc{da2025dodoorefficientrandomizeddecentralized,
      title={Dodoor: Efficient Randomized Decentralized Scheduling with Load Caching for Heterogeneous Tasks and Clusters}, 
      author={Wei Da and Evangelia Kalyvianaki},
      year={2025},
      eprint={2510.12889},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2510.12889}, 
}
```

