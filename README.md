# Dodoor: A Research Prototype for Heterogeneous Task Scheduling

Dodoor is a research prototype for investigating scheduling algorithms for heterogeneous tasks on heterogeneous clusters. It is designed to be a flexible and extensible platform for experimenting with different scheduling policies.

## Architecture

The system is composed of the following main components:

*   **Scheduler**: The central component responsible for making scheduling decisions. It receives job submissions from clients and assigns tasks to worker nodes based on the selected scheduling policy.
*   **Data Store**: A service that maintains the state of the cluster, including information about the available nodes and their resources.
*   **Worker Nodes**: The nodes that execute the tasks. Each worker node runs a `NodeMonitorService` to report its status and a `NodeEnqueueService` to receive tasks from the scheduler.
*   **Client**: A client that submits jobs to the scheduler.

The communication between these components is done using [Apache Thrift](https://thrift.apache.org/), a software framework for scalable cross-language services development.

### Scheduling Policies

Dodoor supports the following scheduling policies:

*   **Power of Two**: A simple randomized scheduling policy where the scheduler probes two randomly selected nodes and sends the task to the one with the shorter queue.
*   **Cached Power of Two**: A variation of the power of two policy where the scheduler uses a cached view of the node states to make scheduling decisions. This avoids the overhead of probing the nodes in real-time.
*   **Late Binding (Sparrow)**: A scheduling policy where the scheduler sends the task to a randomly selected node, and the node itself decides whether to accept the task or forward it to another node.

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

The system can be run in a distributed environment with a scheduler, a data store, and multiple worker nodes. The configuration for these components can be found in the `deploy/resources/configuration` directory.

#### 1. Start the Data Store

```bash
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.datastore.DataStore --config deploy/resources/configuration/example_dodoor_configuration.conf
```

#### 2. Start the Scheduler

```bash
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.scheduler.Scheduler --config deploy/resources/configuration/example_dodoor_configuration.conf
```

#### 3. Start the Worker Nodes

```bash
java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.node.NodeMonitor --config deploy/resources/configuration/example_dodoor_configuration.conf
```

### Running Experiments

The `deploy/python` directory contains scripts for running experiments and analyzing the results. The `deploy/python/data/generator` directory contains scripts for generating synthetic workloads. The `deploy/python/function_bench` directory contains a set of real-world computation tasks that can be used for evaluation.

The `single_exp.sh` script in the `deploy/script` directory can be used to run a single experiment.

```bash
./deploy/script/single_exp.sh
```

## User Guide

### Submitting a Job

A job can be submitted to the scheduler using the `SchedulerService` Thrift API. A job is a collection of tasks, where each task is defined by its resource requirements, duration, and type.

### Monitoring the System

The `DataStoreService` can be used to monitor the state of the cluster. It provides methods for retrieving the list of nodes and their current resource utilization.

### Extending the System

Dodoor is designed to be extensible. New scheduling policies can be added by implementing the `Scheduler` interface. New task types can be added by extending the `Task` class.

## Paper Draft

A draft of a paper describing the system and some preliminary experimental results can be found in the `paper_draft_latex` directory. The paper was rejected from ATC'23, but it provides a good overview of the project.