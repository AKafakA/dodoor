"""
Configuration classes and validation for Dodoor Python Simulator.

This module provides comprehensive configuration management for simulation parameters,
including validation, default values, and JSON serialization/deserialization.
"""

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class SchedulerType(Enum):
    """Supported scheduler types."""
    DODOOR = "dodoor"
    SPARROW = "sparrow"
    POWER_OF_TWO = "power_of_two"
    PREQUAL = "prequal" 
    RANDOM = "random"


class PackingStrategy(Enum):
    """Resource packing strategies."""
    SCORE = "score"  # Multi-dimensional load scoring
    RIF = "rif"      # Random in flight
    DURATION = "duration"  # Total pending duration
    NONE = "none"    # No packing optimization


class WorkloadType(Enum):
    """Workload generation types."""
    TRACE = "trace"       # Read from trace files
    SYNTHETIC = "synthetic"  # Generate synthetic workload


@dataclass
class ResourceWeights:
    """Resource weighting configuration for load scoring."""
    cpu: float = 1.0
    memory: float = 1.0  
    disk: float = 0.0
    duration: float = 0.5  # Weight for task duration in load scoring
    
    def __post_init__(self):
        """Validate resource weights."""
        if any(w < 0 for w in [self.cpu, self.memory, self.disk, self.duration]):
            raise ValueError("Resource weights must be non-negative")
        if not (0 <= self.duration <= 1):
            raise ValueError("Duration weight must be between 0 and 1")


@dataclass 
class SchedulerConfig:
    """Scheduler-specific configuration."""
    type: SchedulerType = SchedulerType.DODOOR
    beta: float = 1.0  # Power-of-two probability for Dodoor
    batch_size: int = 80  # Batch size for load updates
    packing_strategy: PackingStrategy = PackingStrategy.SCORE
    weights: ResourceWeights = field(default_factory=ResourceWeights)
    num_tasks_to_update: int = 4  # Number of tasks before updating load info
    late_binding_probe_count: int = 2  # Sparrow: number of nodes to probe/reserve per task
    
    # Prequal-specific parameters
    rif_quantile: float = 0.8  # Quantile for load cutoff in Prequal
    probe_pool_size: int = 10  # Size of probe pool
    delta: int = 1  # Delta parameter for probe reuse budget calculation
    probe_rate: int = 2  # Probe rate parameter
    probe_delete_rate: int = 1  # Probe deletion rate parameter
    probe_age_budget_ms: float = 10000.0  # Probe age budget in milliseconds
    
    # Simulation calibration overrides (per-scheduler)
    overhead_ms_override: float = None  # If set, overrides computed scheduling overhead (ms)
    messages_per_task_override: int = None  # If set, overrides modeled message count per task
    
    # Optional replay file for reservation targets (Sparrow), CSV or JSON
    replay_reservations_file: str = None
    
    def __post_init__(self):
        """Validate scheduler configuration."""
        if not (0 <= self.beta <= 1):
            raise ValueError("Beta must be between 0 and 1")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.num_tasks_to_update <= 0:
            raise ValueError("Number of tasks to update must be positive")
        if self.late_binding_probe_count <= 0:
            raise ValueError("late_binding_probe_count must be positive")
        if not (0 <= self.rif_quantile <= 1):
            raise ValueError("RIF quantile must be between 0 and 1")
        if self.probe_pool_size <= 0:
            raise ValueError("Probe pool size must be positive")
        if self.probe_age_budget_ms <= 0:
            raise ValueError("Probe age budget must be positive")


@dataclass
class NodeTypeConfig:
    """Configuration for a specific node type."""
    type: str  # Node type identifier (e.g., "m510", "xl170")
    count: int  # Number of nodes of this type
    cores: float  # CPU cores available
    memory: int  # Memory in MB
    disk: int = 0  # Disk space in MB  
    slots: int = 1  # Number of task slots per node
    late_binding_enabled: bool = False  # Enable late-binding queues
    
    def __post_init__(self):
        """Validate node type configuration."""
        if self.count <= 0:
            raise ValueError("Node count must be positive")
        if self.cores <= 0:
            raise ValueError("CPU cores must be positive")
        if self.memory <= 0:
            raise ValueError("Memory must be positive") 
        if self.slots <= 0:
            raise ValueError("Slots must be positive")


@dataclass
class NetworkConfig:
    """Network delay modeling configuration."""
    mean_latency_ms: float = 2.0  # Mean network latency in milliseconds
    std_latency_ms: float = 0.5   # Standard deviation of network latency
    scheduler_to_node_ms: float = 2.0    # Specific latencies for different paths
    node_to_datastore_ms: float = 2.0
    scheduler_to_datastore_ms: float = 2.0
    
    def __post_init__(self):
        """Validate network configuration."""
        if any(latency < 0 for latency in [self.mean_latency_ms, self.std_latency_ms,
                                          self.scheduler_to_node_ms, self.node_to_datastore_ms,
                                          self.scheduler_to_datastore_ms]):
            raise ValueError("Network latencies must be non-negative")


@dataclass
class ClusterConfig:
    """Cluster topology and resource configuration."""
    node_types: List[NodeTypeConfig] = field(default_factory=list)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    restrict_fifo: bool = True
    
    def __post_init__(self):
        """Validate cluster configuration."""
        if not self.node_types:
            raise ValueError("At least one node type must be specified")
        
        # Check for duplicate node type names
        type_names = [nt.type for nt in self.node_types]
        if len(type_names) != len(set(type_names)):
            raise ValueError("Node type names must be unique")
    
    @property
    def total_nodes(self) -> int:
        """Total number of nodes across all types."""
        return sum(nt.count for nt in self.node_types)
    
    @property 
    def total_slots(self) -> int:
        """Total task slots across all nodes."""
        return sum(nt.count * nt.slots for nt in self.node_types)


@dataclass
class SyntheticWorkloadConfig:
    """Configuration for synthetic workload generation."""
    arrival_rate: float  # Tasks per second
    arrival_pattern: str = "poisson"  # "poisson", "uniform", "burst"
    task_mix: Dict[str, float] = field(default_factory=dict)  # Task type probabilities
    
    def __post_init__(self):
        """Validate synthetic workload configuration."""
        if self.arrival_rate <= 0:
            raise ValueError("Arrival rate must be positive")
        
        if self.task_mix:
            total_prob = sum(self.task_mix.values())
            if abs(total_prob - 1.0) > 1e-6:
                raise ValueError("Task mix probabilities must sum to 1.0")


@dataclass
class TaskTypeConfig:
    """Configuration for a specific task type."""
    resource_requests: List[Dict[str, Any]]  # Resource requirement variants
    durations: List[int]  # Duration variants in milliseconds
    distribution: str = "uniform"  # "uniform", "normal", "exponential"
    
    def __post_init__(self):
        """Validate task type configuration."""
        if not self.resource_requests:
            raise ValueError("At least one resource request variant must be specified")
        if not self.durations:
            raise ValueError("At least one duration variant must be specified")


@dataclass
class WorkloadConfig:
    """Workload generation and trace configuration."""
    type: WorkloadType = WorkloadType.SYNTHETIC
    trace_file: Optional[str] = None  # Path to trace file
    synthetic: Optional[SyntheticWorkloadConfig] = None
    task_types: Dict[str, TaskTypeConfig] = field(default_factory=dict)
    task_profile_file: Optional[str] = None
    
    def __post_init__(self):
        """Validate workload configuration."""
        if self.type == WorkloadType.TRACE and not self.trace_file:
            raise ValueError("Trace file must be specified for trace workload type")
        if self.type == WorkloadType.SYNTHETIC and not self.synthetic:
            raise ValueError("Synthetic config must be specified for synthetic workload type")


@dataclass
class OutputConfig:
    """Output and logging configuration."""
    metrics_file: str = "simulation_metrics.json"
    log_level: str = "INFO"
    enable_detailed_metrics: bool = True
    metrics_interval_ms: int = 1000  # Metrics collection interval
    output_directory: str = "simulation_output"
    
    def __post_init__(self):
        """Validate output configuration."""
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level not in valid_log_levels:
            raise ValueError(f"Log level must be one of {valid_log_levels}")
        if self.metrics_interval_ms <= 0:
            raise ValueError("Metrics interval must be positive")


@dataclass
class ExperimentConfig:
    """Experiment execution configuration."""
    name: str = "simulation_experiment"
    duration_ms: int = 300000  # Simulation duration in milliseconds (5 minutes)
    warmup_duration_ms: int = 30000  # Warmup period in milliseconds (30 seconds)
    timeout_ms: Optional[int] = None  # Physical experiment timeout (30 min = 1800000ms)
    target_completed_tasks: Optional[int] = None  # Physical experiment target (30000 tasks)
    seed: Optional[int] = None  # Random seed for reproducibility
    replay_with_disk: bool = False  # Whether to include disk resources in scheduling decisions (matches Java --replay_with_disk)
    drain_on_finish: bool = False   # Run event loop until all scheduled events finish (drain queues)
    
    def __post_init__(self):
        """Validate experiment configuration."""
        if self.duration_ms <= 0:
            raise ValueError("Simulation duration must be positive")
        if self.warmup_duration_ms < 0:
            raise ValueError("Warmup duration must be non-negative")
        if self.warmup_duration_ms >= self.duration_ms:
            raise ValueError("Warmup duration must be less than total duration")


@dataclass
class SimulationConfig:
    """Complete simulation configuration."""
    experiment: ExperimentConfig
    scheduler: SchedulerConfig 
    cluster: ClusterConfig
    workload: WorkloadConfig
    output: OutputConfig
    
    @classmethod
    def from_json(cls, json_str: str) -> 'SimulationConfig':
        """Create configuration from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_file(cls, filename: str) -> 'SimulationConfig':
        """Create configuration from JSON file."""
        with open(filename, 'r') as f:
            return cls.from_json(f.read())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SimulationConfig':
        """Create configuration from dictionary."""
        # Parse experiment config
        experiment_data = data.get('experiment', {})
        experiment = ExperimentConfig(
            name=experiment_data.get('name', 'simulation_experiment'),
            duration_ms=experiment_data.get('duration_ms', experiment_data.get('duration', 300) * 1000),  # duration_ms already in ms, duration in seconds
            warmup_duration_ms=experiment_data.get('warmup_duration_ms', experiment_data.get('warmup_duration', 30) * 1000),
            timeout_ms=experiment_data.get('timeout_ms'),  # Physical experiment timeout
            target_completed_tasks=experiment_data.get('target_completed_tasks'),  # Physical experiment target
            seed=experiment_data.get('seed'),
            drain_on_finish=experiment_data.get('drain_on_finish', False)
        )
        
        # Parse scheduler config
        scheduler_data = data.get('scheduler', {})
        weights_data = scheduler_data.get('weights', {})
        weights = ResourceWeights(
            cpu=weights_data.get('cpu', 1.0),
            memory=weights_data.get('memory', 1.0),
            disk=weights_data.get('disk', 1.0),
            duration=weights_data.get('duration', 0.5)
        )
        
        scheduler = SchedulerConfig(
            type=SchedulerType(scheduler_data.get('type', 'dodoor')),
            beta=scheduler_data.get('beta', 0.6),
            batch_size=scheduler_data.get('batch_size', 1024),
            packing_strategy=PackingStrategy(scheduler_data.get('packing_strategy', 'score')),
            weights=weights,
            num_tasks_to_update=scheduler_data.get('num_tasks_to_update', 1),
            late_binding_probe_count=scheduler_data.get('late_binding_probe_count', 2)
        )
        
        # Parse cluster config
        cluster_data = data.get('cluster', {})
        node_types = []
        for nt_data in cluster_data.get('node_types', []):
            node_types.append(NodeTypeConfig(
                type=nt_data['type'],
                count=nt_data['count'],
                cores=nt_data.get('cores', nt_data.get('system.cores', 8)),
                memory=nt_data.get('memory', nt_data.get('system.memory', 65536)), 
                disk=nt_data.get('disk', nt_data.get('system.disks', 0)),
                slots=nt_data.get('slots', nt_data.get('node_monitor.num_slots', 1)),
                late_binding_enabled=nt_data.get('late_binding_enabled', False)
            ))
        
        network_data = cluster_data.get('network', {})
        network = NetworkConfig(
            mean_latency_ms=network_data.get('mean_latency_ms', 2.0),
            std_latency_ms=network_data.get('std_latency_ms', 0.5),
            scheduler_to_node_ms=network_data.get('scheduler_to_node_ms', 2.0),
            node_to_datastore_ms=network_data.get('node_to_datastore_ms', 1.0),
            scheduler_to_datastore_ms=network_data.get('scheduler_to_datastore_ms', 1.0)
        )
        
        cluster = ClusterConfig(
            node_types=node_types,
            network=network,
            restrict_fifo=cluster_data.get('restrict_fifo', True)
        )
        
        # Parse workload config
        workload_data = data.get('workload', {})
        type_value = workload_data.get('type', 'trace')
        workload_type = WorkloadType(type_value)
        
        synthetic = None
        if workload_type == WorkloadType.SYNTHETIC:
            synthetic_data = workload_data.get('synthetic', {})
            synthetic = SyntheticWorkloadConfig(
                arrival_rate=synthetic_data['arrival_rate'],
                arrival_pattern=synthetic_data.get('arrival_pattern', 'poisson'),
                task_mix=synthetic_data.get('task_mix', {})
            )
        
        task_types = {}
        for tt_name, tt_data in workload_data.get('task_types', {}).items():
            task_types[tt_name] = TaskTypeConfig(
                resource_requests=tt_data['resource_requests'],
                durations=tt_data['durations'],
                distribution=tt_data.get('distribution', 'uniform')
            )
        
        workload = WorkloadConfig(
            type=workload_type,
            trace_file=workload_data.get('trace_file'),
            synthetic=synthetic,
            task_types=task_types,
            task_profile_file=workload_data.get('task_profile_file')
        )
        
        # Parse output config
        output_data = data.get('output', {})
        output = OutputConfig(
            metrics_file=output_data.get('metrics_file', 'simulation_metrics.json'),
            log_level=output_data.get('log_level', 'INFO'),
            enable_detailed_metrics=output_data.get('enable_detailed_metrics', True),
            metrics_interval_ms=output_data.get('metrics_interval_ms', 1000),
            output_directory=output_data.get('output_directory', 'simulation_output')
        )
        
        return cls(
            experiment=experiment,
            scheduler=scheduler,
            cluster=cluster,
            workload=workload,
            output=output
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'experiment': {
                'name': self.experiment.name,
                'duration_ms': self.experiment.duration_ms,
                'warmup_duration_ms': self.experiment.warmup_duration_ms,
                'seed': self.experiment.seed,
                'timeout_ms': self.experiment.timeout_ms,
                'target_completed_tasks': self.experiment.target_completed_tasks,
                'drain_on_finish': self.experiment.drain_on_finish
            },
            'scheduler': {
                'type': self.scheduler.type.value,
                'beta': self.scheduler.beta,
                'batch_size': self.scheduler.batch_size,
                'packing_strategy': self.scheduler.packing_strategy.value,
                'weights': {
                    'cpu': self.scheduler.weights.cpu,
                    'memory': self.scheduler.weights.memory,
                    'disk': self.scheduler.weights.disk,
                    'duration': self.scheduler.weights.duration
                },
                'num_tasks_to_update': self.scheduler.num_tasks_to_update,
                'late_binding_probe_count': self.scheduler.late_binding_probe_count
            },
            'cluster': {
                'node_types': [
                    {
                        'type': nt.type,
                        'count': nt.count,
                        'cores': nt.cores,
                        'memory': nt.memory,
                        'disk': nt.disk,
                        'slots': nt.slots,
                        'late_binding_enabled': nt.late_binding_enabled
                    }
                    for nt in self.cluster.node_types
                ],
                'network': {
                    'mean_latency_ms': self.cluster.network.mean_latency_ms,
                    'std_latency_ms': self.cluster.network.std_latency_ms,
                    'scheduler_to_node_ms': self.cluster.network.scheduler_to_node_ms,
                    'node_to_datastore_ms': self.cluster.network.node_to_datastore_ms,
                    'scheduler_to_datastore_ms': self.cluster.network.scheduler_to_datastore_ms
                },
                'restrict_fifo': self.cluster.restrict_fifo
            },
            'workload': {
                'type': self.workload.type.value,
                'trace_file': self.workload.trace_file,
                'task_profile_file': self.workload.task_profile_file,
                'synthetic': {
                    'arrival_rate': self.workload.synthetic.arrival_rate,
                    'arrival_pattern': self.workload.synthetic.arrival_pattern,
                    'task_mix': self.workload.synthetic.task_mix
                } if self.workload.synthetic else None,
                'task_types': {
                    name: {
                        'resource_requests': tt.resource_requests,
                        'durations': tt.durations,
                        'distribution': tt.distribution
                    }
                    for name, tt in self.workload.task_types.items()
                }
            },
            'output': {
                'metrics_file': self.output.metrics_file,
                'log_level': self.output.log_level,
                'enable_detailed_metrics': self.output.enable_detailed_metrics,
                'metrics_interval_ms': self.output.metrics_interval_ms,
                'output_directory': self.output.output_directory
            }
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert configuration to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    def save_to_file(self, filename: str) -> None:
        """Save configuration to JSON file."""
        with open(filename, 'w') as f:
            f.write(self.to_json())
