"""
Base scheduler interface and common functionality.

This module defines the abstract base class for all scheduler implementations
and provides common utilities for task placement decisions.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import random
import logging

try:
    from ..config.simulation_config import SchedulerConfig, PackingStrategy, ResourceWeights
except ImportError:
    from config.simulation_config import SchedulerConfig, PackingStrategy, ResourceWeights

logger = logging.getLogger(__name__)


@dataclass
class ResourceVector:
    """Resource requirement/capacity vector."""
    cores: float = 0.0
    memory: int = 0  # MB
    disk: int = 0    # MB
    
    def __add__(self, other: 'ResourceVector') -> 'ResourceVector':
        return ResourceVector(
            cores=self.cores + other.cores,
            memory=self.memory + other.memory,
            disk=self.disk + other.disk
        )
    
    def __sub__(self, other: 'ResourceVector') -> 'ResourceVector':
        return ResourceVector(
            cores=self.cores - other.cores,
            memory=self.memory - other.memory,
            disk=self.disk - other.disk
        )
    
    def __mul__(self, scalar: float) -> 'ResourceVector':
        return ResourceVector(
            cores=self.cores * scalar,
            memory=int(self.memory * scalar),
            disk=int(self.disk * scalar)
        )
    
    def can_fit(self, other: 'ResourceVector') -> bool:
        """Check if this resource vector can accommodate another."""
        return (self.cores >= other.cores and 
                self.memory >= other.memory and
                self.disk >= other.disk)
    
    def utilization(self, capacity: 'ResourceVector') -> float:
        """Calculate utilization as fraction of capacity."""
        if capacity.cores == 0 and capacity.memory == 0:
            return 0.0
        
        cpu_util = self.cores / max(capacity.cores, 1e-6)
        mem_util = self.memory / max(capacity.memory, 1e-6)
        
        return (cpu_util + mem_util) / 2.0


@dataclass
class Task:
    """Task representation for scheduling."""
    task_id: str
    task_type: str
    resource_request: ResourceVector
    duration_ms: int
    submission_time: float
    priority: int = 0
    
    def __hash__(self):
        return hash(self.task_id)
    
    def __eq__(self, other):
        return isinstance(other, Task) and self.task_id == other.task_id


@dataclass
class NodeState:
    """Current state of a worker node."""
    node_id: str
    node_type: str
    capacity: ResourceVector
    allocated: ResourceVector
    num_tasks: int = 0
    total_duration_ms: float = 0.0
    queue_length: int = 0
    last_update_time: float = 0.0
    
    @property
    def available(self) -> ResourceVector:
        """Calculate available resources."""
        return self.capacity - self.allocated
    
    @property
    def utilization(self) -> float:
        """Calculate current resource utilization."""
        return self.allocated.utilization(self.capacity)
    
    def can_accept_task(self, task: Task) -> bool:
        """Check if node can accept a task."""
        return self.available.can_fit(task.resource_request)
    
    def allocate_task(self, task: Task) -> bool:
        """
        Allocate resources for a task.
        
        CRITICAL: Tasks are always allocated - no rejection at this level.
        Physical Java system always accepts tasks into FIFO queues.
        """
        self.allocated += task.resource_request
        self.num_tasks += 1
        self.total_duration_ms += task.duration_ms
        self.queue_length += 1
        
        return True
    
    def release_task(self, task: Task):
        """Release resources from a completed task."""
        # Prevent negative resource allocations
        self.allocated.cores = max(0.0, self.allocated.cores - task.resource_request.cores)
        self.allocated.memory = max(0, self.allocated.memory - task.resource_request.memory)
        self.allocated.disk = max(0, self.allocated.disk - task.resource_request.disk)
        
        self.num_tasks = max(0, self.num_tasks - 1)
        self.total_duration_ms = max(0, self.total_duration_ms - task.duration_ms)
        self.queue_length = max(0, self.queue_length - 1)


@dataclass
class SchedulingDecision:
    """Result of a scheduling decision."""
    task: Task
    assigned_node: str
    placement_score: float = 0.0
    scheduling_latency_ms: float = 0.0


class LoadScoreCalculator:
    """
    Load scoring implementation matching the Java LoadScore class.
    
    Implements multi-dimensional load scoring combining resource utilization
    and pending task duration with configurable weights.
    """
    
    def __init__(self, weights: ResourceWeights):
        self.weights = weights
    
    def calculate_resource_load_score(self, node_allocated: ResourceVector, 
                                    task_resources: ResourceVector,
                                    node_capacity: ResourceVector) -> float:
        """
        Calculate resource load score for a node-task pair.
        
        This implementation matches the getResourceLoadScores method from 
        LoadScore.java in the original system.
        """
        if (node_capacity.cores == 0 or node_capacity.memory == 0):
            return 0.0
        
        # Calculate per-resource load contributions
        # CPU load: current utilization * task demand relative to capacity
        cpu_utilization = node_allocated.cores / node_capacity.cores
        cpu_task_demand = task_resources.cores / node_capacity.cores
        cpu_load = self.weights.cpu * cpu_utilization * cpu_task_demand
        
        # Memory load: current utilization * task demand relative to capacity  
        mem_utilization = node_allocated.memory / node_capacity.memory
        mem_task_demand = task_resources.memory / node_capacity.memory
        mem_load = self.weights.memory * mem_utilization * mem_task_demand
        
        # Disk load: current utilization * task demand relative to capacity
        disk_load = 0.0
        if node_capacity.disk > 0:
            disk_utilization = node_allocated.disk / node_capacity.disk
            disk_task_demand = task_resources.disk / node_capacity.disk
            disk_load = self.weights.disk * disk_utilization * disk_task_demand
        
        # Normalize by total weights
        total_weight = self.weights.cpu + self.weights.memory + self.weights.disk
        normalized_resource_load = (cpu_load + mem_load + disk_load) / total_weight
        
        logger.debug(f"Resource load calculation: cpu={cpu_load:.4f}, mem={mem_load:.4f}, "
                    f"disk={disk_load:.4f}, normalized={normalized_resource_load:.4f}")
        
        return normalized_resource_load
    
    def calculate_combined_load_score(self, node1: NodeState, node2: NodeState,
                                    task: Task) -> Tuple[float, float]:
        """
        Calculate combined load scores for two nodes competing for a task.
        
        This matches the getLoadScoresPairs method from LoadScore.java.
        """
        # Calculate resource load scores for both nodes
        resource_load1 = self.calculate_resource_load_score(
            node1.allocated, task.resource_request, node1.capacity
        )
        resource_load2 = self.calculate_resource_load_score(
            node2.allocated, task.resource_request, node2.capacity
        )
        
        # Normalize resource loads relative to each other
        total_resource_load = resource_load1 + resource_load2
        if total_resource_load > 0:
            norm_resource_load1 = resource_load1 / total_resource_load
            norm_resource_load2 = resource_load2 / total_resource_load
        else:
            norm_resource_load1 = norm_resource_load2 = 0.5
        
        # Calculate duration loads
        total_duration = node1.total_duration_ms + node2.total_duration_ms
        if total_duration > 0:
            norm_duration_load1 = node1.total_duration_ms / total_duration
            norm_duration_load2 = node2.total_duration_ms / total_duration
        else:
            norm_duration_load1 = norm_duration_load2 = 0.5
        
        # Combine resource and duration loads
        alpha = 1.0 - self.weights.duration
        load_score1 = (norm_resource_load1 * alpha + 
                      norm_duration_load1 * self.weights.duration)
        load_score2 = (norm_resource_load2 * alpha + 
                      norm_duration_load2 * self.weights.duration)
        
        logger.debug(f"Combined load scores: node1={load_score1:.4f}, node2={load_score2:.4f}")
        
        return load_score1, load_score2


class BaseScheduler(ABC):
    """
    Abstract base class for all scheduler implementations.
    
    Provides common functionality and defines the interface that all
    schedulers must implement.
    """
    
    def __init__(self, config: SchedulerConfig, scheduler_id: str = "scheduler"):
        self.config = config
        self.scheduler_id = scheduler_id
        self.load_calculator = LoadScoreCalculator(config.weights)
        self._random = random.Random()
        
        # Scheduling statistics
        self.total_tasks_scheduled = 0
        self.total_scheduling_time_ms = 0.0
        
        logger.info(f"Initialized {self.__class__.__name__} with config: "
                   f"beta={config.beta}, batch_size={config.batch_size}, "
                   f"strategy={config.packing_strategy}")
    
    @abstractmethod
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[SchedulingDecision]:
        """
        Schedule a batch of tasks to available nodes.
        
        Args:
            tasks: List of tasks to schedule
            node_states: Current state of all nodes  
            current_time: Current simulation time
            
        Returns:
            List of scheduling decisions
        """
        pass
    
    def set_random_seed(self, seed: int):
        """Set random seed for reproducible scheduling decisions."""
        self._random.seed(seed)
        logger.info(f"Set random seed to {seed}")
    
    def get_node_score(self, node: NodeState, task: Task, strategy: PackingStrategy) -> float:
        """
        Calculate node score based on packing strategy.
        
        Lower scores indicate better nodes (less loaded).
        """
        if strategy == PackingStrategy.SCORE:
            # Use multi-dimensional load scoring
            return self.load_calculator.calculate_resource_load_score(
                node.allocated, task.resource_request, node.capacity
            )
        elif strategy == PackingStrategy.RIF:
            # Random in flight - use number of tasks
            return float(node.num_tasks)
        elif strategy == PackingStrategy.DURATION:
            # Use total pending duration
            return node.total_duration_ms
        elif strategy == PackingStrategy.NONE:
            # No load balancing
            return 0.0
        else:
            raise ValueError(f"Unknown packing strategy: {strategy}")
    
    def select_best_nodes(self, tasks: List[Task], node_states: Dict[str, NodeState],
                         num_candidates: int = 2) -> Dict[Task, List[str]]:
        """
        Select best candidate nodes for each task based on load scores.
        
        CRITICAL: Tasks are never rejected for resource reasons - nodes handle queuing.
        This matches the Java physical system behavior where tasks are always queued.
        
        Returns:
            Dictionary mapping each task to list of candidate node IDs
        """
        candidates = {}
        available_nodes = list(node_states.keys())
        
        if len(available_nodes) == 0:
            logger.warning("No available nodes for task scheduling")
            return {task: [] for task in tasks}
        
        for task in tasks:
            # CRITICAL: All nodes are viable - no resource checking at scheduler level
            # Physical Java system always accepts tasks into FIFO queues
            viable_nodes = available_nodes
            
            # Score all nodes for load balancing
            node_scores = []
            for node_id in viable_nodes:
                score = self.get_node_score(
                    node_states[node_id], task, self.config.packing_strategy
                )
                node_scores.append((score, node_id))
            
            # Sort by score (ascending - lower is better)
            node_scores.sort(key=lambda x: x[0])
            
            # Select top candidates
            selected = [node_id for _, node_id in node_scores[:num_candidates]]
            candidates[task] = selected
            
        return candidates
    
    def update_statistics(self, num_tasks: int, scheduling_time_ms: float):
        """Update scheduling statistics."""
        self.total_tasks_scheduled += num_tasks
        self.total_scheduling_time_ms += scheduling_time_ms
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        avg_scheduling_time = (self.total_scheduling_time_ms / 
                             max(1, self.total_tasks_scheduled))
        
        return {
            'scheduler_type': self.__class__.__name__,
            'total_tasks_scheduled': self.total_tasks_scheduled,
            'total_scheduling_time_ms': self.total_scheduling_time_ms,
            'average_scheduling_time_ms': avg_scheduling_time,
            'config': {
                'beta': self.config.beta,
                'batch_size': self.config.batch_size,
                'packing_strategy': self.config.packing_strategy.value,
                'weights': {
                    'cpu': self.config.weights.cpu,
                    'memory': self.config.weights.memory,
                    'disk': self.config.weights.disk,
                    'duration': self.config.weights.duration
                }
            }
        }