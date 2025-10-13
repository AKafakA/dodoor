"""
Trace reader for physical experiment data formats.

This module reads task configuration files and host configurations
used in physical experiments and converts them to simulation format.
"""

import json
import logging
import random
from typing import Dict, List, Iterator, Tuple
from dataclasses import dataclass
from pathlib import Path

try:
    from ..schedulers.base_scheduler import Task, ResourceVector
except ImportError:
    from schedulers.base_scheduler import Task, ResourceVector

logger = logging.getLogger(__name__)


@dataclass
class TaskTypeProfile:
    """Profile information for a task type across different node types."""
    task_type_id: str
    node_profiles: Dict[str, 'NodeTaskProfile']  # node_type -> profile
    
    def get_profile_for_node(self, node_type: str) -> 'NodeTaskProfile':
        """Get task profile for specific node type."""
        if node_type in self.node_profiles:
            return self.node_profiles[node_type]
        # Fallback to first available profile
        return list(self.node_profiles.values())[0] if self.node_profiles else None


@dataclass
class NodeTaskProfile:
    """Task execution profile on a specific node type."""
    resource_vectors: List[ResourceVector]  # Different resource requirement variants
    durations_ms: List[int]  # Corresponding execution durations
    
    def sample_task_config(self, random_gen: random.Random) -> Tuple[ResourceVector, int]:
        """Sample a resource vector and duration for this task type."""
        if not self.resource_vectors or not self.durations_ms:
            return ResourceVector(), 1000
            
        # Select random variant
        idx = random_gen.randint(0, len(self.resource_vectors) - 1)
        return self.resource_vectors[idx], self.durations_ms[idx]


class TaskConfigReader:
    """
    Reader for task configuration files used in physical experiments.
    
    Parses merged_profiler_config.json and similar task profile files
    to extract task types and their resource requirements.
    """
    
    def __init__(self, config_file: str):
        self.config_file = Path(config_file)
        self.task_profiles: Dict[str, TaskTypeProfile] = {}
        self._load_task_profiles()
        
    def _load_task_profiles(self):
        """Load task profiles from configuration file."""
        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
                
            logger.info(f"Loading task profiles from {self.config_file}")
            
            # Parse tasks from config
            tasks = config_data.get('tasks', [])
            
            for task_config in tasks:
                task_type_id = task_config['taskTypeId']
                instance_info = task_config.get('instanceInfo', {})
                
                node_profiles = {}
                
                # Parse profiles for each node type
                for node_type, node_data in instance_info.items():
                    resource_vectors = []
                    durations_ms = []
                    
                    # Extract resource vectors
                    resource_data = node_data.get('resourceVector', {})
                    cores_list = resource_data.get('cores', [1])
                    memory_list = resource_data.get('memory', [1024])
                    disks_list = resource_data.get('disks', [0])
                    
                    # Extract durations
                    duration_list = node_data.get('estimatedDuration', [1000])
                    
                    # Create resource vectors for all combinations
                    max_variants = max(len(cores_list), len(memory_list), 
                                     len(disks_list), len(duration_list))
                    
                    for i in range(max_variants):
                        cores = cores_list[min(i, len(cores_list) - 1)]
                        memory = memory_list[min(i, len(memory_list) - 1)]
                        disks = disks_list[min(i, len(disks_list) - 1)]
                        duration = duration_list[min(i, len(duration_list) - 1)]
                        
                        resource_vectors.append(ResourceVector(
                            cores=float(cores),
                            memory=int(memory),
                            disk=int(disks)
                        ))
                        durations_ms.append(int(duration))
                    
                    node_profiles[node_type] = NodeTaskProfile(
                        resource_vectors=resource_vectors,
                        durations_ms=durations_ms
                    )
                
                # Store task profile
                self.task_profiles[task_type_id] = TaskTypeProfile(
                    task_type_id=task_type_id,
                    node_profiles=node_profiles
                )
                
            logger.info(f"Loaded {len(self.task_profiles)} task types: "
                       f"{list(self.task_profiles.keys())}")
                       
        except Exception as e:
            logger.error(f"Failed to load task profiles: {e}")
            raise
    
    def get_task_types(self) -> List[str]:
        """Get list of available task types."""
        return list(self.task_profiles.keys())
    
    def get_task_profile(self, task_type: str) -> TaskTypeProfile:
        """Get profile for specific task type."""
        return self.task_profiles.get(task_type)
    
    def sample_task(self, task_type: str, node_type: str, task_id: str,
                   submission_time: float, random_gen: random.Random) -> Task:
        """Sample a task instance of given type for specific node type."""
        profile = self.get_task_profile(task_type)
        if not profile:
            # Default task if profile not found
            return Task(
                task_id=task_id,
                task_type=task_type,
                resource_request=ResourceVector(cores=1, memory=1024, disk=0),
                duration_ms=1000,
                submission_time=submission_time
            )
        
        node_profile = profile.get_profile_for_node(node_type)
        if not node_profile:
            # Default task if node profile not found  
            return Task(
                task_id=task_id,
                task_type=task_type,
                resource_request=ResourceVector(cores=1, memory=1024, disk=0),
                duration_ms=1000,
                submission_time=submission_time
            )
        
        # Sample resource requirements and duration
        resource_vector, duration = node_profile.sample_task_config(random_gen)
        
        return Task(
            task_id=task_id,
            task_type=task_type,
            resource_request=resource_vector,
            duration_ms=duration,
            submission_time=submission_time
        )


class HostConfigReader:
    """
    Reader for host configuration files used in physical experiments.
    
    Parses host_config.json files to extract cluster topology and
    node specifications.
    """
    
    def __init__(self, config_file: str):
        self.config_file = Path(config_file)
        self.node_types: Dict[str, dict] = {}
        self._load_host_config()
        
    def _load_host_config(self):
        """Load host configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
                
            logger.info(f"Loading host config from {self.config_file}")
            
            # Parse node types from config
            nodes_config = config_data.get('nodes', {})
            node_types = nodes_config.get('node.types', [])
            
            for node_config in node_types:
                node_type = node_config.get('node.type')
                if node_type:
                    self.node_types[node_type] = {
                        'cores': node_config.get('system.cores', 8),
                        'memory': node_config.get('system.memory', 65536),
                        'disk': node_config.get('system.disks', 0),
                        'slots': node_config.get('node_monitor.num_slots', 4),
                        'host_count': len(node_config.get('hosts', []))
                    }
                    
            logger.info(f"Loaded {len(self.node_types)} node types: "
                       f"{list(self.node_types.keys())}")
                       
        except Exception as e:
            logger.error(f"Failed to load host config: {e}")
            raise
    
    def get_node_types(self) -> Dict[str, dict]:
        """Get all node type configurations."""
        return self.node_types.copy()
    
    def get_total_nodes(self) -> int:
        """Get total number of nodes across all types."""
        return sum(config['host_count'] for config in self.node_types.values())


class WorkloadGenerator:
    """
    Generates synthetic workloads based on task profiles.
    
    Can generate Poisson arrival processes with configurable
    task type distributions and QPS rates.
    """
    
    def __init__(self, task_reader: TaskConfigReader, arrival_rate: float,
                 task_mix: Dict[str, float], seed: int = None):
        self.task_reader = task_reader
        self.arrival_rate = arrival_rate  # Tasks per second
        self.task_mix = task_mix  # Task type -> probability
        self._random = random.Random(seed)
        self._task_counter = 0
        
        # Validate task mix
        if abs(sum(task_mix.values()) - 1.0) > 1e-6:
            raise ValueError("Task mix probabilities must sum to 1.0")
            
        # Build cumulative distribution for task type selection
        self._task_type_cdf = []
        cumulative = 0.0
        for task_type, prob in task_mix.items():
            cumulative += prob
            self._task_type_cdf.append((cumulative, task_type))
            
        logger.info(f"Initialized workload generator: {arrival_rate} tasks/sec, "
                   f"mix={task_mix}")
    
    def generate_workload(self, duration_ms: float, start_time: float = 0.0) -> List[Task]:
        """
        Generate a complete workload for the specified duration.
        
        Args:
            duration_ms: Duration to generate workload for
            start_time: Start time for workload generation
            
        Returns:
            List of tasks with submission times
        """
        tasks = []
        current_time = start_time
        end_time = start_time + duration_ms
        
        # Generate Poisson arrival process
        while current_time < end_time:
            # Sample inter-arrival time (exponential distribution)
            inter_arrival = self._random.expovariate(self.arrival_rate / 1000.0)  # Convert to ms
            current_time += inter_arrival
            
            if current_time >= end_time:
                break
                
            # Select task type based on mix
            task_type = self._select_task_type()
            
            # Generate task ID
            self._task_counter += 1
            task_id = f"task_{self._task_counter:06d}"
            
            # Create task (will be customized for specific node when scheduled)
            task = Task(
                task_id=task_id,
                task_type=task_type,
                resource_request=ResourceVector(cores=1, memory=1024, disk=0),  # Placeholder
                duration_ms=1000,  # Placeholder
                submission_time=current_time
            )
            
            tasks.append(task)
        
        logger.info(f"Generated {len(tasks)} tasks over {duration_ms/1000:.1f}s "
                   f"(avg rate: {len(tasks)*1000/duration_ms:.2f} tasks/sec)")
        
        return tasks
    
    def _select_task_type(self) -> str:
        """Select task type based on configured mix."""
        r = self._random.random()
        for cumulative_prob, task_type in self._task_type_cdf:
            if r <= cumulative_prob:
                return task_type
        # Fallback to last task type
        return self._task_type_cdf[-1][1]
    
    def customize_task_for_node(self, task: Task, node_type: str) -> Task:
        """Customize task resource requirements for specific node type."""
        return self.task_reader.sample_task(
            task.task_type, node_type, task.task_id,
            task.submission_time, self._random
        )