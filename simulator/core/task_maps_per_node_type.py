"""
TaskMapsPerNodeType implementation matching Java node task mapping.

Direct port from Java TaskMapsPerNodeType.java - critical missing component
that maps task types to actual resource requirements per node type.
"""

import logging
from typing import Dict, List, Optional

try:
    from ..schedulers.base_scheduler import ResourceVector
except ImportError:
    from schedulers.base_scheduler import ResourceVector

logger = logging.getLogger(__name__)


class TaskMapsPerNodeType:
    """
    Maps task types to resource requirements and durations for specific node types.
    
    This is a critical component missing from the Python simulator that exists
    in the Java physical system. It handles the mapping between:
    - Task type ID (e.g., "web_server", "ml_training") 
    - Node type (e.g., "small", "large", "gpu")
    - Actual resource requirements (cores, memory, disk)
    - Estimated task duration
    
    Matches Java edu.cam.dodoor.node.TaskMapsPerNodeType.java
    """
    
    def __init__(self, node_type: str):
        """
        Initialize task maps for a specific node type.
        
        Args:
            node_type: Node type identifier (e.g., "small", "large", "gpu")
        """
        self.node_type = node_type
        
        # Task type -> ResourceVector mapping
        self._task_resource_map: Dict[str, ResourceVector] = {}
        
        # Task type -> duration mapping (in milliseconds)
        self._task_duration_map: Dict[str, int] = {}
        
        # Task type -> multiple resource variants (for distributions)
        self._task_cpu_requirements: Dict[str, List[float]] = {}
        self._task_memory_requirements: Dict[str, List[int]] = {}
        self._task_duration_estimates: Dict[str, List[int]] = {}
        
        logger.debug(f"Initialized TaskMapsPerNodeType for node type: {node_type}")
    
    def add_task_mapping(self, task_type: str, resource_vector: ResourceVector, 
                        duration_ms: int):
        """
        Add a task type mapping.
        
        Args:
            task_type: Task type identifier
            resource_vector: Resource requirements for this task on this node type
            duration_ms: Estimated duration in milliseconds
        """
        self._task_resource_map[task_type] = resource_vector
        self._task_duration_map[task_type] = duration_ms
        
        logger.debug(f"Added task mapping: {task_type} -> {resource_vector}, {duration_ms}ms")
    
    def add_task_requirements_distribution(self, task_type: str, 
                                         cpu_requirements: List[float],
                                         memory_requirements: List[int],
                                         duration_estimates: List[int]):
        """
        Add task requirements with distribution (matches Java's list-based approach).
        
        Args:
            task_type: Task type identifier
            cpu_requirements: List of possible CPU requirements
            memory_requirements: List of possible memory requirements  
            duration_estimates: List of possible duration estimates
        """
        self._task_cpu_requirements[task_type] = cpu_requirements
        self._task_memory_requirements[task_type] = memory_requirements
        self._task_duration_estimates[task_type] = duration_estimates
        
        # Set default mapping to first value in lists
        if cpu_requirements and memory_requirements:
            default_resource = ResourceVector(
                cores=cpu_requirements[0],
                memory=memory_requirements[0],
                disk=0  # Default disk requirement
            )
            default_duration = duration_estimates[0] if duration_estimates else 1000
            
            self.add_task_mapping(task_type, default_resource, default_duration)
    
    def get_resource_vector(self, task_type: str) -> ResourceVector:
        """
        Get resource requirements for a task type on this node type.
        
        Args:
            task_type: Task type identifier
            
        Returns:
            ResourceVector with resource requirements
            
        Raises:
            KeyError: If task type not found
        """
        if task_type in self._task_resource_map:
            return self._task_resource_map[task_type]

        # Fallback for unknown task types (Azure trace compatibility)
        if 'default_task' in self._task_resource_map:
            logger.debug(f"Task type '{task_type}' not found in node type '{self.node_type}', "
                        f"using 'default_task' fallback")
            return self._task_resource_map['default_task']

        # If no default_task either, raise error
        raise KeyError(f"Task type '{task_type}' not found in node type '{self.node_type}' "
                      f"and no 'default_task' fallback available")
    
    def get_task_duration(self, task_type: str) -> int:
        """
        Get estimated duration for a task type on this node type.
        
        Args:
            task_type: Task type identifier
            
        Returns:
            Duration in milliseconds
            
        Raises:
            KeyError: If task type not found
        """
        if task_type in self._task_duration_map:
            return self._task_duration_map[task_type]

        # Fallback for unknown task types (Azure trace compatibility)
        if 'default_task' in self._task_duration_map:
            logger.debug(f"Task type '{task_type}' not found in node type '{self.node_type}', "
                        f"using 'default_task' fallback for duration")
            return self._task_duration_map['default_task']

        # If no default_task either, raise error
        raise KeyError(f"Task type '{task_type}' not found in node type '{self.node_type}' "
                      f"and no 'default_task' fallback available")
    
    def has_task_type(self, task_type: str) -> bool:
        """Check if task type is supported on this node type."""
        return task_type in self._task_resource_map
    
    def get_supported_task_types(self) -> List[str]:
        """Get list of all supported task types for this node type."""
        return list(self._task_resource_map.keys())
    
    def get_cpu_requirements_distribution(self, task_type: str) -> List[float]:
        """Get CPU requirements distribution for task type."""
        return self._task_cpu_requirements.get(task_type, [])
    
    def get_memory_requirements_distribution(self, task_type: str) -> List[int]:
        """Get memory requirements distribution for task type."""
        return self._task_memory_requirements.get(task_type, [])
    
    def get_duration_estimates_distribution(self, task_type: str) -> List[int]:
        """Get duration estimates distribution for task type."""
        return self._task_duration_estimates.get(task_type, [])
    
    @classmethod
    def create_from_config(cls, node_type: str, task_type_config: dict) -> 'TaskMapsPerNodeType':
        """
        Create TaskMapsPerNodeType from configuration matching Java format.
        
        Args:
            node_type: Node type identifier
            task_type_config: Configuration dict matching Java JSON format
            
        Returns:
            Configured TaskMapsPerNodeType instance
        """
        task_maps = cls(node_type)
        
        if "tasks" in task_type_config:
            for task_config in task_type_config["tasks"]:
                task_type_id = task_config["taskTypeId"]
                
                if "instanceInfo" in task_config and node_type in task_config["instanceInfo"]:
                    instance_info = task_config["instanceInfo"][node_type]
                    
                    # Extract resource requirements
                    if "resourceVector" in instance_info:
                        resources = instance_info["resourceVector"]
                        cpu_reqs = resources.get("cores", [1.0])
                        mem_reqs = resources.get("memory", [1024])
                        
                        # Convert to proper types
                        if isinstance(cpu_reqs[0], int):
                            cpu_reqs = [float(x) for x in cpu_reqs]
                    else:
                        cpu_reqs = [1.0]
                        mem_reqs = [1024]
                    
                    # Extract duration estimates
                    duration_ests = instance_info.get("estimatedDuration", [1000])
                    
                    # Add to task maps
                    task_maps.add_task_requirements_distribution(
                        task_type_id, cpu_reqs, mem_reqs, duration_ests
                    )
        
        logger.info(f"Created TaskMapsPerNodeType for {node_type} with "
                   f"{len(task_maps.get_supported_task_types())} task types")
        
        return task_maps