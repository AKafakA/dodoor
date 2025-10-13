"""
Resource management utilities matching Java Resources.java.

Provides system resource detection, default values, and resource vector 
operations that match the physical Java implementation exactly.
"""

import logging
from typing import Dict, Optional
from dataclasses import dataclass

try:
    from ..schedulers.base_scheduler import ResourceVector
except ImportError:
    from schedulers.base_scheduler import ResourceVector

logger = logging.getLogger(__name__)


@dataclass
class DodoorDefaults:
    """Default values matching Java DodoorConf.java exactly."""

    # System capacity defaults (from DodoorConf.java lines 129-135)
    DEFAULT_SYSTEM_MEMORY: int = 1024  # MB
    DEFAULT_SYSTEM_CORES: int = 4
    DEFAULT_SYSTEM_DISK: int = 10240   # MB

    # Resource weight defaults
    DEFAULT_CPU_WEIGHT: float = 1.0
    DEFAULT_MEMORY_WEIGHT: float = 1.0
    DEFAULT_DISK_WEIGHT: float = 0.0  # Disabled by default
    DEFAULT_DISK_WEIGHT_ENABLED: float = 1.0  # When disk is enabled

    # CRITICAL FIX: Match Java DodoorConf.java line 86
    DEFAULT_TOTAL_DURATION_WEIGHT: float = 0.95  # Java: DEFAULT_TOTAL_PENDING_DURATION_WEIGHT = 0.95f

    # CRITICAL FIX: Match Java DodoorConf.java line 81
    DEFAULT_BETA: float = 0.75  # Java: DEFAULT_BETA = 0.75 (was 0.6 in Python)

    # Match Java DodoorConf.java line 98
    DEFAULT_BATCH_SIZE: int = 1024  # Java: DEFAULT_BATCH_SIZE = 1024

    # Additional Java defaults (from DodoorConf.java)
    DEFAULT_REPLAY_WITH_DISK: bool = False  # Line 207: DEFAULT_REPLAY_WITH_DISK = false
    DEFAULT_RESTRICT_FIFO: bool = True      # Line 212: DEFAULT_RESTRICT_FIFO = true
    DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE: int = 8  # Java: DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE = 8
    DEFAULT_PREQUAL_PROBE_RATE: int = 3     # Line 61: DEFAULT_PREQUAL_PROBE_RATE = 3
    DEFAULT_PREQUAL_PROBE_POOL_SIZE: int = 16  # Line 63: DEFAULT_PREQUAL_PROBE_POOL_SIZE = 16
    DEFAULT_PREQUAL_RIF_QUANTILE: float = 0.84  # Line 65: DEFAULT_PREQUAL_RIF_QUANTILE = 0.84


class ResourceUtils:
    """
    Resource utilities matching Java Resources.java.
    
    Provides system resource detection and default capacity calculation
    with same logic as the physical Java implementation.
    """
    
    @staticmethod
    def get_system_resource_vector(node_config: Dict, replay_with_disk: bool = False) -> ResourceVector:
        """
        Get system resource vector matching Java getSystemResourceVector().
        
        Args:
            node_config: Node configuration dictionary
            replay_with_disk: Whether to include disk resources
            
        Returns:
            System resource vector with detected/configured capacities
        """
        cores = ResourceUtils.get_system_cores_capacity(node_config)
        memory = ResourceUtils.get_memory_mb_capacity(node_config)
        
        if replay_with_disk:
            disk = ResourceUtils.get_system_disk_gb_capacity(node_config)
        else:
            disk = 0
        
        return ResourceVector(cores=cores, memory=memory, disk=disk)
    
    @staticmethod
    def get_system_cores_capacity(node_config: Dict) -> int:
        """
        Get system cores capacity matching Java getSystemCoresCapacity().
        
        Args:
            node_config: Node configuration dictionary
            
        Returns:
            Number of CPU cores
        """
        return node_config.get('system_cores', DodoorDefaults.DEFAULT_SYSTEM_CORES)
    
    @staticmethod
    def get_memory_mb_capacity(node_config: Dict) -> int:
        """
        Get memory capacity matching Java getMemoryMbCapacity().
        
        In Java this reads /proc/meminfo for actual system memory.
        In simulation we use configured values or defaults.
        
        Args:
            node_config: Node configuration dictionary
            
        Returns:
            Memory capacity in MB
        """
        if 'system_memory' in node_config:
            return node_config['system_memory']
        
        # In real Java system, this would read /proc/meminfo
        # For simulation, use default
        return DodoorDefaults.DEFAULT_SYSTEM_MEMORY
    
    @staticmethod  
    def get_system_disk_gb_capacity(node_config: Dict) -> int:
        """
        Get disk capacity matching Java getSystemDiskGbCapacity().
        
        Args:
            node_config: Node configuration dictionary
            
        Returns:
            Disk capacity in MB (converted from GB)
        """
        disk_gb = node_config.get('system_disk', DodoorDefaults.DEFAULT_SYSTEM_DISK // 1024)
        return disk_gb * 1024  # Convert GB to MB
    
    @staticmethod
    def get_resource_weights(scheduler_config: Dict, 
                           replay_with_disk: bool = False) -> Dict[str, float]:
        """
        Get resource weights matching Java TaskPlacer weight initialization.
        
        Args:
            scheduler_config: Scheduler configuration dictionary
            replay_with_disk: Whether disk resources are enabled
            
        Returns:
            Dictionary with cpu_weight, mem_weight, disk_weight, duration_weight
        """
        cpu_weight = scheduler_config.get('cpu_weight', DodoorDefaults.DEFAULT_CPU_WEIGHT)
        mem_weight = scheduler_config.get('memory_weight', DodoorDefaults.DEFAULT_MEMORY_WEIGHT)
        
        # Disk weight defaults to 0 unless explicitly enabled
        if replay_with_disk:
            disk_weight = scheduler_config.get('disk_weight', DodoorDefaults.DEFAULT_DISK_WEIGHT_ENABLED)
        else:
            disk_weight = DodoorDefaults.DEFAULT_DISK_WEIGHT
        
        duration_weight = scheduler_config.get(
            'total_duration_weight', DodoorDefaults.DEFAULT_TOTAL_DURATION_WEIGHT
        )
        
        return {
            'cpu_weight': cpu_weight,
            'mem_weight': mem_weight, 
            'disk_weight': disk_weight,
            'duration_weight': duration_weight
        }
    
    @staticmethod
    def validate_resource_weights(cpu_weight: float, mem_weight: float, 
                                disk_weight: float, duration_weight: float) -> bool:
        """
        Validate resource weights like Java CachedTaskPlacer validation.
        
        Args:
            cpu_weight: CPU weight
            mem_weight: Memory weight
            disk_weight: Disk weight  
            duration_weight: Duration weight
            
        Returns:
            True if weights are valid for SCORE packing strategy
        """
        # Java validation: not all weights can be 1.0 for SCORE strategy
        return not (cpu_weight == 1.0 and mem_weight == 1.0 and 
                   disk_weight == 1.0 and duration_weight == 1.0)
    
    @staticmethod
    def calculate_resource_utilization(allocated: ResourceVector, 
                                     capacity: ResourceVector) -> Dict[str, float]:
        """
        Calculate resource utilization percentages.
        
        Args:
            allocated: Currently allocated resources
            capacity: Total capacity
            
        Returns:
            Dictionary with cpu_util, memory_util, disk_util percentages
        """
        return {
            'cpu_util': allocated.cores / max(capacity.cores, 1e-6),
            'memory_util': allocated.memory / max(capacity.memory, 1e-6),
            'disk_util': allocated.disk / max(capacity.disk, 1e-6)
        }
    
    @staticmethod
    def can_allocate_resources(allocated: ResourceVector, capacity: ResourceVector,
                             requested: ResourceVector) -> bool:
        """
        Check if requested resources can be allocated.
        
        Args:
            allocated: Currently allocated resources
            capacity: Total capacity
            requested: Requested resources
            
        Returns:
            True if allocation is possible
        """
        remaining = capacity - allocated
        return (requested.cores <= remaining.cores and
                requested.memory <= remaining.memory and  
                requested.disk <= remaining.disk)
    
    @staticmethod
    def get_effective_task_resources(task_type: str, raw_resources: ResourceVector,
                                   task_node_state_map: Optional[Dict] = None,
                                   node_type: str = "default") -> ResourceVector:
        """
        Get effective task resources matching Java task type handling.
        
        Handles the distinction between SIMULATED tasks (use raw resources)
        and real tasks (use mapped resources from TaskMapsPerNodeType).
        
        Args:
            task_type: Task type identifier
            raw_resources: Raw resource request from task
            task_node_state_map: Task mappings per node type
            node_type: Target node type
            
        Returns:
            Effective resource requirements for this node type
        """
        # Java TaskTypeID.SIMULATED check
        if task_type == "simulated" or task_type.upper() == "SIMULATED":
            return raw_resources
        
        # Try to get mapped resources
        if (task_node_state_map and node_type in task_node_state_map and
            task_node_state_map[node_type].has_task_type(task_type)):
            
            task_maps = task_node_state_map[node_type]
            return task_maps.get_resource_vector(task_type)
        
        # Fallback to raw resources
        logger.warning(f"No resource mapping for task type '{task_type}' on node type '{node_type}'")
        return raw_resources