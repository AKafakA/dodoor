"""
TaskPlacer base class matching Java implementation.

Direct port from Java TaskPlacer.java - the unified interface used by all schedulers
in the physical system. Replaces the fictional message-passing protocols.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
import random

try:
    from ..schedulers.base_scheduler import Task, NodeState, ResourceVector
    from .packing_strategy import PackingStrategy
    from .task_maps_per_node_type import TaskMapsPerNodeType
    from .task_type_id import TaskTypeID
    from .load_score import LoadScore
except ImportError:
    from schedulers.base_scheduler import Task, NodeState, ResourceVector
    from core.packing_strategy import PackingStrategy
    from core.task_maps_per_node_type import TaskMapsPerNodeType
    from core.task_type_id import TaskTypeID
    from core.load_score import LoadScore

logger = logging.getLogger(__name__)


class TaskPlacementRequest:
    """
    Task placement request matching Java TEnqueueTaskReservationRequest.
    
    Represents a scheduling decision: which task goes to which node.
    """
    
    def __init__(self, task: Task, assigned_node_id: str, task_resources: ResourceVector,
                 scheduler_address: str = "simulator_scheduler"):
        self.task = task
        self.assigned_node_id = assigned_node_id
        self.task_resources = task_resources  # Actual resources (may differ from task.resource_request)
        self.scheduler_address = scheduler_address
        self.enqueue_time = None  # Set when actually enqueued


class SchedulingRequest:
    """
    Batch scheduling request matching Java TSchedulingRequest.
    
    Contains multiple tasks to be scheduled together.
    """
    
    def __init__(self, tasks: List[Task], user: str = "simulator_user"):
        self.tasks = tasks
        self.user = user


class TaskPlacer(ABC):
    """
    Base TaskPlacer interface matching Java TaskPlacer.java.
    
    All schedulers in the Java system use this unified interface instead of
    the complex message-passing protocols implemented in the Python simulator.
    
    Matches Java edu.cam.dodoor.scheduler.taskplacer.TaskPlacer.java
    """
    
    def __init__(self, beta: float, packing_strategy: PackingStrategy,
                 resource_capacity_map: Dict[str, ResourceVector],
                 cpu_weight: float = 1.0, mem_weight: float = 1.0, 
                 disk_weight: float = 1.0, total_duration_weight: float = 0.5,
                 task_node_state_map: Dict[str, TaskMapsPerNodeType] = None):
        """
        Initialize TaskPlacer.
        
        Args:
            beta: Power-of-two probability (β parameter)
            packing_strategy: Load scoring strategy 
            resource_capacity_map: Node ID -> capacity mapping
            cpu_weight: CPU resource weight
            mem_weight: Memory resource weight
            disk_weight: Disk resource weight
            total_duration_weight: Duration weight (α parameter in Dodoor paper)
            task_node_state_map: Node type -> task mapping
        """
        self.beta = beta
        self.packing_strategy = packing_strategy
        self.resource_capacity_map = resource_capacity_map
        self.cpu_weight = cpu_weight
        self.mem_weight = mem_weight
        self.disk_weight = disk_weight
        self.total_duration_weight = total_duration_weight
        self.task_node_state_map = task_node_state_map or {}
        
        logger.debug(f"Initialized TaskPlacer: β={beta}, strategy={packing_strategy}")
    
    @abstractmethod
    def get_enqueue_task_reservation_requests(self, scheduling_request: SchedulingRequest,
                                            load_maps: Dict[str, NodeState],
                                            scheduler_address: str,
                                            round: int = 0) -> List[TaskPlacementRequest]:
        """
        Get task placement decisions for a batch of tasks.
        
        This is the core method that all schedulers implement - matching exactly
        the Java TaskPlacer.getEnqueueTaskReservationRequests() interface.
        
        Args:
            scheduling_request: Batch of tasks to schedule
            load_maps: Current node states (node_id -> NodeState)
            scheduler_address: Scheduler identifier
            
        Returns:
            List of task placement decisions
        """
        pass
    
    @staticmethod
    def create_task_placer(beta: float, scheduler_type: str,
                          resource_capacity_map: Dict[str, ResourceVector],
                          task_node_state_map: Dict[str, TaskMapsPerNodeType],
                          cpu_weight: float = 1.0, mem_weight: float = 1.0,
                          disk_weight: float = 1.0, total_duration_weight: float = 0.5,
                          **kwargs) -> 'TaskPlacer':
        """
        Factory method to create TaskPlacer instances matching Java factory.
        
        Maps directly to Java TaskPlacer.createTaskPlacer() method.
        
        Args:
            beta: Power-of-two probability
            scheduler_type: Scheduler type identifier
            resource_capacity_map: Node capacities
            task_node_state_map: Task type mappings
            cpu_weight: CPU weight
            mem_weight: Memory weight  
            disk_weight: Disk weight
            total_duration_weight: Duration weight
            **kwargs: Additional scheduler-specific parameters
            
        Returns:
            Appropriate TaskPlacer implementation
        """
        from .cached_task_placer import CachedTaskPlacer
        # from .runtime_probe_task_placer import RunTimeProbeTaskPlacer  # TODO: Implement
        # from .prequal_task_placer import PrequalTaskPlacer  # TODO: Implement
        
        # Map scheduler types to TaskPlacer implementations (matching Java)
        if scheduler_type == "dodoor":
            return CachedTaskPlacer(
                beta, PackingStrategy.SCORE, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        elif scheduler_type == "power_of_two":
            # TODO: Implement RunTimeProbeTaskPlacer 
            return CachedTaskPlacer(
                beta, PackingStrategy.RIF, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        elif scheduler_type == "cached_power_of_two":
            return CachedTaskPlacer(
                beta, PackingStrategy.RIF, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        elif scheduler_type == "random":
            return CachedTaskPlacer(
                -1.0, PackingStrategy.NONE, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        elif scheduler_type == "sparrow":
            return CachedTaskPlacer(
                -2.0, PackingStrategy.NONE, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        elif scheduler_type == "prequal":
            # TODO: Implement PrequalTaskPlacer
            return CachedTaskPlacer(
                beta, PackingStrategy.NONE, resource_capacity_map,
                cpu_weight, mem_weight, disk_weight, total_duration_weight,
                task_node_state_map
            )
        else:
            raise ValueError(f"Unknown scheduler type: {scheduler_type}")
    
    def _get_task_resources(self, task: Task, selected_node_state: NodeState) -> ResourceVector:
        """
        Get actual task resources for selected node type.
        
        Handles the Java distinction between SIMULATED tasks (use provided resources)
        and real tasks (use mapped resources from TaskMapsPerNodeType).
        """
        if TaskTypeID.is_simulated(task.task_type):
            # Simulated tasks use provided resource requirements
            return task.resource_request
        else:
            # Real tasks use mapped resources for the selected node type
            if (selected_node_state.node_type in self.task_node_state_map and
                self.task_node_state_map[selected_node_state.node_type].has_task_type(task.task_type)):
                
                task_maps = self.task_node_state_map[selected_node_state.node_type]
                mapped_resources = task_maps.get_resource_vector(task.task_type)
                
                # Also update task duration if available
                if hasattr(task, 'duration_ms'):
                    task.duration_ms = task_maps.get_task_duration(task.task_type)
                
                return mapped_resources
            else:
                # Fallback to provided resources if mapping not found
                logger.warning(f"No resource mapping for task type '{task.task_type}' "
                             f"on node type '{selected_node_state.node_type}'")
                return task.resource_request
    
    @staticmethod
    def update_scheduling_results(placements: List[TaskPlacementRequest],
                                node_id: str, scheduling_request: SchedulingRequest,
                                task: Task, scheduler_address: str,
                                task_resources: ResourceVector):
        """
        Helper method to add task placement result (matches Java helper method).
        
        Args:
            placements: List to add placement to
            node_id: Selected node ID
            scheduling_request: Original scheduling request
            task: Task being placed
            scheduler_address: Scheduler identifier
            task_resources: Actual task resources
        """
        placement = TaskPlacementRequest(
            task=task,
            assigned_node_id=node_id,
            task_resources=task_resources,
            scheduler_address=scheduler_address
        )
        placements.append(placement)