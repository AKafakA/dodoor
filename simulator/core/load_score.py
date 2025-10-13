"""
LoadScore implementation matching Java multi-dimensional load scoring.

Direct port from Java LoadScore.java - critical missing component for
proper Dodoor scheduler load calculations.
"""

import logging
from typing import Tuple, Dict

try:
    from ..schedulers.base_scheduler import ResourceVector, NodeState
    from .task_maps_per_node_type import TaskMapsPerNodeType
    from .resource_utils import ResourceUtils
except ImportError:
    from schedulers.base_scheduler import ResourceVector, NodeState
    from core.task_maps_per_node_type import TaskMapsPerNodeType
    from core.resource_utils import ResourceUtils

logger = logging.getLogger(__name__)


class LoadScore:
    """
    Multi-dimensional load scoring matching Java LoadScore.java implementation.
    
    Implements the sophisticated load calculation algorithm used by Dodoor
    that combines resource utilization with duration-based scoring.
    
    Matches Java edu.cam.dodoor.scheduler.taskplacer.LoadScore.java
    """
    
    @staticmethod
    def get_load_scores_pairs(state1: NodeState, state2: NodeState,
                            task_type: str, task_resources: ResourceVector,
                            cpu_weight: float, mem_weight: float, disk_weight: float,
                            total_duration_weight: float,
                            resource_capacity_map: Dict[str, ResourceVector],
                            task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> Tuple[float, float]:
        """
        Calculate load scores for two nodes matching Java LoadScore.getLoadScoresPairs().

        This implements the exact algorithm from Java LoadScore.java lines 40-80:
        1. Get resource load scores for both nodes using getResourceLoadScores()
        2. Normalize resource loads relative to each other
        3. Normalize duration loads relative to each other
        4. Combine with total_duration_weight: (1-α)*resource + α*duration

        Args:
            state1: First node state
            state2: Second node state
            task_type: Task type identifier
            task_resources: Task resource requirements
            cpu_weight: CPU weight factor
            mem_weight: Memory weight factor
            disk_weight: Disk weight factor
            total_duration_weight: Duration weight factor (α in Dodoor paper)
            resource_capacity_map: Node type -> capacity mapping
            task_node_state_map: Node type -> task mapping

        Returns:
            Tuple of (score1, score2) - lower scores are better
        """
        if total_duration_weight < 0 or total_duration_weight > 1:
            raise ValueError("total_duration_weight must be between 0 and 1")

        # Get task resource vectors (Java lines 53-58)
        first_resource_vector = task_resources
        second_resource_vector = task_resources
        if task_type != "simulated":
            if state1.node_type in task_node_state_map:
                first_resource_vector = task_node_state_map[state1.node_type].get_resource_vector(task_type)
            if state2.node_type in task_node_state_map:
                second_resource_vector = task_node_state_map[state2.node_type].get_resource_vector(task_type)

        # Calculate resource load scores (Java lines 59-62)
        first_resource_load = LoadScore.get_resource_load_scores(
            state1.allocated, first_resource_vector, cpu_weight, mem_weight, disk_weight,
            resource_capacity_map[state1.node_type]
        )
        second_resource_load = LoadScore.get_resource_load_scores(
            state2.allocated, second_resource_vector, cpu_weight, mem_weight, disk_weight,
            resource_capacity_map[state2.node_type]
        )

        # Normalize resource loads relative to each other (Java lines 64-65)
        total_resource_load = first_resource_load + second_resource_load
        if total_resource_load > 0:
            first_normalized_resource_load = first_resource_load / total_resource_load
            second_normalized_resource_load = second_resource_load / total_resource_load
        else:
            first_normalized_resource_load = 0.5
            second_normalized_resource_load = 0.5

        # Get total durations (Java lines 67-68)
        first_total_duration = state1.total_duration_ms
        second_total_duration = state2.total_duration_ms

        # Normalize duration loads relative to each other (Java lines 70-71)
        total_duration = first_total_duration + second_total_duration
        if total_duration > 0:
            first_normalized_total_duration = first_total_duration / total_duration
            second_normalized_total_duration = second_total_duration / total_duration
        else:
            first_normalized_total_duration = 0.5
            second_normalized_total_duration = 0.5

        # Combine with duration weight (Java lines 73-74)
        first_load_score = (first_normalized_resource_load * (1 - total_duration_weight) +
                           first_normalized_total_duration * total_duration_weight)
        second_load_score = (second_normalized_resource_load * (1 - total_duration_weight) +
                            second_normalized_total_duration * total_duration_weight)

        logger.debug(f"Load score calculation: "
                    f"first_resource_load={first_resource_load:.3f}, first_total_duration={first_total_duration}, "
                    f"first_load_score={first_load_score:.3f}, "
                    f"second_resource_load={second_resource_load:.3f}, second_total_duration={second_total_duration}, "
                    f"second_load_score={second_load_score:.3f}")

        return first_load_score, second_load_score

    @staticmethod
    def get_resource_load_scores(requested_resources: ResourceVector, task_resources: ResourceVector,
                               cpu_weight: float, mem_weight: float, disk_weight: float,
                               resource_capacity: ResourceVector) -> float:
        """
        Calculate resource load score matching Java LoadScore.getResourceLoadScores().

        Java implementation (lines 20-38):
        - cpuLoad = cpuWeight * (requestedResources.cores * taskResources.cores) / (capacity.cores^2)
        - memLoad = memWeight * (requestedResources.memory / capacity.memory) * (taskResources.memory / capacity.memory)
        - diskLoad = diskWeight * (requestedResources.disks / capacity.disks) * (taskResources.disks / capacity.disks)
        - normalizedResourceLoad = (cpuLoad + memLoad + diskLoad) / (cpuWeight + memWeight + diskWeight)

        Args:
            requested_resources: Current node resource allocation (from NodeState.allocated)
            task_resources: New task resource requirements
            cpu_weight: CPU weight factor
            mem_weight: Memory weight factor
            disk_weight: Disk weight factor
            resource_capacity: Node capacity

        Returns:
            Normalized resource load score
        """
        # CPU load calculation (Java line 23-24)
        if resource_capacity.cores > 0:
            cpu_load = cpu_weight * (requested_resources.cores * task_resources.cores) / (
                resource_capacity.cores * resource_capacity.cores)
        else:
            cpu_load = 0.0

        # Memory load calculation (Java line 25-26)
        if resource_capacity.memory > 0:
            mem_load = mem_weight * (requested_resources.memory / resource_capacity.memory) * (
                task_resources.memory / resource_capacity.memory)
        else:
            mem_load = 0.0

        # Disk load calculation (Java lines 27-31)
        if resource_capacity.disk > 0:
            disk_load = disk_weight * (requested_resources.disk / resource_capacity.disk) * (
                task_resources.disk / resource_capacity.disk)
        else:
            disk_load = 0.0

        # Normalize by total weights (Java line 32)
        total_weight = cpu_weight + mem_weight + disk_weight
        if total_weight > 0:
            normalized_resource_load = (cpu_load + mem_load + disk_load) / total_weight
        else:
            normalized_resource_load = 0.0

        logger.debug(f"Resource load calculation: "
                    f"cpuLoad={cpu_load:.3f}, memLoad={mem_load:.3f}, diskLoad={disk_load:.3f}, "
                    f"requested cpu={requested_resources.cores:.1f}, task cpu={task_resources.cores:.1f}, "
                    f"cpu capacity={resource_capacity.cores:.1f}, "
                    f"requested mem={requested_resources.memory:.0f}, task mem={task_resources.memory:.0f}, "
                    f"mem capacity={resource_capacity.memory:.0f}, "
                    f"final resourceScore={normalized_resource_load:.3f}")

        return normalized_resource_load

    @staticmethod
    def _calculate_load_score(node_state: NodeState, task_type: str,
                            task_resources: ResourceVector,
                            cpu_weight: float, mem_weight: float, disk_weight: float,
                            total_duration_weight: float,
                            resource_capacity_map: Dict[str, ResourceVector],
                            task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> float:
        """
        Calculate load score for a single node.
        
        Implements Dodoor's multi-dimensional scoring:
        Load = (1-α) * ResourceLoad + α * DurationLoad
        
        Where ResourceLoad combines CPU/memory/disk utilization with weights.
        """
        # Get node capacity
        if node_state.node_id not in resource_capacity_map:
            logger.warning(f"Node {node_state.node_id} not found in capacity map")
            return float('inf')
        
        node_capacity = resource_capacity_map[node_state.node_id]
        
        # Calculate resource load component
        resource_load = LoadScore._calculate_resource_load_score(
            node_state, task_resources, node_capacity,
            cpu_weight, mem_weight, disk_weight
        )
        
        # Calculate duration load component  
        duration_load = LoadScore._calculate_duration_load_score(
            node_state, task_type, task_node_state_map.get(node_state.node_type)
        )
        
        # Combine with duration weight (α parameter from Dodoor paper)
        total_score = (1 - total_duration_weight) * resource_load + total_duration_weight * duration_load
        
        return total_score
    
    @staticmethod
    def _calculate_resource_load_score(node_state: NodeState, task_resources: ResourceVector,
                                     node_capacity: ResourceVector,
                                     cpu_weight: float, mem_weight: float, disk_weight: float) -> float:
        """
        Calculate resource load score component.
        
        Computes weighted resource utilization after adding the task.
        """
        # Current utilization using ResourceUtils
        current_util = ResourceUtils.calculate_resource_utilization(
            node_state.allocated, node_capacity
        )
        
        # Utilization after adding task
        new_allocated = node_state.allocated + task_resources
        new_util = ResourceUtils.calculate_resource_utilization(
            new_allocated, node_capacity
        )
        
        # Weighted resource score (higher utilization = higher score = worse)
        resource_score = (
            cpu_weight * new_util['cpu_util'] +
            mem_weight * new_util['memory_util'] + 
            disk_weight * new_util['disk_util']
        ) / (cpu_weight + mem_weight + disk_weight)
        
        return resource_score
    
    @staticmethod  
    def _calculate_duration_load_score(node_state: NodeState, task_type: str,
                                     task_maps: TaskMapsPerNodeType) -> float:
        """
        Calculate duration load score component.
        
        Uses total pending duration as proxy for expected wait time.
        """
        # Get task duration for this node type
        if task_maps and task_maps.has_task_type(task_type):
            task_duration = task_maps.get_task_duration(task_type)
        else:
            # Fallback to default duration
            task_duration = 1000  # 1 second default
        
        # Current total duration + new task duration
        total_duration = node_state.total_duration_ms + task_duration
        
        # Normalize by some factor to keep scores reasonable
        duration_score = total_duration / 10000.0  # Normalize to ~seconds
        
        return duration_score
    
    @staticmethod
    def calculate_single_node_score(node_state: NodeState, task_type: str,
                                  task_resources: ResourceVector,
                                  cpu_weight: float, mem_weight: float, disk_weight: float,
                                  total_duration_weight: float,
                                  resource_capacity_map: Dict[str, ResourceVector],
                                  task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> float:
        """
        Calculate load score for a single node (convenience method).
        
        Returns:
            Load score - lower is better
        """
        return LoadScore._calculate_load_score(
            node_state, task_type, task_resources, cpu_weight, mem_weight,
            disk_weight, total_duration_weight, resource_capacity_map, task_node_state_map
        )