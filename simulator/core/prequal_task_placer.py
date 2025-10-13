"""
PrequalTaskPlacer implementation matching Java PrequalTaskPlacer.

Google Prequal scheduler with probe pool management and quantile-based
node selection for centralized scheduling.
"""

import logging
import random
import time
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

try:
    from ..schedulers.base_scheduler import NodeState, ResourceVector
    from .task_placer import TaskPlacer, TaskPlacementRequest, SchedulingRequest
    from .packing_strategy import PackingStrategy
    from .task_maps_per_node_type import TaskMapsPerNodeType
    from .task_type_id import TaskTypeID
except ImportError:
    from schedulers.base_scheduler import NodeState, ResourceVector
    from core.task_placer import TaskPlacer, TaskPlacementRequest, SchedulingRequest
    from core.packing_strategy import PackingStrategy
    from core.task_maps_per_node_type import TaskMapsPerNodeType
    from core.task_type_id import TaskTypeID

logger = logging.getLogger(__name__)


class ProbeInfo:
    """Probe information for a node (timestamp, usage count)."""
    
    def __init__(self, probe_time: float, used_count: int = 0):
        self.probe_time = probe_time
        self.used_count = used_count


class PrequalTaskPlacer(TaskPlacer):
    """
    Prequal task placer matching Java PrequalTaskPlacer.java.
    
    Implements Google's Prequal scheduler with probe pool management:
    - Maintains a pool of recently probed nodes
    - Uses quantile-based cutoffs for load balancing
    - Reuses probes within budget constraints
    - Falls back to random selection when pool is empty
    
    Matches Java edu.cam.dodoor.scheduler.taskplacer.PrequalTaskPlacer.java
    """
    
    def __init__(self, beta: float, resource_capacity_map: Dict[str, ResourceVector],
                 rif_quantile: float = 0.5, probe_pool_size: int = 10,
                 delta: int = 1, probe_rate: int = 2, probe_delete_rate: int = 1,
                 probe_age_budget_ms: float = 5000.0,
                 task_node_state_map: Dict[str, TaskMapsPerNodeType] = None):
        """
        Initialize PrequalTaskPlacer.
        
        Args:
            beta: Power-of-two probability (not used in Prequal)
            resource_capacity_map: Node capacities
            rif_quantile: Quantile for task count cutoff (default 0.5 = median)
            probe_pool_size: Maximum size of probe pool
            delta: Delta parameter for probe budget calculation
            probe_rate: Rate parameter for probe budget calculation
            probe_delete_rate: Delete rate parameter for probe budget
            probe_age_budget_ms: Maximum age of probe in milliseconds
            task_node_state_map: Task type mappings per node type
        """
        super().__init__(beta, PackingStrategy.NONE, resource_capacity_map,
                        1.0, 1.0, 1.0, 1.0, task_node_state_map)
        
        # Prequal-specific parameters
        self.rif_quantile = rif_quantile
        self.probe_pool_size = probe_pool_size
        self.delta = delta
        self.probe_rate = probe_rate
        self.probe_delete_rate = probe_delete_rate
        self.probe_age_budget_ms = probe_age_budget_ms
        
        # Probe pool: node_id -> ProbeInfo
        self.probe_info: Dict[str, ProbeInfo] = {}
        
        logger.info(f"Initialized PrequalTaskPlacer: quantile={rif_quantile}, "
                   f"pool_size={probe_pool_size}, age_budget={probe_age_budget_ms}ms")
    
    def get_enqueue_task_reservation_requests(self, scheduling_request: SchedulingRequest,
                                            load_maps: Dict[str, NodeState],
                                            scheduler_address: str,
                                            round: int = 0) -> List[TaskPlacementRequest]:
        """
        Get task placement decisions using Prequal algorithm.
        
        Implements the exact algorithm from Java PrequalTaskPlacer.java:
        1. Calculate quantile cutoff for task counts
        2. Select best node from prequal probe pool
        3. All tasks in request go to same node (batch placement)
        4. Update probe usage counts
        
        Args:
            scheduling_request: Batch of tasks to schedule
            load_maps: Current node states
            scheduler_address: Scheduler identifier
            
        Returns:
            List of task placement decisions
        """
        placements = []
        
        if not load_maps:
            logger.warning("No nodes available for scheduling")
            return placements
        
        # Calculate quantile cutoff for task counts
        task_counts = [state.num_tasks for state in load_maps.values()]
        cutoff = self._get_quantile(task_counts, self.rif_quantile)
        
        # Select best node from prequal pool
        selected_node_id, selected_node_state = self._select_least_node_from_prequal_pool(
            load_maps, cutoff
        )
        
        # Place all tasks from request on selected node (batch placement)
        for task in scheduling_request.tasks:
            # Get actual task resources (handles SIMULATED vs real task types)
            task_resources = self._get_task_resources(task, selected_node_state)
            
            # Create placement request
            TaskPlacer.update_scheduling_results(
                placements, selected_node_id, scheduling_request, 
                task, scheduler_address, task_resources
            )
        
        logger.debug(f"PrequalTaskPlacer scheduled {len(placements)} tasks to {selected_node_id}")
        return placements
    
    def _select_least_node_from_prequal_pool(self, load_maps: Dict[str, NodeState],
                                           task_count_cutoff: int) -> Tuple[str, NodeState]:
        """
        Select best node from prequal probe pool.
        
        Matches Java selectLeastNodeFromPrequalPool() method exactly:
        1. Filter probe pool by age and usage budget
        2. Prefer nodes below task count cutoff
        3. Among eligible nodes, select by lowest total duration
        4. Fallback to lowest task count if no nodes below cutoff
        5. Fallback to random if probe pool empty
        
        Args:
            load_maps: Current node states
            task_count_cutoff: Task count cutoff from quantile
            
        Returns:
            Tuple of (selected_node_id, selected_node_state)
        """
        current_time = time.time() * 1000  # Convert to milliseconds
        
        # Build prequal load maps from valid probes
        prequal_load_maps = {}
        probe_reuse_budget = self._get_probe_reuse_budget(len(load_maps))
        
        # Sort probe addresses by reverse order (most recent first)
        probe_addresses = list(self.probe_info.keys())
        probe_addresses.reverse()
        
        # Filter probe pool by budget constraints
        for i, node_id in enumerate(probe_addresses):
            if i >= self.probe_pool_size:
                break
                
            if node_id not in load_maps:
                continue
                
            probe_info = self.probe_info[node_id]
            
            # Check probe age and usage budget
            probe_age = current_time - probe_info.probe_time
            if (probe_info.used_count < probe_reuse_budget and 
                probe_age < self.probe_age_budget_ms):
                
                prequal_load_maps[node_id] = load_maps[node_id]
                # Increment usage count
                probe_info.used_count += 1
        
        if not prequal_load_maps:
            # Prequal queue is empty, select random node
            logger.debug("Prequal queue is empty, selecting random node")
            random_node_id = random.choice(list(load_maps.keys()))
            return random_node_id, load_maps[random_node_id]
        
        # Find nodes below task count cutoff, sorted by total duration
        below_cutoff = [
            (node_id, state) for node_id, state in prequal_load_maps.items()
            if state.num_tasks < task_count_cutoff
        ]
        
        if below_cutoff:
            # Select node with lowest total duration among those below cutoff
            selected_node_id, selected_state = min(
                below_cutoff, key=lambda x: x[1].total_duration_ms
            )
        else:
            # No nodes below cutoff, select node with minimum task count
            selected_node_id, selected_state = min(
                prequal_load_maps.items(), key=lambda x: x[1].num_tasks
            )
        
        return selected_node_id, selected_state
    
    def _get_probe_reuse_budget(self, total_nodes: int) -> int:
        """
        Calculate probe reuse budget using Prequal formula.
        
        Matches Java SchedulerUtils.getProbeReuseBudget() calculation.
        
        Args:
            total_nodes: Total number of nodes in cluster
            
        Returns:
            Probe reuse budget count
        """
        # Simplified version of Java calculation
        budget = max(1, (total_nodes * self.probe_rate) // 
                        (self.probe_pool_size * self.probe_delete_rate + self.delta))
        return budget
    
    def _get_quantile(self, values: List[int], quantile: float) -> int:
        """
        Calculate quantile of values (matches Java MetricsUtils.getQuantile).
        
        Args:
            values: List of values
            quantile: Quantile to calculate (0.0 to 1.0)
            
        Returns:
            Quantile value
        """
        if not values:
            return 0
        
        sorted_values = sorted(values)
        index = int(quantile * (len(sorted_values) - 1))
        return sorted_values[index]
    
    def add_probe(self, node_id: str, probe_time: Optional[float] = None):
        """
        Add a probe to the probe pool.
        
        Args:
            node_id: Node that was probed
            probe_time: Time of probe (default: current time)
        """
        if probe_time is None:
            probe_time = time.time() * 1000  # Convert to milliseconds
        
        self.probe_info[node_id] = ProbeInfo(probe_time, 0)
        
        # Maintain probe pool size limit
        if len(self.probe_info) > self.probe_pool_size * 2:  # Allow some buffer
            # Remove oldest probes
            oldest_nodes = sorted(
                self.probe_info.items(), 
                key=lambda x: x[1].probe_time
            )[:len(self.probe_info) - self.probe_pool_size]
            
            for node_id, _ in oldest_nodes:
                del self.probe_info[node_id]
    
    def cleanup_expired_probes(self):
        """Remove expired probes from the pool."""
        current_time = time.time() * 1000
        expired_nodes = [
            node_id for node_id, probe_info in self.probe_info.items()
            if current_time - probe_info.probe_time > self.probe_age_budget_ms
        ]
        
        for node_id in expired_nodes:
            del self.probe_info[node_id]
    
    @staticmethod
    def create_prequal_placer(resource_capacity_map: Dict[str, ResourceVector],
                             task_node_state_map: Dict[str, TaskMapsPerNodeType],
                             rif_quantile: float = 0.5,
                             probe_pool_size: int = 10) -> 'PrequalTaskPlacer':
        """
        Create PrequalTaskPlacer with default Google Prequal configuration.
        
        Args:
            resource_capacity_map: Node capacities
            task_node_state_map: Task mappings
            rif_quantile: Quantile for cutoff (default 0.5)
            probe_pool_size: Size of probe pool (default 10)
            
        Returns:
            Configured PrequalTaskPlacer
        """
        return PrequalTaskPlacer(
            beta=0.0,  # Not used in Prequal
            resource_capacity_map=resource_capacity_map,
            rif_quantile=rif_quantile,
            probe_pool_size=probe_pool_size,
            task_node_state_map=task_node_state_map
        )