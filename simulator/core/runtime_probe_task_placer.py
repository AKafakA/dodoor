"""
RunTimeProbeTaskPlacer implementation matching Java RunTimeProbeTaskPlacer.

Real-time probing implementation of Power-of-Two scheduling that queries
nodes for fresh state during scheduling decisions (vs cached state).
"""

import logging
import random
from typing import Dict, List

try:
    from ..schedulers.base_scheduler import NodeState, ResourceVector
    from .task_placer import TaskPlacer, TaskPlacementRequest, SchedulingRequest
    from .packing_strategy import PackingStrategy
    from .task_maps_per_node_type import TaskMapsPerNodeType
    from .task_type_id import TaskTypeID
    from .load_score import LoadScore
except ImportError:
    from schedulers.base_scheduler import NodeState, ResourceVector
    from core.task_placer import TaskPlacer, TaskPlacementRequest, SchedulingRequest
    from core.packing_strategy import PackingStrategy
    from core.task_maps_per_node_type import TaskMapsPerNodeType
    from core.task_type_id import TaskTypeID
    from core.load_score import LoadScore

logger = logging.getLogger(__name__)


class RunTimeProbeTaskPlacer(TaskPlacer):
    """
    Runtime probing task placer matching Java RunTimeProbeTaskPlacer.java.
    
    Implements Power-of-Two scheduling with real-time node state probing
    instead of using cached state. This matches the original Power-of-Two
    algorithm more closely but has higher latency due to network calls.
    
    Key differences from CachedTaskPlacer:
    - Queries nodes for fresh state during each scheduling decision
    - Higher accuracy but increased scheduling latency
    - Used by real Power-of-Two scheduler vs cached variant
    """
    
    def __init__(self, beta: float, packing_strategy: PackingStrategy,
                 resource_capacity_map: Dict[str, ResourceVector],
                 cpu_weight: float = 1.0, mem_weight: float = 1.0,
                 disk_weight: float = 1.0, total_duration_weight: float = 0.5,
                 task_node_state_map: Dict[str, TaskMapsPerNodeType] = None,
                 node_probe_callback=None):
        """
        Initialize RunTimeProbeTaskPlacer.
        
        Args:
            beta: Power-of-two probability
            packing_strategy: Load scoring strategy
            resource_capacity_map: Node capacities
            cpu_weight: CPU resource weight
            mem_weight: Memory resource weight
            disk_weight: Disk resource weight
            total_duration_weight: Duration weight
            task_node_state_map: Task type mappings per node type
            node_probe_callback: Function to probe nodes for fresh state
        """
        super().__init__(beta, packing_strategy, resource_capacity_map,
                        cpu_weight, mem_weight, disk_weight, total_duration_weight,
                        task_node_state_map)
        
        # Callback to get fresh node state (simulates network calls)
        self.node_probe_callback = node_probe_callback
        
        # Validate configuration like Java does
        if packing_strategy == PackingStrategy.SCORE and (
            cpu_weight == 1 and mem_weight == 1 and disk_weight == 1 and total_duration_weight == 1):
            raise ValueError("PackingStrategy.SCORE requires proper resource weights")
        
        logger.info(f"Initialized RunTimeProbeTaskPlacer: β={beta}, strategy={packing_strategy}")
    
    def get_enqueue_task_reservation_requests(self, scheduling_request: SchedulingRequest,
                                            load_maps: Dict[str, NodeState],
                                            scheduler_address: str,
                                            round: int = 0) -> List[TaskPlacementRequest]:
        """
        Get task placement decisions using real-time node probing.

        Implements the exact algorithm from Java RunTimeProbeTaskPlacer.java:
        1. For each task, select nodes using (1+β)-choice
        2. Probe selected nodes for fresh state via network calls
        3. Score nodes based on packing strategy using fresh state
        4. Create placement requests

        Args:
            scheduling_request: Batch of tasks to schedule
            load_maps: Current node states (used for fallback only)
            scheduler_address: Scheduler identifier
            round: Reservation round number (for reproducible placement)

        Returns:
            List of task placement decisions
        """
        placements = []
        node_addresses = list(load_maps.keys())

        if not node_addresses:
            logger.warning("No nodes available for scheduling")
            return placements

        # Track probe latency for scheduling metrics
        total_probe_latency_ms = 0.0

        for task in scheduling_request.tasks:
            # Select node using (1+β)-choice with real-time probing
            selected_node_id, probe_latency = self._select_node_with_probing(
                task, node_addresses, load_maps, round
            )
            
            if selected_node_id is None:
                logger.warning(f"No node selected for task {task.task_id}")
                continue
            
            # Accumulate probe latency for scheduling metrics
            total_probe_latency_ms += probe_latency
            
            selected_node_state = load_maps[selected_node_id]
            
            # Get actual task resources (handles SIMULATED vs real task types)
            task_resources = self._get_task_resources(task, selected_node_state)
            
            # Create placement request
            TaskPlacer.update_scheduling_results(
                placements, selected_node_id, scheduling_request, 
                task, scheduler_address, task_resources
            )
        
        logger.debug(f"RunTimeProbeTaskPlacer scheduled {len(placements)} tasks "
                    f"with {total_probe_latency_ms:.2f}ms total probe latency")
        
        # Store probe latency for scheduling metrics (this represents the overhead
        # of real-time probing vs cached approaches)
        if hasattr(scheduling_request, 'additional_latency_ms'):
            scheduling_request.additional_latency_ms = total_probe_latency_ms
        
        return placements
    
    def _select_node_with_probing(self, task, node_addresses: List[str],
                                 load_maps: Dict[str, NodeState], round: int = 0) -> tuple:
        """
        Select node using (1+β)-choice with real-time probing.

        Matches Java RunTimeProbeTaskPlacer node selection logic exactly.

        Args:
            task: Task to schedule
            node_addresses: Available node IDs
            load_maps: Current node states (for fallback)
            round: Reservation round number (combined with task_id for seeding)

        Returns:
            Tuple of (selected_node_id, probe_latency_ms)
        """
        # Match Java: Random ran = new Random(taskSpec.taskId.hashCode() + round);
        seed = hash(task.task_id) + round
        ran = random.Random(seed)
        first_index = ran.randint(0, len(node_addresses) - 1)
        
        if self.beta > 0 and ran.random() < self.beta and len(node_addresses) > 1:
            # Power-of-two selection with real-time probing
            second_index = first_index
            while second_index == first_index:
                second_index = ran.randint(0, len(node_addresses) - 1)
            
            node1_id = node_addresses[first_index]
            node2_id = node_addresses[second_index]
            
            # Probe both nodes for fresh state (simulates network calls)
            fresh_state1, latency1 = self._probe_node_state(node1_id, load_maps)
            fresh_state2, latency2 = self._probe_node_state(node2_id, load_maps)
            
            # Total probe latency for this task
            total_probe_latency = latency1 + latency2
            
            # Score both nodes using fresh state
            score1, score2 = self._score_nodes_with_strategy(
                fresh_state1, fresh_state2, task
            )
            
            # Select node with better (lower) score
            if score1 <= score2:
                return node1_id, total_probe_latency
            else:
                return node2_id, total_probe_latency
        else:
            # Random selection (1-β probability) - no probe latency
            return node_addresses[first_index], 0.0
    
    def _probe_node_state(self, node_id: str, load_maps: Dict[str, NodeState]) -> tuple:
        """
        Probe node for fresh state (simulates network call).
        
        In Java this calls nodeMonitorClient.getNodeState() over Thrift.
        In simulation, we use a callback to get fresh state from node executors.
        
        Args:
            node_id: Node to probe
            load_maps: Cached state (fallback)
            
        Returns:
            Tuple of (fresh_node_state, probe_latency_ms)
        """
        # Simulate network probe latency (2ms mean like Java system)
        probe_latency_ms = random.uniform(1.0, 3.0)  # 1-3ms with 2ms mean
        
        if self.node_probe_callback:
            try:
                fresh_state = self.node_probe_callback(node_id)
                if fresh_state:
                    return fresh_state, probe_latency_ms
            except Exception as e:
                logger.warning(f"Failed to probe node {node_id}: {e}")
        
        # Fallback to cached state if probe fails
        return load_maps[node_id], probe_latency_ms
    
    def _score_nodes_with_strategy(self, state1: NodeState, state2: NodeState, task) -> tuple:
        """
        Score two nodes based on packing strategy using fresh state.
        
        Args:
            state1: First node state (fresh from probe)
            state2: Second node state (fresh from probe)
            task: Task being scheduled
            
        Returns:
            Tuple of (score1, score2) - lower is better
        """
        if self.packing_strategy == PackingStrategy.SCORE:
            # Multi-dimensional load scoring
            return LoadScore.get_load_scores_pairs(
                state1, state2, task.task_type, task.resource_request,
                self.cpu_weight, self.mem_weight, self.disk_weight,
                self.total_duration_weight, self.resource_capacity_map,
                self.task_node_state_map
            )
        
        elif self.packing_strategy == PackingStrategy.RIF:
            # Running in FIFO - use task count (most common for Power-of-Two)
            return state1.num_tasks, state2.num_tasks
        
        elif self.packing_strategy == PackingStrategy.DURATION:
            # Duration-based scoring
            return state1.total_duration_ms, state2.total_duration_ms
        
        elif self.packing_strategy == PackingStrategy.NONE:
            # No optimization - equal scores
            return 0.0, 0.0
        
        else:
            raise ValueError(f"Unknown packing strategy: {self.packing_strategy}")
    
    @staticmethod
    def create_power_of_two_placer(beta: float, resource_capacity_map: Dict[str, ResourceVector],
                                  task_node_state_map: Dict[str, TaskMapsPerNodeType],
                                  node_probe_callback=None) -> 'RunTimeProbeTaskPlacer':
        """
        Create RunTimeProbeTaskPlacer for real Power-of-Two scheduler.
        
        Args:
            beta: Power-of-two probability
            resource_capacity_map: Node capacities
            task_node_state_map: Task mappings
            node_probe_callback: Function to probe nodes
            
        Returns:
            Configured RunTimeProbeTaskPlacer for Power-of-Two
        """
        return RunTimeProbeTaskPlacer(
            beta, PackingStrategy.RIF, resource_capacity_map,
            1.0, 1.0, 1.0, 1.0, task_node_state_map, node_probe_callback
        )