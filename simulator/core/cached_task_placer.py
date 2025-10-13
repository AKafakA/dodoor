"""
CachedTaskPlacer implementation matching Java CachedTaskPlacer.

Direct port from Java CachedTaskPlacer.java - handles Dodoor, Random, Sparrow,
and cached Power-of-Two scheduling using the unified TaskPlacer interface.
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


class CachedTaskPlacer(TaskPlacer):
    """
    Cached task placer matching Java CachedTaskPlacer.java.
    
    Used by multiple scheduler types in the Java system:
    - Dodoor: β > 0, PackingStrategy.SCORE with resource weights
    - Random: β = -1.0, PackingStrategy.NONE  
    - Sparrow: β = -2.0, PackingStrategy.NONE
    - Cached Power-of-Two: β > 0, PackingStrategy.RIF
    
    Implements the core (1+β)-choice algorithm with cached node states.
    """
    
    def __init__(self, beta: float, packing_strategy: PackingStrategy,
                 resource_capacity_map: Dict[str, ResourceVector],
                 cpu_weight: float = 1.0, mem_weight: float = 1.0,
                 disk_weight: float = 1.0, total_duration_weight: float = 0.5,
                 task_node_state_map: Dict[str, TaskMapsPerNodeType] = None):
        """
        Initialize CachedTaskPlacer.
        
        Args:
            beta: Power-of-two probability 
                  β > 0: Normal Dodoor/Power-of-Two behavior
                  β = -1.0: Random selection (Random scheduler)
                  β = -2.0: Random selection (Sparrow scheduler - late binding handled elsewhere)
            packing_strategy: Load scoring strategy
            resource_capacity_map: Node capacities
            cpu_weight: CPU resource weight
            mem_weight: Memory resource weight
            disk_weight: Disk resource weight  
            total_duration_weight: Duration weight (α in Dodoor paper)
            task_node_state_map: Task type mappings per node type
        """
        super().__init__(beta, packing_strategy, resource_capacity_map,
                        cpu_weight, mem_weight, disk_weight, total_duration_weight,
                        task_node_state_map)
        
        # Validate configuration like Java does
        if packing_strategy == PackingStrategy.SCORE and (
            cpu_weight == 1 and mem_weight == 1 and disk_weight == 1 and total_duration_weight == 1):
            raise ValueError("PackingStrategy.SCORE requires proper resource weights")
        
        logger.info(f"Initialized CachedTaskPlacer: β={beta}, strategy={packing_strategy}")
    
    def get_enqueue_task_reservation_requests(self, scheduling_request: SchedulingRequest,
                                            load_maps: Dict[str, NodeState],
                                            scheduler_address: str,
                                            round: int = 0) -> List[TaskPlacementRequest]:
        """
        Get task placement decisions using cached node states.

        Implements the exact algorithm from Java CachedTaskPlacer.java:
        1. For each task, select nodes using (1+β)-choice
        2. Score nodes based on packing strategy
        3. Map task types to actual resource requirements
        4. Create placement requests

        Args:
            scheduling_request: Batch of tasks to schedule
            load_maps: Current node states (cached)
            scheduler_address: Scheduler identifier
            round: Reservation round number (for Sparrow multi-round placement)

        Returns:
            List of task placement decisions
        """
        placements = []
        node_addresses = list(load_maps.keys())

        if not node_addresses:
            logger.warning("No nodes available for scheduling")
            return placements

        for task in scheduling_request.tasks:
            # Select node using (1+β)-choice algorithm with round-based seeding
            selected_node_id = self._select_node(task, node_addresses, load_maps, round)
            
            if selected_node_id is None:
                logger.warning(f"No node selected for task {task.task_id}")
                continue
            
            selected_node_state = load_maps[selected_node_id]
            
            # Get actual task resources (handles SIMULATED vs real task types)
            task_resources = self._get_task_resources(task, selected_node_state)
            
            # Create placement request
            TaskPlacer.update_scheduling_results(
                placements, selected_node_id, scheduling_request, 
                task, scheduler_address, task_resources
            )
        
        logger.debug(f"CachedTaskPlacer scheduled {len(placements)} tasks")
        return placements
    
    def _select_node(self, task, node_addresses: List[str],
                    load_maps: Dict[str, NodeState], round: int = 0) -> str:
        """
        Select node using (1+β)-choice algorithm matching Java implementation.

        Args:
            task: Task to schedule
            node_addresses: Available node IDs
            load_maps: Current node states
            round: Reservation round number (combined with task_id for seeding)

        Returns:
            Selected node ID
        """
        # Match Java: Random ran = new Random(taskSpec.taskId.hashCode() + round);
        seed = hash(task.task_id) + round
        ran = random.Random(seed)
        first_index = ran.randint(0, len(node_addresses) - 1)
        
        # Handle special beta values for different schedulers
        if self.beta == -1.0:  # Random scheduler
            return node_addresses[first_index]
        elif self.beta == -2.0:  # Sparrow scheduler (random selection here, late binding elsewhere)
            return node_addresses[first_index]
        elif self.beta > 0:
            # Normal (1+β)-choice: with probability β, do power-of-two selection
            flag = ran.random()
            
            if flag < self.beta and len(node_addresses) > 1:
                # Power-of-two selection
                second_index = first_index
                while second_index == first_index:
                    second_index = ran.randint(0, len(node_addresses) - 1)
                
                node1_id = node_addresses[first_index]
                node2_id = node_addresses[second_index]
                state1 = load_maps[node1_id]
                state2 = load_maps[node2_id]
                
                # Score both nodes
                score1, score2 = self._score_nodes(state1, state2, task)
                
                # Select node with better (lower) score
                if score1 <= score2:
                    return node1_id
                else:
                    return node2_id
            else:
                # Random selection (1-β probability)
                return node_addresses[first_index]
        else:
            # Invalid beta
            logger.warning(f"Invalid beta value: {self.beta}, using random selection")
            return node_addresses[first_index]
    
    def _score_nodes(self, state1: NodeState, state2: NodeState, task) -> tuple:
        """
        Score two nodes based on packing strategy.
        
        Args:
            state1: First node state
            state2: Second node state  
            task: Task being scheduled
            
        Returns:
            Tuple of (score1, score2) - lower is better
        """
        if self.packing_strategy == PackingStrategy.SCORE:
            # Multi-dimensional load scoring (Dodoor)
            return LoadScore.get_load_scores_pairs(
                state1, state2, task.task_type, task.resource_request,
                self.cpu_weight, self.mem_weight, self.disk_weight,
                self.total_duration_weight, self.resource_capacity_map,
                self.task_node_state_map
            )
        
        elif self.packing_strategy == PackingStrategy.RIF:
            # Running in FIFO - use task count (Power-of-Two)
            return state1.num_tasks, state2.num_tasks
        
        elif self.packing_strategy == PackingStrategy.DURATION:
            # Duration-based scoring
            return state1.total_duration_ms, state2.total_duration_ms
        
        elif self.packing_strategy == PackingStrategy.NONE:
            # No optimization - equal scores (Random/Sparrow)
            return 0.0, 0.0
        
        else:
            raise ValueError(f"Unknown packing strategy: {self.packing_strategy}")
    
    @staticmethod
    def create_dodoor_placer(beta: float, resource_capacity_map: Dict[str, ResourceVector],
                           cpu_weight: float, mem_weight: float, disk_weight: float,
                           total_duration_weight: float,
                           task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> 'CachedTaskPlacer':
        """
        Create CachedTaskPlacer for Dodoor scheduler.
        
        Args:
            beta: Power-of-two probability
            resource_capacity_map: Node capacities
            cpu_weight: CPU weight
            mem_weight: Memory weight
            disk_weight: Disk weight
            total_duration_weight: Duration weight (α parameter)
            task_node_state_map: Task mappings
            
        Returns:
            Configured CachedTaskPlacer for Dodoor
        """
        return CachedTaskPlacer(
            beta, PackingStrategy.SCORE, resource_capacity_map,
            cpu_weight, mem_weight, disk_weight, total_duration_weight,
            task_node_state_map
        )
    
    @staticmethod 
    def create_random_placer(resource_capacity_map: Dict[str, ResourceVector],
                           task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> 'CachedTaskPlacer':
        """
        Create CachedTaskPlacer for Random scheduler.
        
        Args:
            resource_capacity_map: Node capacities
            task_node_state_map: Task mappings
            
        Returns:
            Configured CachedTaskPlacer for Random scheduler
        """
        return CachedTaskPlacer(
            -1.0, PackingStrategy.NONE, resource_capacity_map,
            1.0, 1.0, 1.0, 1.0, task_node_state_map
        )
    
    @staticmethod
    def create_sparrow_placer(resource_capacity_map: Dict[str, ResourceVector],
                            task_node_state_map: Dict[str, TaskMapsPerNodeType]) -> 'CachedTaskPlacer':
        """
        Create CachedTaskPlacer for Sparrow scheduler.
        
        Args:
            resource_capacity_map: Node capacities
            task_node_state_map: Task mappings
            
        Returns:
            Configured CachedTaskPlacer for Sparrow scheduler
        """
        return CachedTaskPlacer(
            -2.0, PackingStrategy.NONE, resource_capacity_map,
            1.0, 1.0, 1.0, 1.0, task_node_state_map
        )