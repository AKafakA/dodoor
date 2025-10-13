"""
Unified scheduler using TaskPlacer interface matching Java implementation.

Replaces all individual scheduler implementations with a single unified approach
that uses the TaskPlacer interface, matching the Java physical system exactly.
"""

import logging
import time
from collections import Counter
from statistics import mean
from typing import Any, Dict, List, Optional

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, ResourceVector
    from ..core.events import EventType, Event
    from ..core.task_placer import TaskPlacer, SchedulingRequest, TaskPlacementRequest
    from ..core.packing_strategy import PackingStrategy  
    from ..core.task_maps_per_node_type import TaskMapsPerNodeType
    from ..core.cached_task_placer import CachedTaskPlacer
    from ..core.runtime_probe_task_placer import RunTimeProbeTaskPlacer
    from ..core.prequal_task_placer import PrequalTaskPlacer
    from ..core.resource_utils import ResourceUtils, DodoorDefaults
    # Note: ReservationReplay is optional for most simulations
    try:
        from ..replay.reservation_replay import ReservationReplay
    except ImportError:
        ReservationReplay = None
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, ResourceVector
    from core.events import EventType, Event
    from core.task_placer import TaskPlacer, SchedulingRequest, TaskPlacementRequest
    from core.packing_strategy import PackingStrategy
    from core.task_maps_per_node_type import TaskMapsPerNodeType  
    from core.cached_task_placer import CachedTaskPlacer
    from core.runtime_probe_task_placer import RunTimeProbeTaskPlacer
    from core.prequal_task_placer import PrequalTaskPlacer
    from core.resource_utils import ResourceUtils, DodoorDefaults
    # Note: ReservationReplay is optional for most simulations
    try:
        from replay.reservation_replay import ReservationReplay
    except ImportError:
        ReservationReplay = None

logger = logging.getLogger(__name__)


class UnifiedScheduler(BaseScheduler):
    """
    Unified scheduler implementing Java's TaskPlacer-based approach.
    
    Replaces the fictional message-passing protocols with the actual
    synchronous scheduling logic used in the Java physical system.
    
    Supports all scheduler types through TaskPlacer interface:
    - Dodoor: CachedTaskPlacer with PackingStrategy.SCORE
    - Power-of-Two: CachedTaskPlacer with PackingStrategy.RIF  
    - Random: CachedTaskPlacer with β=-1.0, PackingStrategy.NONE
    - Sparrow: CachedTaskPlacer with β=-2.0, PackingStrategy.NONE
    - Prequal: PrequalTaskPlacer (TODO: implement)
    """
    
    def __init__(self, config, scheduler_id: str = "unified_scheduler"):
        super().__init__(config, scheduler_id)
        
        # Get scheduler type from config
        self.scheduler_type = getattr(config, 'scheduler_type', 'dodoor')
        
        # TaskPlacer configuration with Java defaults
        self.beta = getattr(config, 'beta', DodoorDefaults.DEFAULT_BETA)
        
        # Get resource weights matching Java system
        replay_with_disk = getattr(config, 'replay_with_disk', False)
        weights = ResourceUtils.get_resource_weights(
            config.__dict__ if hasattr(config, '__dict__') else {}, 
            replay_with_disk
        )
        
        self.cpu_weight = weights['cpu_weight']
        self.mem_weight = weights['mem_weight'] 
        self.disk_weight = weights['disk_weight']
        self.duration_weight = weights['duration_weight']
        
        # Late-binding configuration (Sparrow)
        self.late_binding_probe_count = getattr(config, 'late_binding_probe_count', 2)

        # Initialize maps that will be populated during setup
        self.resource_capacity_map: Dict[str, 'ResourceVector'] = {}
        self.task_node_state_map: Dict[str, TaskMapsPerNodeType] = {}
        self.task_profile_config: Optional[dict] = None
        
        # TaskPlacer instance (created after setup)
        self.task_placer: TaskPlacer = None

        # Late-binding state: task_id -> set(node_id)
        self._preserved_nodes: Dict[str, set] = {}
        self._asked_to_execute: Dict[str, set] = {}
        self._task_reservations: Dict[str, Dict[str, TaskPlacementRequest]] = {}
        self._reservation_targets: Dict[str, List[str]] = {}
        self._task_enqueue_time: Dict[str, float] = {}
        self._sparrow_confirm_latencies: List[float] = []
        self._sparrow_confirm_attempts = 0
        self._sparrow_cancellations = 0
        self._sparrow_confirm_rejections: Counter[str] = Counter()

        logger.info(f"Initialized UnifiedScheduler: type={self.scheduler_type}, β={self.beta}")

    def set_task_profile_config(self, profile_config: Optional[dict]):
        """Provide task profile configuration loaded from profiler."""
        self.task_profile_config = profile_config

    def setup_task_placer(self, node_states: Dict[str, NodeState], 
                         task_type_config: dict = None):
        """
        Setup TaskPlacer with node capacity and task type mappings.
        
        Must be called before scheduling to populate resource maps.
        
        Args:
            node_states: Current node states to extract capacities
            task_type_config: Task type configuration (optional)
        """
        # Build resource capacity map from node states (keyed by node_type for LoadScore compatibility)
        self.resource_capacity_map = {}
        node_types = set()

        for node_id, node_state in node_states.items():
            # Key by node_type for LoadScore.get_load_scores_pairs() compatibility
            self.resource_capacity_map[node_state.node_type] = node_state.capacity
            node_types.add(node_state.node_type)
        
        # Build task node state map (simplified for now)
        self.task_node_state_map = {}
        if task_type_config:
            for node_type in node_types:
                self.task_node_state_map[node_type] = TaskMapsPerNodeType.create_from_config(
                    node_type, task_type_config
                )
        else:
            # Create default mappings
            for node_type in node_types:
                task_maps = TaskMapsPerNodeType(node_type)
                # Add default task types
                task_maps.add_task_mapping("default_task", ResourceVector(cores=1.0, memory=1024, disk=0), 1000)
                task_maps.add_task_mapping("simulated", ResourceVector(cores=1.0, memory=1024, disk=0), 1000)
                self.task_node_state_map[node_type] = task_maps
        
        # Create TaskPlacer instance based on scheduler type
        self.task_placer = self._create_task_placer()
        
        logger.info(f"Setup TaskPlacer for {len(self.resource_capacity_map)} nodes, "
                   f"{len(self.task_node_state_map)} node types")

        # Optional replay provider (only used by Sparrow)
        self._replay = None
        replay_file = getattr(self.config, 'replay_reservations_file', None)
        if replay_file and ReservationReplay is not None:
            try:
                self._replay = ReservationReplay(replay_file)
                logger.info(f"Loaded reservation replay file: {replay_file}")
            except Exception as e:
                logger.error(f"Failed to load replay file {replay_file}: {e}")
                self._replay = None
        # Expected confirm map for divergence checks
        self._replay_expected_confirm: dict[str, str] = {}
    
    def _create_task_placer(self) -> TaskPlacer:
        """Create appropriate TaskPlacer based on scheduler type matching Java system exactly."""
        
        # Exact mapping from Java TaskPlacer.createTaskPlacer switch statement
        if self.scheduler_type == "dodoor":
            # Java: case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, PackingStrategy.SCORE, ...)
            return CachedTaskPlacer(
                1.0,  # Java uses beta=1.0 from debug.sh for ALL schedulers
                PackingStrategy.SCORE,  # Dodoor uses sophisticated load scoring
                self.resource_capacity_map,
                self.cpu_weight, self.mem_weight, self.disk_weight,
                self.duration_weight, self.task_node_state_map
            )
        
        elif self.scheduler_type == "powerOfTwo":  # Note: Java uses camelCase
            # Java: case DodoorConf.POWER_OF_TWO_SCHEDULER -> new RunTimeProbeTaskPlacer(beta, PackingStrategy.RIF, ...)
            return RunTimeProbeTaskPlacer(
                1.0,  # Java uses beta=1.0
                PackingStrategy.RIF,  # Power-of-Two uses RIF (task count based)
                self.resource_capacity_map,
                node_probe_callback=self._get_node_probe_callback(),  # Runtime probing - should add overhead
                task_node_state_map=self.task_node_state_map
            )
        
        elif self.scheduler_type == "power_of_two":  # CLI snake_case compatibility
            # Same as powerOfTwo, just snake_case variant for CLI
            return RunTimeProbeTaskPlacer(
                1.0,  # Java uses beta=1.0
                PackingStrategy.RIF,  # Power-of-Two uses RIF (task count based)
                self.resource_capacity_map,
                node_probe_callback=self._get_node_probe_callback(),  # Runtime probing - should add overhead
                task_node_state_map=self.task_node_state_map
            )
        
        elif self.scheduler_type == "cachedPowerOfTwo":  # Note: Java uses camelCase
            # Java: case DodoorConf.CACHED_POWER_OF_TWO_SCHEDULER -> new CachedTaskPlacer(beta, PackingStrategy.RIF, ...)
            return CachedTaskPlacer(
                1.0,  # Java uses beta=1.0
                PackingStrategy.RIF,  # Cached Power-of-Two uses RIF
                self.resource_capacity_map,
                self.cpu_weight, self.mem_weight, self.disk_weight,
                self.duration_weight, self.task_node_state_map
            )
        
        elif self.scheduler_type == "sparrow":
            # Java: case DodoorConf.SPARROW_SCHEDULER -> new CachedTaskPlacer(-2.0, PackingStrategy.NONE, ...)
            return CachedTaskPlacer(
                -2.0,  # Java hardcodes -2.0 for Sparrow (random selection)
                PackingStrategy.NONE,  # No sophisticated packing
                self.resource_capacity_map,
                self.cpu_weight, self.mem_weight, self.disk_weight,
                self.duration_weight, self.task_node_state_map
            )
        
        elif self.scheduler_type == "prequal":
            # Java: case DodoorConf.PREQUAL -> new PrequalTaskPlacer(beta, ...)
            return PrequalTaskPlacer(
                1.0,  # Java uses beta=1.0 for Prequal
                self.resource_capacity_map,
                rif_quantile=0.84,  # Java defaults from DodoorConf
                probe_pool_size=16,
                delta=1,
                probe_rate=3,
                probe_delete_rate=1,
                probe_age_budget_ms=1000,
                task_node_state_map=self.task_node_state_map
            )
        
        elif self.scheduler_type == "random":
            # Java: case DodoorConf.RANDOM_SCHEDULER -> new CachedTaskPlacer(-1.0, PackingStrategy.NONE, ...)
            return CachedTaskPlacer(
                -1.0,  # Java uses -1.0 for random
                PackingStrategy.NONE,  # No packing strategy
                self.resource_capacity_map,
                task_node_state_map=self.task_node_state_map
            )
        
        else:
            raise ValueError(f"Unknown scheduler type: {self.scheduler_type}")

    def confirm_task_ready(self, task_id: str, node_id: str, current_time: float) -> tuple[bool, list[str]]:
        """Sparrow confirm handshake: allow first confirming node and cancel others.

        Returns (confirmed, nodes_to_cancel)
        """
        if self.scheduler_type != "sparrow":
            return False, []

        self._sparrow_confirm_attempts += 1
        preserved = self._preserved_nodes.get(task_id)

        logger.info(f"DEBUG CONFIRM: task={task_id}, node={node_id}, preserved={preserved}, has_entry={task_id in self._preserved_nodes}")

        if not preserved:
            # Not preserved or already confirmed
            self._sparrow_confirm_rejections['missing_reservation'] += 1
            logger.info(f"DEBUG CONFIRM REJECT: task={task_id}, node={node_id}, reason=missing_reservation")
            return False, []

        if node_id not in preserved:
            self._sparrow_confirm_rejections['node_not_preserved'] += 1
            logger.info(f"DEBUG CONFIRM REJECT: task={task_id}, node={node_id}, reason=node_not_in_preserved, preserved={preserved}")
            return False, []

        # Mark asked to execute
        self._asked_to_execute.setdefault(task_id, set()).add(node_id)

        # Confirm this node, cancel others
        preserved.discard(node_id)
        nodes_to_cancel = list(preserved)
        # Cleanup
        self._preserved_nodes.pop(task_id, None)
        self._sparrow_cancellations += len(nodes_to_cancel)

        logger.info(f"DEBUG CONFIRM SUCCESS: task={task_id}, node={node_id}, nodes_to_cancel={nodes_to_cancel}")

        enqueue_time = self._task_enqueue_time.pop(task_id, None)
        if enqueue_time is not None:
            latency_ms = max(0.0, current_time - enqueue_time)
            self._sparrow_confirm_latencies.append(latency_ms)

        reservations = self._task_reservations.pop(task_id, None)
        if reservations:
            reservations.pop(node_id, None)
            for cancel_node in nodes_to_cancel:
                reservations.pop(cancel_node, None)
        self._reservation_targets.pop(task_id, None)

        return True, nodes_to_cancel

    def get_statistics(self) -> Dict[str, Any]:
        """Extend base statistics with Sparrow-specific runtime data."""
        stats = super().get_statistics()

        if self.scheduler_type == 'sparrow':
            confirm_count = len(self._sparrow_confirm_latencies)
            avg_latency = mean(self._sparrow_confirm_latencies) if confirm_count else 0.0
            if confirm_count:
                sorted_latencies = sorted(self._sparrow_confirm_latencies)
                p95_index = min(confirm_count - 1, int(round(0.95 * (confirm_count - 1))))
                p95_latency = sorted_latencies[p95_index]
            else:
                p95_latency = 0.0
            pending_fanout = (
                mean(len(targets) for targets in self._reservation_targets.values())
                if self._reservation_targets else 0.0
            )

            stats['sparrow_runtime'] = {
                'pending_reservations': len(self._preserved_nodes),
                'active_reservation_tasks': len(self._task_reservations),
                'avg_pending_reservation_fanout': pending_fanout,
                'confirm_attempts': self._sparrow_confirm_attempts,
                'confirm_success': confirm_count,
                'confirm_rejections': dict(self._sparrow_confirm_rejections),
                'avg_confirm_latency_ms': avg_latency,
                'p95_confirm_latency_ms': p95_latency,
                'cancellation_messages': self._sparrow_cancellations,
            }

        return stats
    
    def _get_node_probe_callback(self):
        """
        Get callback function for runtime node probing.
        
        This callback allows RunTimeProbeTaskPlacer to get fresh node state
        during scheduling decisions, simulating the network calls in Java.
        
        Returns:
            Function that takes node_id and returns fresh NodeState
        """
        def probe_node(node_id: str):
            # In a full implementation, this would query the simulation engine
            # for fresh state from the node executors. For now, return None
            # to fall back to cached state.
            return None
        
        return probe_node
    
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """
        Schedule tasks using TaskPlacer interface with distributed scheduling overhead modeling.

        Models the message-passing delays that differentiate scheduler performance:
        - Dodoor: Cached decisions → minimal network overhead
        - PowerOfTwo: Runtime probing → multiple probe messages + delays
        - Prequal: Probe pool management → variable probe overhead
        - Sparrow: Late binding → highest message complexity
        """
        logger.info(f"DEBUG SCHEDULE_TASKS ENTRY: tasks={[t.task_id for t in tasks]}, time={current_time}")
        # Special path for Sparrow: multi-reservation + late binding
        if self.scheduler_type == 'sparrow':
            if self.task_placer is None:
                self.setup_task_placer(node_states, self.task_profile_config)
                self.task_placer = self._create_task_placer()

            events: List[Event] = []
            for task in tasks:
                # Reset per-task bookkeeping before issuing new probes
                self._task_reservations.pop(task.task_id, None)
                self._reservation_targets.pop(task.task_id, None)
                self._preserved_nodes.pop(task.task_id, None)
                self._task_enqueue_time.pop(task.task_id, None)

                # Java behavior: multi-round placement with round-based seeding
                # Matches Java SchedulerImpl.java:237-248
                preserved_nodes = set()
                if self._replay:
                    nodes = self._replay.get_preserved(task.task_id, self.late_binding_probe_count)
                    preserved_nodes.update(nodes)
                else:
                    # Use round parameter to ensure different nodes selected each round
                    # Matches Java: Random ran = new Random(taskSpec.taskId.hashCode() + round);
                    logger.info(f"DEBUG SPARROW PLACEMENT: task={task.task_id}, starting {self.late_binding_probe_count} rounds")
                    for round_num in range(self.late_binding_probe_count):
                        req = SchedulingRequest([task])
                        placements = self.task_placer.get_enqueue_task_reservation_requests(
                            req, node_states, self.scheduler_id, round=round_num
                        )
                        if not placements:
                            logger.warning(f"No placement returned for task {task.task_id} round {round_num}")
                            break
                        node_id = placements[0].assigned_node_id
                        was_duplicate = node_id in preserved_nodes
                        preserved_nodes.add(node_id)
                        logger.info(f"DEBUG SPARROW ROUND {round_num}: task={task.task_id}, node={node_id}, duplicate={was_duplicate}")

                # Track reservation bookkeeping and emit enqueues to each preserved node
                logger.info(f"DEBUG SPARROW EMIT: task={task.task_id}, preserved_nodes={preserved_nodes}, count={len(preserved_nodes)}")
                for node_id in preserved_nodes:
                    # Bookkeeping
                    placement = TaskPlacementRequest(task, node_id, task.resource_request)
                    self._task_reservations.setdefault(task.task_id, {})[node_id] = placement
                    self._reservation_targets.setdefault(task.task_id, []).append(node_id)
                    self._task_enqueue_time.setdefault(task.task_id, current_time)

                    # Network overhead modeling
                    scheduling_overhead_ms, messages_per_task = self._calculate_scheduling_overhead(1)
                    delayed_timestamp = current_time + scheduling_overhead_ms
                    event = Event(
                        event_id=self._generate_event_id(),
                        timestamp=delayed_timestamp,
                        event_type=EventType.TASK_SCHEDULED,
                        source_id=self.scheduler_id,
                        target_id=node_id,
                        data={
                            'task': task,
                            'task_resources': task.resource_request,
                            'assignment_method': 'sparrow',
                            'scheduling_overhead_ms': scheduling_overhead_ms,
                            'network_messages': messages_per_task
                        }
                    )
                    events.append(event)
                    logger.info(f"DEBUG SPARROW EVENT: task={task.task_id}, node={node_id}, event_id={event.event_id}, timestamp={delayed_timestamp}")
                if preserved_nodes:
                    self._preserved_nodes[task.task_id] = preserved_nodes
                    # Record expected confirm if provided
                    if self._replay:
                        exp = self._replay.expected_confirm(task.task_id)
                        if exp:
                            self._replay_expected_confirm[task.task_id] = exp
                    logger.info(f"DEBUG SPARROW STORE: task={task.task_id}, stored preserved_nodes={preserved_nodes}, count={len(preserved_nodes)}")
                else:
                    logger.warning(
                        f"Sparrow scheduler could not find reservation targets for task {task.task_id}"
                    )

            self.update_statistics(len(tasks), 0.0)
            return events

        # Default path for non-Sparrow schedulers
        start_time = time.perf_counter()
        if self.task_placer is None:
            self.setup_task_placer(node_states, self.task_profile_config)
            self.task_placer = self._create_task_placer()

        scheduling_request = SchedulingRequest(tasks)
        placement_requests = self.task_placer.get_enqueue_task_reservation_requests(
            scheduling_request, node_states, self.scheduler_id
        )
        scheduling_overhead_ms, messages_per_task = self._calculate_scheduling_overhead(len(tasks))

        events = []
        for placement in placement_requests:
            selected_node = node_states[placement.assigned_node_id]
            selected_node.allocate_task(placement.task)
            delayed_timestamp = current_time + scheduling_overhead_ms
            events.append(Event(
                event_id=self._generate_event_id(),
                timestamp=delayed_timestamp,
                event_type=EventType.TASK_SCHEDULED,
                source_id=self.scheduler_id,
                target_id=placement.assigned_node_id,
                data={
                    'task': placement.task,
                    'task_resources': placement.task_resources,
                    'assignment_method': self.scheduler_type,
                    'scheduling_overhead_ms': scheduling_overhead_ms,
                    'network_messages': messages_per_task
                }
            ))

        scheduling_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), scheduling_time_ms)
        logger.debug(f"UnifiedScheduler ({self.scheduler_type}) scheduled {len(events)} tasks in {scheduling_time_ms:.2f}ms")
        return events
    
    def _calculate_scheduling_overhead(self, num_tasks: int) -> tuple[float, int]:
        """
        Calculate scheduling overhead and network messages based on scheduler type.
        
        Models the distributed protocols that create performance differences:
        - Dodoor: Cached state → 1 message per task, ~2ms network delay
        - PowerOfTwo: Runtime probing → 5 messages per task, ~10ms delay  
        - Prequal: Probe pool → 2-4 messages per task, ~6ms delay
        - Sparrow: Late binding → 6-8 messages per task, ~15ms delay
        
        Args:
            num_tasks: Number of tasks being scheduled
            
        Returns:
            tuple: (scheduling_overhead_ms, total_network_messages)
        """
        # Base network latency from configuration (mean=2ms, std=0.5ms)
        try:
            base_network_delay = float(getattr(self.config.cluster.network, 'mean_latency_ms', 2.0))
        except Exception:
            base_network_delay = 2.0
        
        if self.scheduler_type in ["dodoor"]:
            # Cached scheduling: 1 assignment message per task
            messages_per_task = 1
            # Single network hop for task assignment
            overhead_per_task = base_network_delay * 1
            
        elif self.scheduler_type in ["power_of_two", "powerOfTwo"]:
            # Runtime probing: 2 probes + 2 responses + 1 assignment = 5 messages
            messages_per_task = 5
            # Multiple network hops: probe(2ms) + wait + response(2ms) + assign(2ms)
            overhead_per_task = base_network_delay * 5
            
        elif self.scheduler_type in ["prequal"]:
            # Probe pool management: some cached, some new probes
            # Average: 1-2 new probes + 1-2 responses + 1 assignment = 3 messages
            messages_per_task = 3
            # Moderate overhead: some cache hits reduce latency
            overhead_per_task = base_network_delay * 3
            
        elif self.scheduler_type in ["sparrow"]:
            # Late binding: multiple probes + queuing + cancellation
            # Probe multiple nodes + queue + cancel unused = 7 messages average
            messages_per_task = 7
            # Highest overhead due to coordination complexity
            overhead_per_task = base_network_delay * 6
            
        else:
            # Default/unknown scheduler
            messages_per_task = 2
            overhead_per_task = base_network_delay * 2
        
        # Allow explicit overrides from scheduler config (per-scheduler calibration)
        try:
            if getattr(self.config.scheduler, 'messages_per_task_override', None) is not None:
                messages_per_task = int(self.config.scheduler.messages_per_task_override)
            if getattr(self.config.scheduler, 'overhead_ms_override', None) is not None:
                overhead_per_task = float(self.config.scheduler.overhead_ms_override)
        except Exception:
            pass

        total_messages = messages_per_task * num_tasks
        total_overhead_ms = overhead_per_task  # Assuming parallel processing of tasks
        
        return total_overhead_ms, total_messages
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        import random
        return random.randint(1000000, 9999999)
    
    def get_scheduler_type(self) -> str:
        """Get the scheduler type for identification."""
        return self.scheduler_type

    # --- Replay helpers for divergence checks ---
    def get_expected_confirm(self, task_id: str) -> str | None:
        return self._replay_expected_confirm.get(task_id)

    def clear_expected_confirm(self, task_id: str) -> None:
        self._replay_expected_confirm.pop(task_id, None)
    
    def update_resource_capacity_map(self, node_states: Dict[str, NodeState]):
        """Update resource capacity map when node states change."""
        for node_id, node_state in node_states.items():
            # Key by node_type for LoadScore.get_load_scores_pairs() compatibility
            self.resource_capacity_map[node_state.node_type] = node_state.capacity

    


# Factory functions for specific scheduler types

def create_dodoor_scheduler(config, scheduler_id: str = "dodoor_scheduler") -> UnifiedScheduler:
    """Create UnifiedScheduler configured for Dodoor."""
    config.scheduler_type = "dodoor"
    return UnifiedScheduler(config, scheduler_id)


def create_power_of_two_scheduler(config, scheduler_id: str = "power_of_two_scheduler") -> UnifiedScheduler:
    """Create UnifiedScheduler configured for Power-of-Two.""" 
    config.scheduler_type = "power_of_two"
    return UnifiedScheduler(config, scheduler_id)


def create_random_scheduler(config, scheduler_id: str = "random_scheduler") -> UnifiedScheduler:
    """Create UnifiedScheduler configured for Random."""
    config.scheduler_type = "random"
    return UnifiedScheduler(config, scheduler_id)


def create_sparrow_scheduler(config, scheduler_id: str = "sparrow_scheduler") -> UnifiedScheduler:
    """Create UnifiedScheduler configured for Sparrow."""
    config.scheduler_type = "sparrow"
    return UnifiedScheduler(config, scheduler_id)


def create_prequal_scheduler(config, scheduler_id: str = "prequal_scheduler") -> UnifiedScheduler:
    """Create UnifiedScheduler configured for Prequal."""
    config.scheduler_type = "prequal"
    return UnifiedScheduler(config, scheduler_id)
