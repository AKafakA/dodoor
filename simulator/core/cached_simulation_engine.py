"""
Phase 4 simulation engine with DataStore cached state architecture.

This demonstrates how the simulation engine integrates with DataStore and
scheduler caches to create realistic staleness patterns.
"""

import logging
from typing import Dict, List, Optional
import time

try:
    from .simulation_engine import SimulationEngine
    from .datastore_service import DataStoreService, NodeLoadUpdate
    from .scheduler_state_cache import SchedulerStateCache
    from .events import Event, EventType
    from ..schedulers.base_scheduler import NodeState, Task
except ImportError:
    from core.simulation_engine import SimulationEngine
    from core.datastore_service import DataStoreService, NodeLoadUpdate
    from core.scheduler_state_cache import SchedulerStateCache
    from core.events import Event, EventType
    from schedulers.base_scheduler import NodeState, Task

logger = logging.getLogger(__name__)


class CachedSimulationEngine(SimulationEngine):
    """
    Simulation engine with Phase 4 cached state architecture.

    Key differences from base SimulationEngine:
    1. Uses DataStore service for centralized state management
    2. Schedulers operate on cached (potentially stale) states
    3. Bidirectional update flow: Schedulers↔DataStore↔Nodes
    4. Realistic batch update frequencies
    """

    def __init__(self, config, output_dir):
        super().__init__(config, output_dir)

        # Phase 4: DataStore service
        batch_size = getattr(config.scheduler, 'batch_size', 50)
        scheduler_update_freq = getattr(config.scheduler, 'scheduler_num_tasks_to_update', 4)

        self.datastore = DataStoreService(
            batch_size=batch_size,
            scheduler_update_frequency=scheduler_update_freq
        )

        # Phase 4: Scheduler state cache
        self.scheduler_cache = SchedulerStateCache(
            scheduler_id="main_scheduler",
            scheduler_num_tasks_to_update=scheduler_update_freq
        )

        # Connect scheduler cache to DataStore
        self.scheduler_cache.set_datastore_callback(self._send_to_datastore)
        self.datastore.register_scheduler(self.scheduler_cache.update_cached_states)

        logger.info("Initialized Phase 4 cached simulation engine")

    def initialize_nodes(self):
        """Initialize nodes and register them with DataStore."""
        super().initialize_nodes()

        # Register all nodes with DataStore
        for node_id, executor in self.node_executors.items():
            node_state = executor.get_node_state()
            self.datastore.register_node(
                node_id=node_id,
                node_type=node_state.node_type,
                capacity=node_state.capacity
            )

        logger.info(f"Registered {len(self.node_executors)} nodes with DataStore")

        # Initialize scheduler cache with initial states from DataStore
        initial_states = self.datastore.get_node_states()
        self.scheduler_cache.update_cached_states(initial_states)
        logger.info(f"Initialized scheduler cache with {len(initial_states)} node states")

    def _process_task_submission_event(self, event: Event):
        """
        Process task submission using cached states (Phase 4 behavior).

        CRITICAL CHANGE: Schedulers now get cached states from DataStore,
        not real-time perfect states from nodes.
        """
        tasks = event.data.get('tasks', [])
        if not tasks:
            return []

        logger.debug(f"Processing task submission: {len(tasks)} tasks at {event.timestamp}")

        # Phase 4: Get CACHED states from scheduler, not real-time states from nodes
        cached_node_states = self.scheduler_cache.get_cached_states()

        if not cached_node_states:
            logger.warning("No cached node states available, using empty states")
            cached_node_states = {}

        # Schedule tasks using cached (potentially stale) states
        events = self.scheduler.schedule_tasks(tasks, cached_node_states, event.timestamp)

        # Record scheduler decisions for DataStore updates
        for task in tasks:
            # Find which node this task was assigned to (simplified for demo)
            assigned_node = self._find_assigned_node(task, events)
            if assigned_node:
                self.scheduler_cache.record_task_placement(
                    node_id=assigned_node,
                    task=task,
                    current_time=event.timestamp
                )

        logger.debug(f"Scheduled {len(tasks)} tasks using cached states")
        return events

    def _process_task_completed_event(self, event: Event):
        """Process task completion with DataStore updates."""
        task_id = event.data.get('task_id')
        node_id = event.target_id

        if not task_id or not node_id:
            logger.error(f"Invalid task completion event: {event}")
            return []

        # Process completion in node executor (existing logic)
        events = super()._process_task_completed_event(event)

        # Phase 4: Send completion update to DataStore
        if node_id in self.node_executors:
            # Get completed task info (simplified - would normally track this)
            completed_task = self._get_completed_task_info(task_id)
            if completed_task:
                # Send completion update to DataStore via node
                self._send_node_completion_to_datastore(node_id, completed_task, event.timestamp)

        return events

    def _process_datastore_broadcast_event(self, event: Event):
        """Process DataStore broadcast to scheduler (Phase 4 new event type)."""
        node_states = event.data.get('node_states', {})
        callback = event.data.get('callback')

        if callback:
            callback(node_states)
            logger.debug(f"DataStore broadcast processed: {len(node_states)} node states updated")

        return []

    def _send_to_datastore(self, node_updates: Dict[str, NodeLoadUpdate], current_time: float):
        """Send scheduler updates to DataStore."""
        events = self.datastore.add_node_loads(node_updates, current_time)

        # Schedule any resulting broadcast events
        for event in events:
            self.event_scheduler.schedule_event(event)

        logger.debug(f"Sent {len(node_updates)} updates to DataStore, "
                    f"generated {len(events)} broadcast events")

    def _send_node_completion_to_datastore(self, node_id: str, task: Task, current_time: float):
        """Send node completion update to DataStore."""
        # Create completion update (sign=-1 for removal)
        completion_update = {
            node_id: NodeLoadUpdate(
                node_id=node_id,
                node_type=self.node_executors[node_id].get_node_state().node_type,
                resource_change=task.resource_request,
                num_tasks_change=1,
                total_duration_change=task.duration_ms,
                sign=-1,  # Completion removes load
                update_source="node",
                timestamp=current_time
            )
        }

        events = self.datastore.add_node_loads(completion_update, current_time)

        # Schedule any resulting broadcast events
        for event in events:
            self.event_scheduler.schedule_event(event)

    def _find_assigned_node(self, task: Task, events: List[Event]) -> Optional[str]:
        """Find which node a task was assigned to (simplified logic)."""
        for event in events:
            if (event.event_type == EventType.TASK_SCHEDULED and
                event.data and event.data.get('task_id') == task.task_id):
                return event.target_id
        return None

    def _get_completed_task_info(self, task_id: str) -> Optional[Task]:
        """Get completed task information (simplified - would normally track this)."""
        # In a real implementation, we'd track task assignments
        # For now, return a dummy task for demonstration
        from ..schedulers.base_scheduler import ResourceVector
        return Task(
            task_id=task_id,
            task_type="demo",
            resource_request=ResourceVector(cores=1, memory=1024, disk=0),
            duration_ms=1000,
            submission_time=time.time() * 1000
        )

    def process_event(self, event: Event) -> List[Event]:
        """Process events with Phase 4 DataStore support."""
        if event.event_type == EventType.DATASTORE_BROADCAST:
            return self._process_datastore_broadcast_event(event)
        else:
            return super().process_event(event)

    def get_phase4_statistics(self) -> Dict[str, any]:
        """Get Phase 4 specific statistics."""
        return {
            'datastore_stats': self.datastore.get_statistics(),
            'scheduler_cache_stats': self.scheduler_cache.get_statistics(),
            'cached_vs_realtime': {
                'cached_nodes': len(self.scheduler_cache.get_cached_states()),
                'realtime_nodes': len(self.node_executors),
                'staleness_updates': self.datastore.get_statistics()['current_staleness']
            }
        }