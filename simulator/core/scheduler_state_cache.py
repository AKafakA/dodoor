"""
Scheduler-side state caching implementation matching Java SchedulerImpl behavior.

This module implements the cached node state management that schedulers use for
placement decisions, with realistic staleness from DataStore batch updates.
"""

import logging
from typing import Dict, Optional, List
from dataclasses import dataclass
import time

try:
    from ..schedulers.base_scheduler import NodeState, ResourceVector, Task
    from .datastore_service import NodeLoadUpdate
except ImportError:
    from schedulers.base_scheduler import NodeState, ResourceVector, Task
    from core.datastore_service import NodeLoadUpdate

logger = logging.getLogger(__name__)


@dataclass
class PendingUpdate:
    """Pending update to send to DataStore."""
    node_id: str
    resource_change: ResourceVector
    num_tasks_change: int
    total_duration_change: float
    timestamp: float


class SchedulerStateCache:
    """
    Scheduler-side state cache matching Java SchedulerImpl._loadMapEqueueSocketToNodeState.

    Key behaviors:
    1. Maintains cached node states (potentially stale)
    2. Accumulates local placement decisions
    3. Sends batch updates to DataStore every N tasks
    4. Receives periodic state updates from DataStore
    """

    def __init__(self, scheduler_id: str, scheduler_num_tasks_to_update: int = 4):
        """Initialize scheduler state cache.

        Args:
            scheduler_id: Unique identifier for this scheduler
            scheduler_num_tasks_to_update: How often to send updates to DataStore
        """
        self.scheduler_id = scheduler_id
        self.scheduler_num_tasks_to_update = scheduler_num_tasks_to_update

        # Cached node states (matches Java _loadMapEqueueSocketToNodeState)
        self._cached_node_states: Dict[str, NodeState] = {}

        # Pending updates to send to DataStore (matches Java _nodeLoadChanges)
        self._pending_updates: Dict[str, PendingUpdate] = {}

        # Task counter for batch triggering (matches Java _counter)
        self._scheduled_task_counter = 0

        # DataStore update callback (set by simulation engine)
        self._datastore_update_callback: Optional[callable] = None

        logger.info(f"Initialized scheduler cache {scheduler_id}: "
                   f"update_freq={scheduler_num_tasks_to_update}")

    def set_datastore_callback(self, callback: callable):
        """Set callback to send updates to DataStore."""
        self._datastore_update_callback = callback

    def update_cached_states(self, node_states: Dict[str, NodeState]):
        """
        Update cached states from DataStore broadcast (matches Java updateNodeState).

        This is called when DataStore sends batch updates to all schedulers.
        """
        for node_id, state in node_states.items():
            if node_id in self._cached_node_states:
                logger.debug(f"Updating cached state for node {node_id}")
            else:
                logger.debug(f"Adding new cached state for node {node_id}")

            # Deep copy to prevent external modifications
            self._cached_node_states[node_id] = NodeState(
                node_id=state.node_id,
                node_type=state.node_type,
                capacity=ResourceVector(
                    cores=state.capacity.cores,
                    memory=state.capacity.memory,
                    disk=state.capacity.disk
                ),
                allocated=ResourceVector(
                    cores=state.allocated.cores,
                    memory=state.allocated.memory,
                    disk=state.allocated.disk
                ),
                num_tasks=state.num_tasks,
                total_duration_ms=state.total_duration_ms,
                queue_length=state.queue_length,
                last_update_time=state.last_update_time
            )

        logger.debug(f"Updated cached states for {len(node_states)} nodes")

    def get_cached_states(self) -> Dict[str, NodeState]:
        """Get current cached node states for scheduling decisions."""
        return self._cached_node_states.copy()

    def record_task_placement(self, node_id: str, task: Task, current_time: float):
        """
        Record a task placement decision (matches Java updateDataStoreLoad).

        This accumulates local placement decisions to send to DataStore in batches.
        """
        self._scheduled_task_counter += 1

        # Initialize pending update for this node if needed
        if node_id not in self._pending_updates:
            self._pending_updates[node_id] = PendingUpdate(
                node_id=node_id,
                resource_change=ResourceVector(),
                num_tasks_change=0,
                total_duration_change=0.0,
                timestamp=current_time
            )

        # Accumulate resource changes (matches Java lines 344-347)
        pending = self._pending_updates[node_id]
        pending.resource_change.cores += task.resource_request.cores
        pending.resource_change.memory += task.resource_request.memory
        pending.resource_change.disk += task.resource_request.disk
        pending.num_tasks_change += 1
        pending.total_duration_change += task.duration_ms
        pending.timestamp = current_time

        # Update local cached state immediately for scheduling decisions
        if node_id in self._cached_node_states:
            cached_state = self._cached_node_states[node_id]
            cached_state.allocated.cores += task.resource_request.cores
            cached_state.allocated.memory += task.resource_request.memory
            cached_state.allocated.disk += task.resource_request.disk
            cached_state.num_tasks += 1
            cached_state.total_duration_ms += task.duration_ms
            cached_state.last_update_time = current_time * 1000

        logger.debug(f"Recorded placement: task {task.task_id} → node {node_id} "
                    f"(counter: {self._scheduled_task_counter})")

        # Check if we need to send batch update to DataStore
        if self._should_send_datastore_update():
            self._send_datastore_update(current_time)

    def record_task_completion(self, node_id: str, task: Task, current_time: float):
        """Record a task completion to update local cached state."""
        # Update local cached state for completion
        if node_id in self._cached_node_states:
            cached_state = self._cached_node_states[node_id]
            cached_state.allocated.cores -= task.resource_request.cores
            cached_state.allocated.memory -= task.resource_request.memory
            cached_state.allocated.disk -= task.resource_request.disk
            cached_state.num_tasks -= 1
            cached_state.total_duration_ms -= task.duration_ms
            cached_state.last_update_time = current_time * 1000

        logger.debug(f"Recorded completion: task {task.task_id} completed on node {node_id}")

    def _should_send_datastore_update(self) -> bool:
        """Check if we should send batch update to DataStore (matches Java logic)."""
        # Matches Java: numTasksBefore / _numTasksToUpdateDataStore != _counter.get() / _numTasksToUpdateDataStore
        tasks_before = self._scheduled_task_counter - 1
        tasks_after = self._scheduled_task_counter

        return (tasks_before // self.scheduler_num_tasks_to_update !=
                tasks_after // self.scheduler_num_tasks_to_update)

    def _send_datastore_update(self, current_time: float):
        """Send batch update to DataStore (matches Java updateDataStoreLoad)."""
        if not self._pending_updates or not self._datastore_update_callback:
            return

        # Convert pending updates to DataStore format
        datastore_updates = {}
        for node_id, pending in self._pending_updates.items():
            # Get node type from cached state
            node_type = "unknown"
            if node_id in self._cached_node_states:
                node_type = self._cached_node_states[node_id].node_type

            datastore_updates[node_id] = NodeLoadUpdate(
                node_id=node_id,
                node_type=node_type,
                resource_change=pending.resource_change,
                num_tasks_change=pending.num_tasks_change,
                total_duration_change=pending.total_duration_change,
                sign=1,  # +1 for scheduler placements
                update_source=self.scheduler_id,
                timestamp=current_time
            )

        logger.debug(f"Sending batch update to DataStore: {len(datastore_updates)} nodes, "
                    f"{self._scheduled_task_counter} tasks scheduled")

        # Send to DataStore
        self._datastore_update_callback(datastore_updates, current_time)

        # Reset pending updates (matches Java resetNodeLoadChanges)
        self._pending_updates.clear()

    def get_statistics(self) -> Dict[str, any]:
        """Get cache statistics for debugging."""
        return {
            'scheduler_id': self.scheduler_id,
            'cached_nodes': len(self._cached_node_states),
            'pending_updates': len(self._pending_updates),
            'scheduled_tasks': self._scheduled_task_counter,
            'update_frequency': self.scheduler_num_tasks_to_update,
            'next_update_in': (self.scheduler_num_tasks_to_update -
                             (self._scheduled_task_counter % self.scheduler_num_tasks_to_update))
        }