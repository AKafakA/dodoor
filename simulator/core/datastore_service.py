"""
DataStore service implementation matching Java BasicDataStoreImpl.

This module implements the critical centralized state management with batch updates
that exists in the physical Java system but was missing from the Python simulator.

Key Features:
- Bidirectional updates (Schedulers→DataStore, Nodes→DataStore)
- Batch broadcasting (every batch_size tasks)
- Centralized node state aggregation
- Realistic staleness patterns
"""

import logging
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from collections import defaultdict
import time

try:
    from ..schedulers.base_scheduler import NodeState, ResourceVector
    from .events import Event, EventType
except ImportError:
    from schedulers.base_scheduler import NodeState, ResourceVector
    from core.events import Event, EventType

logger = logging.getLogger(__name__)


@dataclass
class NodeLoadUpdate:
    """Node load update from scheduler or node (matches Java TNodeState)."""
    node_id: str
    node_type: str
    resource_change: ResourceVector  # Delta to apply
    num_tasks_change: int           # Delta to apply
    total_duration_change: float    # Delta to apply
    sign: int                       # +1 for additions, -1 for removals
    update_source: str              # "scheduler" or "node"
    timestamp: float


class DataStoreService:
    """
    DataStore service matching Java BasicDataStoreImpl and DataStoreThrift behavior.

    Manages centralized node state aggregation with bidirectional batch updates:
    1. Schedulers → DataStore: Batch placement decisions (every scheduler_num_tasks_to_update)
    2. Nodes → DataStore: Real-time task completion notifications
    3. DataStore → Schedulers: Batch state broadcasts (every batch_size updates)

    This creates realistic staleness patterns that differentiate scheduler performance.
    """

    def __init__(self, batch_size: int = 50, scheduler_update_frequency: int = 4):
        """Initialize DataStore service.

        Args:
            batch_size: DataStore broadcasts to schedulers every N tasks (matches Java _batchSize)
            scheduler_update_frequency: How often schedulers send updates (matches _numTasksToUpdateDataStore)
        """
        self.batch_size = batch_size
        self.scheduler_update_frequency = scheduler_update_frequency

        # Centralized node state storage (matches Java _nodeStates)
        self._node_states: Dict[str, NodeState] = {}

        # Update counters for batch triggering
        self._total_updates_received = 0
        self._last_broadcast_count = 0

        # Registered schedulers to broadcast updates to
        self._registered_schedulers: List[Callable[[Dict[str, NodeState]], None]] = []

        # Update history for debugging
        self._update_history: List[NodeLoadUpdate] = []

        logger.info(f"Initialized DataStore: batch_size={batch_size}, "
                   f"scheduler_update_freq={scheduler_update_frequency}")

    def register_scheduler(self, scheduler_update_callback: Callable[[Dict[str, NodeState]], None]):
        """Register a scheduler to receive state updates.

        Args:
            scheduler_update_callback: Function to call with updated node states
        """
        self._registered_schedulers.append(scheduler_update_callback)
        logger.debug(f"Registered scheduler, total: {len(self._registered_schedulers)}")

    def register_node(self, node_id: str, node_type: str, capacity: ResourceVector):
        """Register a node with initial empty state (matches Java registerNode)."""
        initial_state = NodeState(
            node_id=node_id,
            node_type=node_type,
            capacity=capacity,
            allocated=ResourceVector(),  # Empty initially
            num_tasks=0,
            total_duration_ms=0.0,
            queue_length=0,
            last_update_time=time.time() * 1000
        )

        self._node_states[node_id] = initial_state
        logger.debug(f"Registered node {node_id} of type {node_type}")

    def add_node_loads(self, node_updates: Dict[str, NodeLoadUpdate], current_time: float) -> List[Event]:
        """
        Add node load updates from scheduler or nodes (matches Java addNodeLoads).

        This is the core bidirectional update mechanism:
        - Schedulers call this with placement decisions (sign=+1)
        - Nodes call this with completion notifications (sign=-1)

        Args:
            node_updates: Map of node_id -> load update
            current_time: Current simulation time

        Returns:
            List of events to broadcast state updates to schedulers (if batch threshold reached)
        """
        events = []

        # Apply all updates atomically (matches Java synchronized addSingleNodeLoad)
        for node_id, update in node_updates.items():
            self._apply_single_node_update(node_id, update, current_time)
            self._update_history.append(update)

        self._total_updates_received += len(node_updates)

        # Check if we need to broadcast to schedulers (matches Java batch trigger logic)
        updates_since_broadcast = self._total_updates_received - self._last_broadcast_count

        if updates_since_broadcast >= self.batch_size:
            logger.debug(f"Batch threshold reached: {updates_since_broadcast} >= {self.batch_size}, "
                        f"broadcasting to {len(self._registered_schedulers)} schedulers")

            # Create broadcast events to all schedulers
            for i, scheduler_callback in enumerate(self._registered_schedulers):
                broadcast_event = Event(
                    event_id=self._total_updates_received * 1000 + i,  # Unique ID
                    timestamp=current_time + 1.0,  # Small delay for network latency
                    event_type=EventType.DATASTORE_BROADCAST,
                    source_id="datastore",
                    target_id=f"scheduler_{i}",
                    data={
                        'node_states': self._get_node_states_snapshot(),
                        'callback': scheduler_callback
                    }
                )
                events.append(broadcast_event)

            self._last_broadcast_count = self._total_updates_received

        return events

    def _apply_single_node_update(self, node_id: str, update: NodeLoadUpdate, current_time: float):
        """Apply a single node load update (matches Java addSingleNodeLoad)."""
        if node_id not in self._node_states:
            logger.warning(f"Node {node_id} not found, creating new entry")
            # Create minimal state for unknown node
            self._node_states[node_id] = NodeState(
                node_id=node_id,
                node_type=update.node_type,
                capacity=ResourceVector(cores=8, memory=32768, disk=50000),  # Default capacity
                allocated=ResourceVector(),
                num_tasks=0,
                total_duration_ms=0.0,
                queue_length=0,
                last_update_time=current_time * 1000
            )

        node_state = self._node_states[node_id]

        # Apply resource changes (matches Java line 63-67)
        node_state.allocated.cores += update.resource_change.cores * update.sign
        node_state.allocated.memory += update.resource_change.memory * update.sign
        node_state.allocated.disk += update.resource_change.disk * update.sign

        # Apply task and duration changes (matches Java line 66-67)
        node_state.num_tasks += update.num_tasks_change * update.sign
        node_state.total_duration_ms += update.total_duration_change * update.sign

        # Update timestamp
        node_state.last_update_time = current_time * 1000

        logger.debug(f"Applied {update.sign:+d} update to {node_id}: "
                    f"tasks={node_state.num_tasks}, "
                    f"cpu={node_state.allocated.cores:.1f}, "
                    f"memory={node_state.allocated.memory:.0f}")

    def _get_node_states_snapshot(self) -> Dict[str, NodeState]:
        """Get current node states snapshot (matches Java getNodeStates)."""
        # Return deep copy to prevent external modifications
        snapshot = {}
        for node_id, state in self._node_states.items():
            snapshot[node_id] = NodeState(
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
        return snapshot

    def get_node_states(self) -> Dict[str, NodeState]:
        """Public interface to get current node states."""
        return self._get_node_states_snapshot()

    def contains_node(self, node_id: str) -> bool:
        """Check if node is registered (matches Java containsNode)."""
        return node_id in self._node_states

    def get_statistics(self) -> Dict[str, any]:
        """Get DataStore statistics for debugging."""
        return {
            'total_nodes': len(self._node_states),
            'total_updates_received': self._total_updates_received,
            'broadcasts_sent': self._last_broadcast_count // self.batch_size,
            'registered_schedulers': len(self._registered_schedulers),
            'batch_size': self.batch_size,
            'scheduler_update_frequency': self.scheduler_update_frequency,
            'current_staleness': self._total_updates_received - self._last_broadcast_count
        }