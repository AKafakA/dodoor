"""
Event definitions and handling for discrete event simulation.

This module defines all event types used in the Dodoor simulation and provides
a priority queue-based event scheduler for precise temporal ordering.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, List
import heapq
import logging

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Essential event types for discrete event simulation matching Java behavior."""

    # Task lifecycle events (essential for discrete event simulation)
    TASK_SUBMISSION = "task_submission"      # Workload generator submits new task
    TASK_SCHEDULED = "task_scheduled"        # Scheduler assigns task to node (after overhead)
    TASK_STARTED = "task_started"            # Task execution begins (optional)
    TASK_COMPLETED = "task_completed"        # Task execution finishes
    TASK_FAILED = "task_failed"              # Task execution fails (optional)

    # Sparrow late-binding events (matches Java LateBindTaskScheduler protocol)
    SPARROW_CONFIRM_REQUEST = "sparrow_confirm_request"  # Node requests confirm from scheduler
    SPARROW_CONFIRM_RESPONSE = "sparrow_confirm_response"  # Scheduler confirms/rejects node
    SPARROW_CANCEL_RESERVATION = "sparrow_cancel_reservation"  # Scheduler cancels other reservations

    # Simulation control events (essential for framework)
    METRICS_COLLECTION = "metrics_collection"  # Periodic metrics logging (every 10s)
    SIMULATION_END = "simulation_end"        # End simulation

    # DataStore events (Phase 4: Cached State Architecture)
    DATASTORE_BROADCAST = "datastore_broadcast"  # DataStore broadcasts state updates to schedulers

    # REMOVED: Fictional message-passing events that don't exist in Java
    # - Sparrow probe/confirm/cancel cycles (Java Sparrow uses direct calls)
    # - Power-of-Two probe request/response (Java uses direct nodeMonitorClient calls)
    # - DataStore query/response (Java uses direct DataStore method calls)
    # - Node heartbeat/state updates (Java doesn't have separate heartbeat protocol)
    # - Network message events (overhead modeled in scheduling delays instead)
    #
    # PRESERVED: Scheduling overhead modeling in UnifiedScheduler._calculate_scheduling_overhead()
    # - Dodoor: ~2ms overhead, 1 message per task (cached decisions)
    # - PowerOfTwo: ~10ms overhead, 5 messages per task (runtime probing)
    # - Prequal: ~6ms overhead, 2-4 messages per task (probe pool management)
    # - Sparrow: ~15ms overhead, 6-8 messages per task (late binding complexity)


@dataclass
class Event:
    """Base event class for discrete event simulation."""
    
    event_id: int  # Unique event identifier
    timestamp: float  # Event timestamp in milliseconds
    event_type: EventType  # Type of event
    source_id: str  # ID of event source component
    target_id: Optional[str] = None  # ID of event target component
    data: Dict[str, Any] = None  # Event-specific data payload
    
    def __post_init__(self):
        """Initialize event data if not provided."""
        if self.data is None:
            self.data = {}
    
    def __lt__(self, other: 'Event') -> bool:
        """Compare events for priority queue ordering."""
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        # Break ties with event ID for deterministic ordering
        return self.event_id < other.event_id
    
    def __repr__(self) -> str:
        """String representation of event."""
        return (f"Event(id={self.event_id}, t={self.timestamp:.3f}ms, "
                f"type={self.event_type.value}, src={self.source_id}, "
                f"tgt={self.target_id})")


class EventHandler(ABC):
    """Abstract base class for event handlers."""
    
    @abstractmethod
    def handle_event(self, event: Event) -> Optional[List[Event]]:
        """
        Handle an event and optionally return new events to schedule.
        
        Args:
            event: Event to handle
            
        Returns:
            List of new events to schedule, or None if no new events
        """
        pass


@dataclass
class ProbeRequest:
    """Probe request message for Sparrow late-binding."""
    probe_id: str
    task_id: str
    resource_requirements: 'ResourceVector'
    timeout_ms: float
    scheduler_id: str


@dataclass 
class ProbeResponse:
    """Probe response message from node to scheduler."""
    probe_id: str
    task_id: str
    node_id: str
    can_accept: bool
    estimated_wait_time_ms: float
    queue_position: int


@dataclass
class ConfirmRequest:
    """Confirm request to actually place task on node."""
    probe_id: str
    task_id: str
    task: 'Task'
    scheduler_id: str


@dataclass
class ConfirmResponse:
    """Confirmation response from node."""
    probe_id: str
    task_id: str
    node_id: str
    confirmed: bool
    actual_start_time_ms: float


@dataclass
class LoadProbeRequest:
    """Load probe request for Power-of-Two scheduler."""
    probe_id: str
    scheduler_id: str
    request_timestamp: float


@dataclass
class LoadProbeResponse:
    """Load probe response with current node state."""
    probe_id: str
    node_id: str
    current_load: float
    queue_length: int
    available_resources: 'ResourceVector'
    response_timestamp: float


@dataclass
class DataStoreQuery:
    """Query to DataStore for cached load information."""
    query_id: str
    scheduler_id: str
    node_ids: List[str]
    timestamp: float


@dataclass
class DataStoreResponse:
    """Response from DataStore with cached load data."""
    query_id: str
    node_loads: Dict[str, 'NodeLoad']
    cache_age_ms: float
    timestamp: float


@dataclass
class NodeLoad:
    """Cached node load information."""
    node_id: str
    cpu_utilization: float
    memory_utilization: float
    queue_length: int
    total_duration_ms: float
    last_updated: float


class EventScheduler:
    """
    Priority queue-based event scheduler for discrete event simulation.
    
    Maintains temporal ordering of events and dispatches them to registered handlers.
    """
    
    def __init__(self):
        """Initialize event scheduler."""
        self._event_queue: List[Event] = []  # Priority queue of events
        self._event_handlers: Dict[EventType, List[EventHandler]] = {}
        self._next_event_id = 0
        self._current_time = 0.0
        self._total_events_processed = 0
        
    def register_handler(self, event_type: EventType, handler: EventHandler) -> None:
        """Register an event handler for a specific event type."""
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)
        logger.debug(f"Registered handler {handler} for event type {event_type}")
    
    def schedule_event(self, event: Event) -> None:
        """Schedule an event for future execution."""
        if event.timestamp < self._current_time:
            raise ValueError(f"Cannot schedule event in the past: {event.timestamp} < {self._current_time}")
        
        # Assign unique event ID if not set
        if event.event_id == 0:
            event.event_id = self._next_event_id
            self._next_event_id += 1
            
        heapq.heappush(self._event_queue, event)
        logger.debug(f"Scheduled {event}")
    
    def schedule_event_at(self, timestamp: float, event_type: EventType,
                         source_id: str, target_id: str = None,
                         data: Dict[str, Any] = None) -> Event:
        """Convenience method to schedule an event at a specific time."""
        event = Event(
            event_id=0,  # Let schedule_event assign unique ID
            timestamp=timestamp,
            event_type=event_type,
            source_id=source_id,
            target_id=target_id,
            data=data or {}
        )
        self.schedule_event(event)
        return event
    
    def schedule_event_after(self, delay_ms: float, event_type: EventType,
                            source_id: str, target_id: str = None,
                            data: Dict[str, Any] = None) -> Event:
        """Convenience method to schedule an event after a delay."""
        return self.schedule_event_at(
            self._current_time + delay_ms, event_type, source_id, target_id, data
        )
    
    def get_next_event(self) -> Optional[Event]:
        """Get the next event from the queue without removing it."""
        return self._event_queue[0] if self._event_queue else None
    
    def process_next_event(self) -> bool:
        """
        Process the next event in the queue.

        Returns:
            True if an event was processed, False if queue is empty
        """
        if not self._event_queue:
            return False

        # Pop next event from priority queue
        event = heapq.heappop(self._event_queue)
        self._current_time = event.timestamp
        self._total_events_processed += 1

        logger.debug(f"Processing event_id={event.event_id}, type={event.event_type}, timestamp={event.timestamp}")

        # Log TASK_COMPLETED events for debugging
        if event.event_type.name == 'TASK_COMPLETED':
            logger.info(f"Processing TASK_COMPLETED event for task at timestamp={event.timestamp}")

        # Dispatch event to registered handlers
        if event.event_type in self._event_handlers:
            for handler in self._event_handlers[event.event_type]:
                try:
                    new_events = handler.handle_event(event)
                    if new_events:
                        for new_event in new_events:
                            self.schedule_event(new_event)
                except Exception as e:
                    logger.error(f"Error handling event {event}: {e}")
                    # PHASE 5 DEBUG: Add full traceback for Azure trace debugging
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
        else:
            logger.warning(f"No handler registered for event type {event.event_type}")
        
        return True
    
    def run_until(self, end_time: float) -> None:
        """Run simulation until specified end time."""
        logger.info(f"Running simulation until time {end_time}")
        
        while self._event_queue and self._event_queue[0].timestamp <= end_time:
            if not self.process_next_event():
                break
                
        logger.info(f"Simulation completed at time {self._current_time}, "
                   f"processed {self._total_events_processed} events")
    
    def run_until_empty(self) -> None:
        """Run simulation until event queue is empty."""
        logger.info("Running simulation until event queue is empty")
        
        while self.process_next_event():
            pass
            
        logger.info(f"Simulation completed, processed {self._total_events_processed} events")
    
    @property
    def current_time(self) -> float:
        """Current simulation time in milliseconds."""
        return self._current_time
    
    @property
    def events_remaining(self) -> int:
        """Number of events remaining in the queue."""
        return len(self._event_queue)
    
    @property
    def events_processed(self) -> int:
        """Total number of events processed."""
        return self._total_events_processed
    
    def clear(self) -> None:
        """Clear all events and reset scheduler state."""
        self._event_queue.clear()
        self._current_time = 0.0
        self._next_event_id = 0
        self._total_events_processed = 0
        logger.info("Event scheduler cleared")


# Specific event data structures for common event types

@dataclass
class TaskEvent(Event):
    """Event related to task operations."""
    task_id: str = ""
    task_type: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        if 'task_id' not in self.data and self.task_id:
            self.data['task_id'] = self.task_id
        if 'task_type' not in self.data and self.task_type:
            self.data['task_type'] = self.task_type


@dataclass  
class SchedulingEvent(Event):
    """Event related to scheduling decisions."""
    num_tasks: int = 0
    scheduler_strategy: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        if 'num_tasks' not in self.data and self.num_tasks > 0:
            self.data['num_tasks'] = self.num_tasks
        if 'scheduler_strategy' not in self.data and self.scheduler_strategy:
            self.data['scheduler_strategy'] = self.scheduler_strategy


@dataclass
class NetworkEvent(Event):
    """Event related to network communication."""
    message_type: str = ""
    message_size: int = 0
    latency_ms: float = 0.0
    
    def __post_init__(self):
        super().__post_init__()
        self.data.update({
            'message_type': self.message_type,
            'message_size': self.message_size, 
            'latency_ms': self.latency_ms
        })


@dataclass
class MetricsEvent(Event):
    """Event for metrics collection."""
    metric_type: str = ""
    metric_value: float = 0.0
    
    def __post_init__(self):
        super().__post_init__()
        self.data.update({
            'metric_type': self.metric_type,
            'metric_value': self.metric_value
        })