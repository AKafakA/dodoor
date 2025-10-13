"""
Power-of-Two scheduler implementation with proper load probing.

This implementation correctly models Power-of-Two load balancing:
1. Send load probe requests to random subset of nodes
2. Wait for load probe responses with current queue info
3. Select least loaded node from responses
4. Send task directly to selected node
"""

import random
import logging
import uuid
from typing import Dict, List, Optional
import time

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from ..core.events import (EventType, Event, LoadProbeRequest, LoadProbeResponse)
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from core.events import (EventType, Event, LoadProbeRequest, LoadProbeResponse)

logger = logging.getLogger(__name__)


class PowerOfTwoScheduler(BaseScheduler):
    """
    Power-of-Two scheduler with proper load probing protocol.
    
    Algorithm:
    1. For each task, randomly sample k nodes (typically k=2)
    2. Send load probe requests to sampled nodes
    3. Wait for responses with current load information
    4. Select least loaded node from responses
    5. Send task assignment to selected node
    """
    
    def __init__(self, config, scheduler_id: str = "power_of_two_scheduler"):
        super().__init__(config, scheduler_id)
        
        # Power-of-Two specific configuration
        self.sample_size = getattr(config, 'sample_size', 2)  # Number of nodes to probe
        self.probe_timeout_ms = getattr(config, 'probe_timeout_ms', 50.0)
        self.use_cached_loads = getattr(config, 'use_cached_loads', False)  # Runtime vs cached
        
        # Probe tracking
        self.pending_probes: Dict[str, 'PendingLoadProbe'] = {}  # probe_id -> PendingLoadProbe
        self.task_load_probes: Dict[str, List[str]] = {}  # task_id -> [probe_ids]
        self.load_responses: Dict[str, List[LoadProbeResponse]] = {}  # task_id -> responses
        
        # Statistics
        self.load_probes_sent = 0
        self.load_responses_received = 0
        
        logger.info(f"Initialized Power-of-Two scheduler: sample_size={self.sample_size}, "
                   f"cached_loads={self.use_cached_loads}")
    
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """
        Schedule tasks using Power-of-Two load probing.
        
        Returns list of load probe request events.
        """
        start_time = time.perf_counter()
        events = []
        
        available_nodes = list(node_states.keys())
        if not available_nodes:
            return events
        
        for task in tasks:
            # CRITICAL: All nodes are viable - no resource checking at scheduler level
            # Physical Java system always accepts tasks into FIFO queues  
            viable_nodes = available_nodes
            
            if self.use_cached_loads:
                # Use cached load information (faster but less accurate)
                decision = self._schedule_with_cached_loads(task, viable_nodes, node_states, current_time)
                if decision:
                    # Would generate TASK_ASSIGNMENT event here
                    pass
            else:
                # Use runtime probing (slower but more accurate)
                probe_events = self._create_load_probe_requests(task, viable_nodes, current_time)
                events.extend(probe_events)
        
        total_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), total_time_ms)
        
        return events
    
    def _create_load_probe_requests(self, task: Task, viable_nodes: List[str], 
                                   current_time: float) -> List[Event]:
        """Create load probe request events for Power-of-Two sampling."""
        events = []
        
        # Sample k nodes randomly (Power-of-Two choices)
        sample_size = min(self.sample_size, len(viable_nodes))
        sampled_nodes = self._random.sample(viable_nodes, sample_size)
        
        probe_ids = []
        
        for node_id in sampled_nodes:
            probe_id = str(uuid.uuid4())
            probe_ids.append(probe_id)
            
            # Create load probe request
            load_probe_request = LoadProbeRequest(
                probe_id=probe_id,
                scheduler_id=self.scheduler_id,
                request_timestamp=current_time
            )
            
            # Track pending probe
            self.pending_probes[probe_id] = PendingLoadProbe(
                probe_id=probe_id,
                task_id=task.task_id,
                task=task,
                target_node_id=node_id,
                sent_time=current_time
            )
            
            # Calculate network delay for load probe request  
            import random
            probe_delay = max(0.1, random.normalvariate(2.0, 0.5))
            
            # Create probe request event with network delay
            event = Event(
                event_id=self._generate_event_id(),
                timestamp=current_time + probe_delay,
                event_type=EventType.LOAD_PROBE_REQUEST,
                source_id=self.scheduler_id,
                target_id=node_id,
                data={'load_probe_request': load_probe_request}
            )
            
            events.append(event)
            self.load_probes_sent += 1
        
        # Track probes for this task
        self.task_load_probes[task.task_id] = probe_ids
        self.load_responses[task.task_id] = []
        
        logger.debug(f"Sent {len(events)} load probes for task {task.task_id} to nodes {sampled_nodes}")
        
        return events
    
    def handle_load_probe_response(self, response: LoadProbeResponse, 
                                  current_time: float) -> Optional[Event]:
        """
        Handle load probe response and make scheduling decision.
        
        Once all probes for a task respond, select best node and assign task.
        """
        probe_id = response.probe_id
        
        if probe_id not in self.pending_probes:
            logger.warning(f"Received response for unknown probe {probe_id}")
            return None
        
        pending_probe = self.pending_probes[probe_id]
        task_id = pending_probe.task_id
        
        # Store the response
        self.load_responses[task_id].append(response)
        self.load_responses_received += 1
        
        # Check if we've received all responses for this task
        expected_responses = len(self.task_load_probes[task_id])
        received_responses = len(self.load_responses[task_id])
        
        if received_responses >= expected_responses:
            # All responses received, make scheduling decision
            selected_node = self._select_best_node(task_id)
            
            if selected_node:
                # Create task assignment event
                assignment_event = self._create_task_assignment(task_id, selected_node, current_time)
                
                # Clean up tracking
                self._cleanup_task_probes(task_id)
                
                logger.debug(f"Assigned task {task_id} to {selected_node} after receiving "
                           f"{received_responses} load probe responses")
                
                return assignment_event
        
        return None
    
    def _select_best_node(self, task_id: str) -> Optional[str]:
        """Select the least loaded node from probe responses."""
        responses = self.load_responses[task_id]
        
        if not responses:
            return None
        
        # Find node with lowest load (Power-of-Two principle)
        best_response = min(responses, key=lambda r: r.current_load)
        
        logger.debug(f"Selected node {best_response.node_id} with load {best_response.current_load:.3f} "
                    f"from {len(responses)} candidates")
        
        return best_response.node_id
    
    def _create_task_assignment(self, task_id: str, node_id: str, current_time: float) -> Event:
        """Create task assignment event."""
        # Find the task from pending probes
        task = None
        for probe_id in self.task_load_probes[task_id]:
            if probe_id in self.pending_probes:
                task = self.pending_probes[probe_id].task
                break
        
        if not task:
            logger.error(f"Could not find task {task_id} for assignment")
            return None
        
        event = Event(
            event_id=self._generate_event_id(),
            timestamp=current_time,
            event_type=EventType.TASK_SCHEDULED,
            source_id=self.scheduler_id,
            target_id=node_id,
            data={'task': task, 'assignment_method': 'power_of_two'}
        )
        
        return event
    
    def _schedule_with_cached_loads(self, task: Task, viable_nodes: List[str],
                                   node_states: Dict[str, NodeState], 
                                   current_time: float) -> Optional[SchedulingDecision]:
        """Schedule using cached load information (fallback for comparison)."""
        
        # Sample nodes and use cached state
        sample_size = min(self.sample_size, len(viable_nodes))
        sampled_nodes = self._random.sample(viable_nodes, sample_size)
        
        # Select least loaded node from cached states
        best_node = None
        best_load = float('inf')
        
        for node_id in sampled_nodes:
            node_state = node_states[node_id]
            load = node_state.num_tasks + node_state.utilization  # Simple load metric
            
            if load < best_load:
                best_load = load
                best_node = node_id
        
        if best_node:
            # Update node state immediately (simplified)
            node_states[best_node].allocate_task(task)
            
            decision = SchedulingDecision(
                task=task,
                assigned_node=best_node,
                placement_score=best_load,
                scheduling_latency_ms=1.0  # Assume 1ms for cached lookup
            )
            
            logger.debug(f"Scheduled task {task.task_id} to {best_node} using cached loads")
            return decision
        
        return None
    
    def _cleanup_task_probes(self, task_id: str):
        """Clean up tracking data for completed task."""
        if task_id in self.task_load_probes:
            for probe_id in self.task_load_probes[task_id]:
                self.pending_probes.pop(probe_id, None)
            del self.task_load_probes[task_id]
        
        self.load_responses.pop(task_id, None)
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        return random.randint(1000000, 9999999)
    
    def get_statistics(self) -> Dict:
        """Get Power-of-Two specific statistics."""
        stats = super().get_statistics()
        stats['power_of_two_stats'] = {
            'load_probes_sent': self.load_probes_sent,
            'load_responses_received': self.load_responses_received,
            'pending_probes': len(self.pending_probes),
            'sample_size': self.sample_size,
            'use_cached_loads': self.use_cached_loads
        }
        return stats


class PendingLoadProbe:
    """Track information about a pending load probe."""
    
    def __init__(self, probe_id: str, task_id: str, task: Task,
                 target_node_id: str, sent_time: float):
        self.probe_id = probe_id
        self.task_id = task_id
        self.task = task
        self.target_node_id = target_node_id
        self.sent_time = sent_time