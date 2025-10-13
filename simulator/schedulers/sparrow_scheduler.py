"""
Sparrow scheduler implementation with proper late-binding messaging.

This implementation correctly models Sparrow's probe/confirm protocol:
1. Send probe requests to multiple nodes
2. Wait for probe responses 
3. Send confirm to fastest responder
4. Send cancellations to remaining nodes
"""

import random
import logging
import uuid
from typing import Dict, List, Set, Optional
from collections import defaultdict
import time

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from ..core.events import (EventType, Event, ProbeRequest, ProbeResponse, 
                              ConfirmRequest, ConfirmResponse)
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from core.events import (EventType, Event, ProbeRequest, ProbeResponse,
                            ConfirmRequest, ConfirmResponse)

logger = logging.getLogger(__name__)


class SparrowScheduler(BaseScheduler):
    """
    Sparrow scheduler with proper late-binding probe/confirm protocol.
    
    Key behaviors:
    - Send probes to multiple nodes (typically 2) per task
    - Wait for probe responses indicating queue position and wait time
    - Confirm task placement with fastest responding node
    - Cancel remaining probes proactively to avoid wasted resources
    """
    
    def __init__(self, config, scheduler_id: str = "sparrow_scheduler"):
        super().__init__(config, scheduler_id)
        
        # Sparrow-specific configuration
        self.probe_ratio = getattr(config, 'probe_ratio', 2)  # Probes per task
        self.probe_timeout_ms = getattr(config, 'probe_timeout_ms', 100.0)
        self.queue_threshold = getattr(config, 'queue_threshold', 5)  # Max queue length
        
        # Probe tracking
        self.pending_probes: Dict[str, 'PendingProbe'] = {}  # probe_id -> PendingProbe
        self.task_probes: Dict[str, Set[str]] = defaultdict(set)  # task_id -> probe_ids
        self.probe_responses: Dict[str, List[ProbeResponse]] = defaultdict(list)  # task_id -> responses
        
        # Statistics
        self.probes_sent = 0
        self.cancellations_sent = 0
        self.confirmations_sent = 0
        
        logger.info(f"Initialized Sparrow scheduler: probe_ratio={self.probe_ratio}, "
                   f"timeout={self.probe_timeout_ms}ms")
    
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """
        Schedule tasks using Sparrow late-binding.
        
        Returns list of probe request events to be sent to nodes.
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
            
            # Select nodes to probe (typically 2)
            num_probes = min(self.probe_ratio, len(viable_nodes))
            nodes_to_probe = self._random.sample(viable_nodes, num_probes)
            
            # Create probe requests for selected nodes
            probe_events = self._create_probe_requests(task, nodes_to_probe, current_time)
            events.extend(probe_events)
            
            logger.debug(f"Sent {len(probe_events)} probes for task {task.task_id} to nodes {nodes_to_probe}")
        
        total_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), total_time_ms)
        
        return events
    
    def _create_probe_requests(self, task: Task, node_ids: List[str], 
                              current_time: float) -> List[Event]:
        """Create probe request events for a task."""
        events = []
        
        for node_id in node_ids:
            probe_id = str(uuid.uuid4())
            
            # Create probe request message
            probe_request = ProbeRequest(
                probe_id=probe_id,
                task_id=task.task_id,
                resource_requirements=task.resource_request,
                timeout_ms=self.probe_timeout_ms,
                scheduler_id=self.scheduler_id
            )
            
            # Track pending probe
            self.pending_probes[probe_id] = PendingProbe(
                probe_id=probe_id,
                task_id=task.task_id,
                task=task,
                target_node_id=node_id,
                sent_time=current_time
            )
            
            self.task_probes[task.task_id].add(probe_id)
            
            # Calculate network delay for probe request
            import random
            probe_delay = max(0.1, random.normalvariate(2.0, 0.5))
            
            # Create probe request event with network delay
            event = Event(
                event_id=self._generate_event_id(),
                timestamp=current_time + probe_delay,
                event_type=EventType.PROBE_REQUEST,
                source_id=self.scheduler_id,
                target_id=node_id,
                data={'probe_request': probe_request}
            )
            
            events.append(event)
            self.probes_sent += 1
        
        return events
    
    def handle_probe_response(self, response: ProbeResponse, current_time: float) -> List[Event]:
        """
        Handle probe response from a node.
        
        Sparrow's key algorithm: confirm with first suitable response,
        cancel remaining probes.
        """
        events = []
        task_id = response.task_id
        
        # Store the response
        self.probe_responses[task_id].append(response)
        
        # Check if this is the first acceptable response for this task
        if self._should_confirm_probe(response, task_id):
            # Confirm this probe
            confirm_event = self._create_confirm_request(response, current_time)
            events.append(confirm_event)
            
            # Cancel all other pending probes for this task
            cancel_events = self._cancel_remaining_probes(task_id, response.probe_id, current_time)
            events.extend(cancel_events)
            
            logger.debug(f"Confirmed task {task_id} on node {response.node_id}, "
                        f"cancelled {len(cancel_events)} other probes")
        
        return events
    
    def _should_confirm_probe(self, response: ProbeResponse, task_id: str) -> bool:
        """
        Decide whether to confirm this probe response.
        
        Sparrow typically confirms the first acceptable response.
        """
        # Don't confirm if node cannot accept the task
        if not response.can_accept:
            return False
        
        # Don't confirm if queue is too long
        if response.queue_position > self.queue_threshold:
            return False
        
        # Check if we've already confirmed another probe for this task
        for existing_response in self.probe_responses[task_id]:
            if (existing_response.probe_id != response.probe_id and 
                existing_response.can_accept and
                existing_response.queue_position <= self.queue_threshold):
                # We already have a suitable response, don't confirm this one
                return False
        
        return True
    
    def _create_confirm_request(self, response: ProbeResponse, current_time: float) -> Event:
        """Create confirm request event."""
        probe_id = response.probe_id
        pending_probe = self.pending_probes[probe_id]
        
        confirm_request = ConfirmRequest(
            probe_id=probe_id,
            task_id=response.task_id,
            task=pending_probe.task,
            scheduler_id=self.scheduler_id
        )
        
        # Calculate network delay for confirm request
        import random
        confirm_delay = max(0.1, random.normalvariate(2.0, 0.5))
        
        event = Event(
            event_id=self._generate_event_id(),
            timestamp=current_time + confirm_delay,
            event_type=EventType.CONFIRM_REQUEST,
            source_id=self.scheduler_id,
            target_id=response.node_id,
            data={'confirm_request': confirm_request}
        )
        
        self.confirmations_sent += 1
        return event
    
    def _cancel_remaining_probes(self, task_id: str, confirmed_probe_id: str, 
                                current_time: float) -> List[Event]:
        """Cancel all probes for a task except the confirmed one."""
        events = []
        
        for probe_id in self.task_probes[task_id]:
            if probe_id != confirmed_probe_id and probe_id in self.pending_probes:
                pending_probe = self.pending_probes[probe_id]
                
                # Calculate network delay for cancel request
                import random
                cancel_delay = max(0.1, random.normalvariate(2.0, 0.5))
                
                cancel_event = Event(
                    event_id=self._generate_event_id(),
                    timestamp=current_time + cancel_delay,
                    event_type=EventType.CANCEL_REQUEST,
                    source_id=self.scheduler_id,
                    target_id=pending_probe.target_node_id,
                    data={'probe_id': probe_id, 'task_id': task_id}
                )
                
                events.append(cancel_event)
                self.cancellations_sent += 1
                
                # Remove from pending probes
                del self.pending_probes[probe_id]
        
        return events
    
    def handle_confirm_response(self, response: ConfirmResponse, current_time: float) -> Optional[SchedulingDecision]:
        """Handle confirmation response from node."""
        if response.confirmed:
            pending_probe = self.pending_probes.get(response.probe_id)
            if pending_probe:
                # Create scheduling decision
                decision = SchedulingDecision(
                    task=pending_probe.task,
                    assigned_node=response.node_id,
                    placement_score=0.0,  # Sparrow doesn't use load-based scoring
                    scheduling_latency_ms=current_time - pending_probe.sent_time
                )
                
                # Clean up tracking
                self._cleanup_task_probes(response.task_id)
                
                logger.debug(f"Task {response.task_id} confirmed on {response.node_id}")
                return decision
        
        return None
    
    def _cleanup_task_probes(self, task_id: str):
        """Clean up tracking data for completed task."""
        if task_id in self.task_probes:
            for probe_id in self.task_probes[task_id]:
                self.pending_probes.pop(probe_id, None)
            del self.task_probes[task_id]
        
        self.probe_responses.pop(task_id, None)
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        return random.randint(1000000, 9999999)
    
    def get_statistics(self) -> Dict:
        """Get Sparrow-specific statistics."""
        stats = super().get_statistics()
        stats['sparrow_stats'] = {
            'probes_sent': self.probes_sent,
            'cancellations_sent': self.cancellations_sent,
            'confirmations_sent': self.confirmations_sent,
            'pending_probes': len(self.pending_probes),
            'probe_ratio': self.probe_ratio
        }
        return stats


class PendingProbe:
    """Track information about a pending probe."""
    
    def __init__(self, probe_id: str, task_id: str, task: Task, 
                 target_node_id: str, sent_time: float):
        self.probe_id = probe_id
        self.task_id = task_id
        self.task = task
        self.target_node_id = target_node_id
        self.sent_time = sent_time