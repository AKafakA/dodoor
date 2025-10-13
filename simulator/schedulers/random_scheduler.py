"""
Random scheduler implementation.

This module implements a simple random task placement scheduler
used as a baseline for comparison.
"""

import random
import logging
from typing import Dict, List
import time

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from ..core.events import EventType, Event
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from core.events import EventType, Event

logger = logging.getLogger(__name__)


class RandomScheduler(BaseScheduler):
    """
    Random scheduler that places tasks on random viable nodes.
    
    This serves as a baseline for comparison with more sophisticated
    scheduling algorithms.
    """
    
    def __init__(self, config, scheduler_id: str = "random_scheduler"):
        super().__init__(config, scheduler_id)
        
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """Schedule tasks randomly to viable nodes."""
        start_time = time.perf_counter()
        events = []
        
        available_nodes = list(node_states.keys())
        if not available_nodes:
            return events
            
        for task in tasks:
            # CRITICAL: All nodes are viable - no resource checking at scheduler level  
            # Physical Java system always accepts tasks into FIFO queues
            viable_nodes = available_nodes
                
            # Select random viable node
            selected_node = self._random.choice(viable_nodes)
            
            # Create task assignment event
            assignment_event = Event(
                event_id=self._generate_event_id(),
                timestamp=current_time,
                event_type=EventType.TASK_SCHEDULED,
                source_id=self.scheduler_id,
                target_id=selected_node,
                data={'task': task, 'assignment_method': 'random'}
            )
            
            events.append(assignment_event)
            
            # Update node state for scheduler compatibility
            node_states[selected_node].allocate_task(task)
                
        total_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), total_time_ms)
        
        logger.debug(f"Randomly scheduled {len(events)} tasks")
        
        return events
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        return random.randint(1000000, 9999999)


