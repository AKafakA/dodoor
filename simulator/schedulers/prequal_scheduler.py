"""
Google Prequal scheduler implementation.

This module implements the Google Prequal scheduler algorithm that maintains
a pool of probed nodes and uses quantile-based selection for task placement.
"""

import random
import logging
import time
import statistics
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from ..core.events import EventType, Event
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from core.events import EventType, Event

logger = logging.getLogger(__name__)


class ProbeInfo:
    """Information about a probed node."""
    
    def __init__(self, probe_time: float, used_count: int = 0):
        self.probe_time = probe_time
        self.used_count = used_count
    
    def increment_usage(self):
        """Increment the usage count for this probe."""
        self.used_count += 1
    
    def is_valid(self, current_time: float, age_budget_ms: float) -> bool:
        """Check if probe is still valid based on age budget."""
        return (current_time - self.probe_time) < age_budget_ms


class PrequalScheduler(BaseScheduler):
    """
    Google Prequal scheduler implementation.
    
    Maintains a pool of probed nodes and uses quantile-based selection
    to choose nodes with load below a threshold for task placement.
    """
    
    def __init__(self, config, scheduler_id: str = "prequal_scheduler"):
        super().__init__(config, scheduler_id)
        
        # Prequal-specific parameters
        self.rif_quantile = getattr(config, 'rif_quantile', 0.8)  # Default to 80th percentile
        self.probe_pool_size = getattr(config, 'probe_pool_size', 10)
        self.delta = getattr(config, 'delta', 1)
        self.probe_rate = getattr(config, 'probe_rate', 2)
        self.probe_delete_rate = getattr(config, 'probe_delete_rate', 1)
        self.probe_age_budget_ms = getattr(config, 'probe_age_budget_ms', 10000.0)  # 10 seconds
        
        # Probe pool management
        self.probe_info: Dict[str, ProbeInfo] = {}  # node_id -> ProbeInfo
        
        logger.info(f"Initialized Prequal scheduler: quantile={self.rif_quantile}, "
                   f"pool_size={self.probe_pool_size}, age_budget={self.probe_age_budget_ms}ms")
    
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """Schedule tasks using Prequal algorithm."""
        start_time = time.perf_counter()
        events = []
        
        if not node_states:
            return events
        
        # Calculate quantile cutoff based on current node loads
        task_counts = [node.num_tasks for node in node_states.values()]
        cutoff = self._calculate_quantile(task_counts, self.rif_quantile)
        
        logger.debug(f"Prequal cutoff: {cutoff} tasks (quantile: {self.rif_quantile})")
        
        # Schedule all tasks in the batch to the same node (as per Java implementation)
        if tasks:
            selected_node = self._select_node_from_prequal_pool(
                node_states, cutoff, current_time
            )
            
            if selected_node:
                for task in tasks:
                    # CRITICAL: No resource checking - always send tasks to nodes
                    # Physical Java system always accepts tasks into FIFO queues
                    
                    # Create task assignment event
                    assignment_event = Event(
                        event_id=self._generate_event_id(),
                        timestamp=current_time,
                        event_type=EventType.TASK_SCHEDULED,
                        source_id=self.scheduler_id,
                        target_id=selected_node,
                        data={'task': task, 'assignment_method': 'prequal'}
                    )
                    
                    events.append(assignment_event)
                    
                    # Update node state for scheduler compatibility
                    node_states[selected_node].allocate_task(task)
        
        # Update probe pool
        self._update_probe_pool(node_states, current_time)
        
        total_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), total_time_ms)
        
        logger.debug(f"Prequal scheduled {len(events)} tasks to node {selected_node if events else 'none'}")
        
        return events
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        return random.randint(1000000, 9999999)
    
    def _calculate_quantile(self, values: List[int], quantile: float) -> int:
        """Calculate quantile value from a list of integers."""
        if not values:
            return 0
        
        # Sort values and find quantile index
        sorted_values = sorted(values)
        index = max(int(quantile * len(sorted_values)) - 1, 0)
        index = min(index, len(sorted_values) - 1)
        
        return sorted_values[index]
    
    def _select_node_from_prequal_pool(self, node_states: Dict[str, NodeState],
                                     task_count_cutoff: int, current_time: float) -> Optional[str]:
        """Select a node from the prequal pool for task placement."""
        
        # Build prequal pool from valid probes
        prequal_nodes = {}
        probe_reuse_budget = self._calculate_probe_reuse_budget(len(node_states))
        
        # Get probes in reverse order (most recent first)
        sorted_probes = sorted(self.probe_info.items(), 
                             key=lambda x: x[1].probe_time, reverse=True)
        
        for node_id, probe_info in sorted_probes[:self.probe_pool_size]:
            if (probe_info.used_count < probe_reuse_budget and
                probe_info.is_valid(current_time, self.probe_age_budget_ms) and
                node_id in node_states):
                
                prequal_nodes[node_id] = node_states[node_id]
                probe_info.increment_usage()
        
        logger.debug(f"Prequal pool contains {len(prequal_nodes)} valid nodes")
        
        # If no valid probes, fall back to random selection
        if not prequal_nodes:
            logger.debug("Prequal pool empty, selecting random node")
            available_nodes = list(node_states.keys())
            return self._random.choice(available_nodes) if available_nodes else None
        
        # Select node from prequal pool
        # First try nodes below the task count cutoff
        qualified_nodes = {nid: node for nid, node in prequal_nodes.items()
                          if node.num_tasks < task_count_cutoff}
        
        if qualified_nodes:
            # Among qualified nodes, select the one with minimum total duration
            selected_node = min(qualified_nodes.items(),
                              key=lambda x: x[1].total_duration_ms)[0]
        else:
            # If no nodes below cutoff, select node with minimum task count
            selected_node = min(prequal_nodes.items(),
                              key=lambda x: x[1].num_tasks)[0]
        
        logger.debug(f"Selected node {selected_node} from prequal pool "
                    f"(tasks: {node_states[selected_node].num_tasks}, "
                    f"duration: {node_states[selected_node].total_duration_ms:.1f}ms)")
        
        return selected_node
    
    def _calculate_probe_reuse_budget(self, num_nodes: int) -> int:
        """Calculate how many times a probe can be reused before expiring."""
        if num_nodes == 0:
            return 1
        
        denominator = (1 - self.probe_pool_size / num_nodes) * self.probe_rate - self.probe_delete_rate
        if denominator <= 0:
            return 1
        
        result = int((1 + self.delta) / denominator)
        return max(result, 1)
    
    def _update_probe_pool(self, node_states: Dict[str, NodeState], current_time: float):
        """Update the probe pool with new node information."""
        
        # Add new probes for nodes not currently in pool
        available_nodes = list(node_states.keys())
        
        # Remove expired probes
        expired_nodes = [node_id for node_id, probe_info in self.probe_info.items()
                        if not probe_info.is_valid(current_time, self.probe_age_budget_ms)]
        
        for node_id in expired_nodes:
            del self.probe_info[node_id]
            logger.debug(f"Removed expired probe for node {node_id}")
        
        # Add new probes if pool is not full
        current_pool_size = len(self.probe_info)
        if current_pool_size < self.probe_pool_size:
            # Select random nodes to probe
            unprobed_nodes = [nid for nid in available_nodes if nid not in self.probe_info]
            
            if unprobed_nodes:
                # Add probes for random unprobed nodes
                num_new_probes = min(self.probe_pool_size - current_pool_size, len(unprobed_nodes))
                new_probes = self._random.sample(unprobed_nodes, num_new_probes)
                
                for node_id in new_probes:
                    self.probe_info[node_id] = ProbeInfo(current_time, 0)
                    logger.debug(f"Added new probe for node {node_id}")
    
    def get_statistics(self) -> Dict:
        """Get Prequal-specific statistics."""
        stats = super().get_statistics()
        stats['prequal_config'] = {
            'rif_quantile': self.rif_quantile,
            'probe_pool_size': self.probe_pool_size,
            'probe_age_budget_ms': self.probe_age_budget_ms,
            'delta': self.delta,
            'probe_rate': self.probe_rate,
            'probe_delete_rate': self.probe_delete_rate
        }
        stats['probe_pool'] = {
            'active_probes': len(self.probe_info),
            'node_ids': list(self.probe_info.keys())
        }
        return stats