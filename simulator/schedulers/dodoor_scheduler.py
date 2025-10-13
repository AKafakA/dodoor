"""
Dodoor scheduler implementation with proper DataStore caching.

This implementation correctly models Dodoor's cached load balancing:
1. Query DataStore for cached node loads
2. Use (1+β)-choice algorithm with cached loads  
3. Batch update DataStore with new load information
4. Handle cache staleness vs. messaging overhead tradeoff
"""

import random
import logging
import uuid
from typing import Dict, List, Optional, Set
import time
from collections import defaultdict

try:
    from .base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from ..core.events import (EventType, Event, DataStoreQuery, DataStoreResponse, NodeLoad)
except ImportError:
    from schedulers.base_scheduler import BaseScheduler, Task, NodeState, SchedulingDecision
    from core.events import (EventType, Event, DataStoreQuery, DataStoreResponse, NodeLoad)

logger = logging.getLogger(__name__)


class DodoorScheduler(BaseScheduler):
    """
    Dodoor scheduler with cached load balancing and DataStore integration.
    
    Key algorithms:
    1. (1+β)-choice: With probability β, use power-of-two; otherwise use cached loads
    2. Batch load updates: Update DataStore in batches to reduce messaging overhead
    3. Cache staleness handling: Balance freshness vs. performance
    """
    
    def __init__(self, config, scheduler_id: str = "dodoor_scheduler"):
        super().__init__(config, scheduler_id)
        
        # Dodoor-specific configuration
        self.beta = config.beta  # Power-of-two probability
        self.batch_size = config.batch_size  # Batch size for DataStore updates
        self.cache_timeout_ms = getattr(config, 'cache_timeout_ms', 1000.0)
        
        # DataStore interaction
        self.cached_loads: Dict[str, NodeLoad] = {}  # node_id -> NodeLoad
        self.last_cache_update = 0.0
        self.pending_load_updates: List[NodeLoad] = []  # Buffer for batch updates
        self.tasks_since_update = 0
        
        # Query tracking
        self.pending_queries: Dict[str, 'PendingDataStoreQuery'] = {}
        self.pending_tasks: Dict[str, List[Task]] = {}  # query_id -> tasks waiting for response
        
        # Statistics
        self.cache_hits = 0
        self.cache_misses = 0
        self.datastore_queries = 0
        self.batch_updates_sent = 0
        self.power_of_two_decisions = 0
        self.cached_decisions = 0
        
        logger.info(f"Initialized Dodoor scheduler: β={self.beta}, "
                   f"batch_size={self.batch_size}, cache_timeout={self.cache_timeout_ms}ms")
    
    def schedule_tasks(self, tasks: List[Task], node_states: Dict[str, NodeState],
                      current_time: float) -> List[Event]:
        """
        Schedule tasks using Dodoor (1+β)-choice with cached loads.
        
        Returns events for DataStore queries or immediate scheduling decisions.
        """
        start_time = time.perf_counter()
        events = []
        
        available_nodes = list(node_states.keys())
        if not available_nodes:
            return events
        
        # Check if we need to refresh cache
        if self._should_refresh_cache(current_time):
            # Send DataStore query for fresh load information
            query_event = self._create_datastore_query(tasks, available_nodes, current_time)
            events.append(query_event)
            
            self.datastore_queries += 1
            self.cache_misses += 1
        else:
            # Use cached loads for scheduling
            scheduling_events = self._schedule_with_cached_loads(tasks, available_nodes, 
                                                               node_states, current_time)
            events.extend(scheduling_events)
            self.cache_hits += 1
        
        # Check if we should send batch load updates
        self.tasks_since_update += len(tasks)
        if self.tasks_since_update >= self.config.num_tasks_to_update:
            batch_update_event = self._create_batch_load_update(current_time)
            if batch_update_event:
                events.append(batch_update_event)
        
        total_time_ms = (time.perf_counter() - start_time) * 1000
        self.update_statistics(len(tasks), total_time_ms)
        
        return events
    
    def _should_refresh_cache(self, current_time: float) -> bool:
        """Determine if cached loads are stale and need refreshing."""
        cache_age = current_time - self.last_cache_update
        return cache_age > self.cache_timeout_ms or not self.cached_loads
    
    def _create_datastore_query(self, tasks: List[Task], node_ids: List[str], 
                               current_time: float) -> Event:
        """Create DataStore query event for fresh load information."""
        query_id = str(uuid.uuid4())
        
        datastore_query = DataStoreQuery(
            query_id=query_id,
            scheduler_id=self.scheduler_id,
            node_ids=node_ids,
            timestamp=current_time
        )
        
        # Track pending query
        self.pending_queries[query_id] = PendingDataStoreQuery(
            query_id=query_id,
            node_ids=node_ids,
            sent_time=current_time
        )
        
        # Store tasks waiting for this query
        self.pending_tasks[query_id] = tasks.copy()
        
        event = Event(
            event_id=self._generate_event_id(),
            timestamp=current_time,
            event_type=EventType.DATASTORE_QUERY,
            source_id=self.scheduler_id,
            target_id="datastore",
            data={'datastore_query': datastore_query}
        )
        
        logger.debug(f"Querying DataStore for {len(node_ids)} nodes, "
                    f"{len(tasks)} tasks waiting")
        
        return event
    
    def handle_datastore_response(self, response: DataStoreResponse, 
                                 current_time: float) -> List[Event]:
        """
        Handle DataStore response with cached load information.
        
        Schedule pending tasks using fresh load data.
        """
        query_id = response.query_id
        
        if query_id not in self.pending_queries:
            logger.warning(f"Received response for unknown query {query_id}")
            return []
        
        # Update cached loads
        self.cached_loads.update(response.node_loads)
        self.last_cache_update = current_time
        
        # Schedule pending tasks
        pending_tasks = self.pending_tasks.pop(query_id, [])
        pending_query = self.pending_queries.pop(query_id)
        
        events = []
        if pending_tasks:
            # Create a fake node_states dict from cached loads for scheduling
            node_states = self._create_node_states_from_cache()
            scheduling_events = self._schedule_with_cached_loads(pending_tasks, 
                                                               pending_query.node_ids,
                                                               node_states, current_time)
            events.extend(scheduling_events)
        
        logger.debug(f"DataStore response received: {len(response.node_loads)} loads, "
                    f"cache age {response.cache_age_ms:.1f}ms, scheduled {len(pending_tasks)} tasks")
        
        return events
    
    def _schedule_with_cached_loads(self, tasks: List[Task], available_nodes: List[str],
                                   node_states: Dict[str, NodeState], 
                                   current_time: float) -> List[Event]:
        """Schedule tasks using (1+β)-choice with cached load information."""
        events = []
        
        for task in tasks:
            # CRITICAL: All nodes are viable - no resource checking at scheduler level
            # Physical Java system always accepts tasks into FIFO queues
            viable_nodes = available_nodes
            
            # Dodoor (1+β)-choice algorithm
            if self._random.random() < self.beta and len(viable_nodes) >= 2:
                # Use power-of-two with cached loads
                selected_node = self._power_of_two_selection(task, viable_nodes)
                self.power_of_two_decisions += 1
            else:
                # Use single best node from cached loads
                selected_node = self._best_cached_selection(task, viable_nodes)
                self.cached_decisions += 1
            
            if selected_node:
                # Calculate network delay for task assignment
                from ..core.network import MessageType, ComponentType, NetworkDelayModel
                
                # For now, use a simple 2ms + 0.5ms std network delay
                # In full integration, this would come from the simulation engine's network model
                import random
                assignment_delay = max(0.1, random.normalvariate(2.0, 0.5))
                
                # Create task assignment event with network delay
                assignment_event = Event(
                    event_id=self._generate_event_id(),
                    timestamp=current_time + assignment_delay,
                    event_type=EventType.TASK_SCHEDULED,
                    source_id=self.scheduler_id,
                    target_id=selected_node,
                    data={'task': task, 'assignment_method': 'dodoor_cached'}
                )
                
                events.append(assignment_event)
                
                # Update local load tracking for batching
                self._track_load_update(selected_node, task, current_time)
                
                logger.debug(f"Scheduled task {task.task_id} to {selected_node} using Dodoor")
        
        return events
    
    def _power_of_two_selection(self, task: Task, viable_nodes: List[str]) -> str:
        """Select node using power-of-two with cached loads."""
        # Sample two nodes randomly
        sampled_nodes = self._random.sample(viable_nodes, min(2, len(viable_nodes)))
        
        # Select least loaded node from samples
        best_node = None
        best_score = float('inf')
        
        for node_id in sampled_nodes:
            score = self._get_cached_load_score(node_id, task)
            if score < best_score:
                best_score = score
                best_node = node_id
        
        return best_node
    
    def _best_cached_selection(self, task: Task, viable_nodes: List[str]) -> str:
        """Select best node based on cached load scores."""
        best_node = None
        best_score = float('inf')
        
        for node_id in viable_nodes:
            score = self._get_cached_load_score(node_id, task)
            if score < best_score:
                best_score = score
                best_node = node_id
        
        return best_node
    
    def _get_cached_load_score(self, node_id: str, task: Task) -> float:
        """Calculate load score using cached information."""
        if node_id in self.cached_loads:
            cached_load = self.cached_loads[node_id]
            
            # Use Dodoor's multi-dimensional load scoring
            resource_score = self.load_calculator.calculate_resource_load_score(
                # Create ResourceVector from cached data
                ResourceVector(cores=cached_load.cpu_utilization * 100,  # Convert to usage
                             memory=int(cached_load.memory_utilization * 100),
                             disk=0),
                task.resource_request,
                ResourceVector(cores=100, memory=100, disk=0)  # Normalized capacity
            )
            
            # Combine with duration load
            duration_weight = self.config.weights.duration
            duration_score = cached_load.total_duration_ms / 1000.0  # Normalize
            
            total_score = resource_score * (1 - duration_weight) + duration_score * duration_weight
            return total_score
        
        # If no cached data, use high penalty score
        return float('inf')
    
    def _track_load_update(self, node_id: str, task: Task, current_time: float):
        """Track load update for batching to DataStore."""
        
        # Update or create load entry for this node
        if node_id in self.cached_loads:
            cached_load = self.cached_loads[node_id]
            updated_load = NodeLoad(
                node_id=node_id,
                cpu_utilization=min(1.0, cached_load.cpu_utilization + task.resource_request.cores / 100),
                memory_utilization=min(1.0, cached_load.memory_utilization + task.resource_request.memory / 100),
                queue_length=cached_load.queue_length + 1,
                total_duration_ms=cached_load.total_duration_ms + task.duration_ms,
                last_updated=current_time
            )
        else:
            # Create new load entry
            updated_load = NodeLoad(
                node_id=node_id,
                cpu_utilization=task.resource_request.cores / 100,
                memory_utilization=task.resource_request.memory / 100,
                queue_length=1,
                total_duration_ms=task.duration_ms,
                last_updated=current_time
            )
        
        # Add to pending updates for batching
        self.pending_load_updates.append(updated_load)
        
        # Update local cache
        self.cached_loads[node_id] = updated_load
    
    def _create_batch_load_update(self, current_time: float) -> Optional[Event]:
        """Create batch load update event for DataStore."""
        if not self.pending_load_updates:
            return None
        
        # Create batch update event
        event = Event(
            event_id=self._generate_event_id(),
            timestamp=current_time,
            event_type=EventType.BATCH_LOAD_UPDATE,
            source_id=self.scheduler_id,
            target_id="datastore",
            data={'load_updates': self.pending_load_updates.copy()}
        )
        
        logger.debug(f"Sending batch load update: {len(self.pending_load_updates)} nodes")
        
        # Clear pending updates
        self.pending_load_updates.clear()
        self.tasks_since_update = 0
        self.batch_updates_sent += 1
        
        return event
    
    def _create_node_states_from_cache(self) -> Dict[str, NodeState]:
        """Create NodeState objects from cached load information."""
        # This is a simplified conversion - in reality, we'd need more complete state
        node_states = {}
        
        for node_id, cached_load in self.cached_loads.items():
            # Create minimal NodeState for compatibility
            capacity = ResourceVector(cores=100, memory=100, disk=0)  # Normalized
            allocated = ResourceVector(
                cores=cached_load.cpu_utilization * 100,
                memory=cached_load.memory_utilization * 100,
                disk=0
            )
            
            node_state = NodeState(
                node_id=node_id,
                node_type="cached",
                capacity=capacity,
                allocated=allocated,
                num_tasks=cached_load.queue_length,
                total_duration_ms=cached_load.total_duration_ms
            )
            
            node_states[node_id] = node_state
        
        return node_states
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID."""
        return random.randint(1000000, 9999999)
    
    def get_statistics(self) -> Dict:
        """Get Dodoor-specific statistics."""
        stats = super().get_statistics()
        
        cache_hit_rate = self.cache_hits / max(1, self.cache_hits + self.cache_misses)
        
        stats['dodoor_stats'] = {
            'beta': self.beta,
            'batch_size': self.batch_size,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': cache_hit_rate,
            'datastore_queries': self.datastore_queries,
            'batch_updates_sent': self.batch_updates_sent,
            'power_of_two_decisions': self.power_of_two_decisions,
            'cached_decisions': self.cached_decisions,
            'pending_updates': len(self.pending_load_updates)
        }
        return stats


class PendingDataStoreQuery:
    """Track information about a pending DataStore query."""
    
    def __init__(self, query_id: str, node_ids: List[str], sent_time: float):
        self.query_id = query_id
        self.node_ids = node_ids
        self.sent_time = sent_time


# Import ResourceVector here to avoid circular imports
try:
    from ..schedulers.base_scheduler import ResourceVector
except ImportError:
    from schedulers.base_scheduler import ResourceVector