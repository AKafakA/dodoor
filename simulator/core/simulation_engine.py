"""
Main discrete event simulation engine.

This module provides the core simulation engine that orchestrates the 
discrete event simulation of the Dodoor distributed scheduling system.
"""

import logging
import json
from typing import Dict, Any, Optional
from pathlib import Path
import time

try:
    from .events import EventScheduler, EventType, Event
    from .network import NetworkSimulator, NetworkDelayModel
    from .metrics import SimulationMetrics
    from ..config.simulation_config import SimulationConfig
    from ..schedulers.base_scheduler import ResourceVector
except ImportError:
    from core.events import EventScheduler, EventType, Event
    from core.network import NetworkSimulator, NetworkDelayModel
    from core.metrics import SimulationMetrics
    from config.simulation_config import SimulationConfig
    from schedulers.base_scheduler import ResourceVector

logger = logging.getLogger(__name__)

class SimulationEngine:
    """
    Main simulation engine coordinating all simulation components.
    """
    
    def __init__(self, config: SimulationConfig, output_dir: Path):
        """Initialize simulation engine with configuration."""
        self.config = config
        self.output_dir = output_dir
        
        # Core simulation components
        self.event_scheduler = EventScheduler()
        self.network_model = NetworkDelayModel(config.cluster.network, config.experiment.seed)
        self.network_simulator = NetworkSimulator(self.network_model)
        self.metrics = SimulationMetrics(config.experiment.name)

        # FIXED: Track scheduled completion events to prevent duplicates
        self.scheduled_completions = set()
        # NEW: End flag to allow early termination when target tasks reached
        self._ended = False
        
        logger.info(f"Initialized simulation engine: {config.experiment.name}")
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete simulation.
        
        Returns:
            Dictionary with simulation results and statistics
        """
        logger.info("Starting discrete event simulation")
        
        # Record simulation start
        self.metrics.set_simulation_times(0.0, self.config.experiment.duration_ms)
        
        try:
            # Initialize simulation components
            self._initialize_simulation()
            
            # Register event handlers
            self._register_event_handlers()
            
            # Run main simulation loop
            self._run_simulation()
            
            # Collect final results
            results = self._finalize_simulation()
            
            logger.info("Simulation completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Simulation failed: {e}")
            raise
    
    def _initialize_simulation(self):
        """Initialize simulation components and schedule initial events."""
        logger.info("Initializing simulation components")
        
        # Initialize scheduler first (so we know scheduler type for node setup)
        self._initialize_scheduler()
        
        # Initialize cluster nodes (may depend on scheduler type e.g., late-binding)
        self._initialize_cluster()
        
        # Initialize and schedule workload
        self._initialize_workload()
        
        # Schedule periodic metrics collection
        self._schedule_metrics_collection()
        
        # Initialize progressive logging
        # FIXED: Use "metrics" directory to match physical experiment format expected by plot_scheduler.py
        metrics_dir = self.output_dir / "metrics"
        self.metrics.initialize_progressive_logging(metrics_dir)
        
        # Schedule simulation end - implementing physical experiment timeout behavior
        # Physical experiments use: 30 min timeout OR 30000 tasks completed (whichever first)
        # If timeout_ms is explicitly None, run until all tasks complete (no timeout)
        timeout_ms = getattr(self.config.experiment, 'timeout_ms', None)
        if timeout_ms is None:
            # Check if timeout_ms was explicitly set to None (vs missing)
            if hasattr(self.config.experiment, 'timeout_ms') and self.config.experiment.timeout_ms is None:
                # Explicitly set to None - no timeout, run until all tasks complete
                timeout_ms = float('inf')
                logger.info("No timeout set - simulation will run until all tasks complete")
            else:
                # Not set - use duration as fallback
                duration_seconds = getattr(self.config.experiment, 'duration', 60)
                timeout_ms = duration_seconds * 1000

        if timeout_ms != float('inf'):
            self.event_scheduler.schedule_event_at(
                timeout_ms,
                EventType.SIMULATION_END,
                "simulation_engine",
                data={'reason': 'timeout_reached'}
            )
        
        # Also set task completion target (like physical NUM_REQUESTS)
        self.target_completed_tasks = getattr(self.config.experiment, 'target_completed_tasks', None)
        self.completed_tasks = 0
        
        logger.info(f"Scheduled {self.event_scheduler.events_remaining} initial events")
    
    def _run_simulation(self):
        """Run the main discrete event simulation loop with early stop on SIMULATION_END."""
        # Determine time limit based on timeout_ms (overrides duration_ms)
        # Priority: timeout_ms > target_completed_tasks > duration_ms
        timeout_ms = self.config.experiment.timeout_ms

        if timeout_ms is None:
            # No timeout specified - run until target_completed_tasks or forever
            if self.target_completed_tasks is not None:
                time_limit = float('inf')  # Will stop when target reached via SIMULATION_END event
                logger.info(f"Running simulation with no timeout - will stop when {self.target_completed_tasks} tasks complete")
            else:
                # No timeout and no target - use duration_ms as fallback
                time_limit = self.config.experiment.duration_ms
                logger.info(f"Running simulation for {self.config.experiment.duration_ms}ms (duration_ms fallback)")
        else:
            # Explicit timeout set - use it as hard limit
            time_limit = timeout_ms
            logger.info(f"Running simulation with {timeout_ms}ms timeout (physical experiment mode)")

        # Process events until time limit or explicit SIMULATION_END
        try:
            while True:
                next_event = self.event_scheduler.get_next_event()
                if not next_event:
                    break

                # Check time limit (skip if infinite)
                if time_limit != float('inf') and next_event.timestamp > time_limit:
                    logger.info(f"Simulation time limit reached: {time_limit}ms, stopping event processing")
                    break

                if self._ended:
                    break

                processed = self.event_scheduler.process_next_event()
                if not processed:
                    break
        except Exception as e:
            logger.error(f"Simulation loop error: {e}")

        logger.info(f"Processed {self.event_scheduler.events_processed} events")

        # Optional drain: continue processing until ALL tasks complete (executing + queued)
        try:
            if getattr(self.config.experiment, 'drain_on_finish', False):
                logger.info("Draining until ALL tasks complete (executing + queued)")

                # Keep processing events until all nodes are empty (no executing, no queued tasks)
                max_iterations = 10000  # Safety limit
                iteration = 0

                while iteration < max_iterations:
                    # Process all pending events
                    self.event_scheduler.run_until_empty()

                    # Check if all nodes are completely empty
                    total_executing = sum(len(node.executing_tasks) for node in self.node_executors.values())
                    total_queued = sum(len(node.task_reservations) for node in self.node_executors.values())

                    if total_executing == 0 and total_queued == 0:
                        logger.info(f"All tasks completed after {iteration + 1} drain iterations")
                        break

                    # Special handling for Sparrow late-binding
                    if self.scheduler.get_scheduler_type() == 'sparrow':
                        self._drain_sparrow_late_binding()

                    # Check if we made progress
                    if self.event_scheduler.events_remaining == 0:
                        logger.warning(f"Drain stalled: {total_executing} executing, {total_queued} queued, but no events remaining")
                        break

                    iteration += 1

                logger.info(f"Drain complete: processed {self.event_scheduler.events_processed} total events")
        except Exception as e:
            logger.warning(f"Drain failed or skipped: {e}")

    def _drain_sparrow_late_binding(self):
        """Actively drain Sparrow late-binding queues by issuing confirm requests until empty."""
        try:
            progress_made = True
            while progress_made:
                progress_made = False
                now = self.event_scheduler.current_time
                # First pass: remove reservations for tasks already completed and
                # deduplicate multiple reservations for the same task across nodes.
                already_completed = getattr(self, 'completed_task_ids', set()) if hasattr(self, 'completed_task_ids') else set()
                kept_tasks = set()
                for node_id, node_executor in self.node_executors.items():
                    if not getattr(node_executor, 'late_binding', False):
                        continue
                    # Work on a copy to allow in-place cancellation
                    queued_ids = [res.task.task_id for res in list(node_executor.task_reservations)]
                    for task_id in queued_ids:
                        if task_id in already_completed:
                            node_executor.cancel_reservation(task_id)
                            progress_made = True
                        elif task_id in kept_tasks:
                            node_executor.cancel_reservation(task_id)
                            progress_made = True
                        else:
                            kept_tasks.add(task_id)
                for node_id, node_executor in self.node_executors.items():
                    if not getattr(node_executor, 'late_binding', False):
                        continue
                    # Directly launch runnable reservations and cancel others
                    while node_executor.available_slots > 0:
                        runnable_task_id = node_executor.select_runnable_reservation()
                        if not runnable_task_id:
                            break
                        completion_time = node_executor.launch_confirmed_reservation(runnable_task_id, now)
                        if completion_time is None:
                            break
                        # Schedule completion
                        completion_key = (runnable_task_id, node_id, completion_time)
                        if completion_key not in self.scheduled_completions:
                            self.scheduled_completions.add(completion_key)
                            launched_task = self._find_task_by_id(runnable_task_id, node_executor)
                            self.event_scheduler.schedule_event(Event(
                                event_id=0,
                                timestamp=completion_time,
                                event_type=EventType.TASK_COMPLETED,
                                source_id=node_id,
                                target_id=None,
                                data={'task': launched_task, 'node_id': node_id}
                            ))
                        # Cancel duplicates on other nodes using scheduler bookkeeping when available
                        try:
                            nodes_to_cancel = []
                            if hasattr(self.scheduler, '_reservation_targets'):
                                nodes_to_cancel = [n for n in self.scheduler._reservation_targets.get(runnable_task_id, []) if n != node_id]
                            for cancel_node in nodes_to_cancel:
                                other_exec = self.node_executors.get(cancel_node)
                                if other_exec:
                                    other_exec.cancel_reservation(runnable_task_id)
                            # Cleanup scheduler maps
                            if hasattr(self.scheduler, '_preserved_nodes'):
                                self.scheduler._preserved_nodes.pop(runnable_task_id, None)
                            if hasattr(self.scheduler, '_task_reservations'):
                                self.scheduler._task_reservations.pop(runnable_task_id, None)
                            if hasattr(self.scheduler, '_reservation_targets'):
                                self.scheduler._reservation_targets.pop(runnable_task_id, None)
                        except Exception:
                            pass
                        progress_made = True
                # Process completions and any triggered follow-ups
                if progress_made:
                    self.event_scheduler.run_until_empty()

                # Check if all queues are empty
                all_empty = True
                for node_executor in self.node_executors.values():
                    if node_executor.task_reservations or node_executor.executing_tasks:
                        all_empty = False
                        break
                if all_empty:
                    break
        except Exception as e:
            logger.warning(f"Sparrow drain encountered an error: {e}")
    
    def _finalize_simulation(self) -> Dict[str, Any]:
        """Finalize simulation and collect results."""
        logger.info("Finalizing simulation and collecting results")
        
        # Generate metrics summary
        summary = self.metrics.get_experiment_summary()
        
        # Save metrics to file
        metrics_file = self.output_dir / self.config.output.metrics_file
        self.metrics.save_to_file(str(metrics_file))
        
        # Finalize progressive logging and generate physical experiment format logs
        # FIXED: Use "metrics" directory to match physical experiment format expected by plot_scheduler.py
        metrics_dir = self.output_dir / "metrics"
        self.metrics.generate_physical_format_logs(metrics_dir)
        
        return {
            'simulation_summary': summary,
            'network_stats': self.network_simulator.get_statistics(),
            'events_processed': self.event_scheduler.events_processed,
            'simulation_time_ms': self.config.experiment.duration_ms
        }
    
    def _initialize_cluster(self):
        """Initialize cluster nodes with slot-based executors."""
        try:
            from ..schedulers.base_scheduler import ResourceVector
            from .node_executor import SlotBasedNodeExecutor
        except ImportError:
            from schedulers.base_scheduler import ResourceVector
            from core.node_executor import SlotBasedNodeExecutor
        
        self.nodes = {}
        self.node_executors = {}  # New: slot-based executors
        
        for node_type_config in self.config.cluster.node_types:
            for i in range(node_type_config.count):
                node_id = f"{node_type_config.type}_{i:03d}"
                
                capacity = ResourceVector(
                    cores=node_type_config.cores,
                    memory=node_type_config.memory,
                    disk=node_type_config.disk
                )
                
                # Create slot-based node executor (matches Java NodeImpl behavior)
                num_slots = getattr(node_type_config, 'slots', 4)  # Default 4 slots like Java
                restrict_fifo = getattr(self.config.cluster, 'restrict_fifo', True)
                replay_with_disk = getattr(self.config.experiment, 'replay_with_disk', False)

                # Enable late-binding ONLY for Sparrow scheduler (matches Java SchedulerUtils.isLateBindingScheduler)
                # CRITICAL FIX: Late-binding is scheduler-specific, not a per-node configuration
                # Java: isLateBindingScheduler returns true ONLY for SPARROW_SCHEDULER
                try:
                    is_sparrow = self.scheduler.get_scheduler_type() == 'sparrow'
                except Exception:
                    is_sparrow = False

                node_executor = SlotBasedNodeExecutor(
                    node_id=node_id,
                    node_type=node_type_config.type,
                    capacity=capacity,
                    num_slots=num_slots,
                    restrict_fifo=restrict_fifo,
                    replay_with_disk=replay_with_disk,
                    late_binding=is_sparrow  # ONLY Sparrow uses late-binding
                )
                
                self.node_executors[node_id] = node_executor
                
                # Create NodeState for scheduler compatibility (derived from executor state)
                self.nodes[node_id] = node_executor.get_node_state()
                
                # Initialize metrics for this node
                self.metrics.get_or_create_component(node_id, "node")
        
        logger.info(f"Initialized {len(self.nodes)} slot-based nodes across "
                   f"{len(self.config.cluster.node_types)} node types, "
                   f"restrict_fifo={restrict_fifo}")
    
    def _initialize_scheduler(self):
        """Initialize scheduler component using unified TaskPlacer approach."""
        try:
            from ..schedulers.unified_scheduler import UnifiedScheduler
        except ImportError:
            from schedulers.unified_scheduler import UnifiedScheduler
        
        # Get scheduler type from config
        scheduler_type = self.config.scheduler.type
        if not isinstance(scheduler_type, str):
            scheduler_type = scheduler_type.value
        
        # Set scheduler type in config for UnifiedScheduler
        self.config.scheduler.scheduler_type = scheduler_type
        
        # Create unified scheduler that handles all types via TaskPlacer interface
        self.scheduler = UnifiedScheduler(self.config.scheduler, "main_scheduler")

        if self.config.experiment.seed:
            self.scheduler.set_random_seed(self.config.experiment.seed)

        # Load task profile configuration if provided (matches merged_profiler_config usage)
        task_profile_file = getattr(self.config.workload, 'task_profile_file', None)
        if task_profile_file:
            profile_path = Path(task_profile_file)
            if not profile_path.is_absolute():
                repo_root = Path(__file__).resolve().parents[2]
                candidate = repo_root / profile_path
                profile_path = candidate if candidate.exists() else profile_path

            try:
                with open(profile_path, 'r') as f:
                    profile_config = json.load(f)
                self.scheduler.set_task_profile_config(profile_config)
                logger.info(f"Loaded task profile config from {profile_path}")
            except Exception as e:
                logger.error(f"Failed to load task profile config {profile_path}: {e}")

        # Initialize scheduler metrics
        self.metrics.get_or_create_component("main_scheduler", "scheduler")

        logger.info(f"Initialized UnifiedScheduler (type: {scheduler_type})")
    
    def _initialize_workload(self):
        """Initialize and schedule workload events."""
        workload_type = self.config.workload.type
        if (isinstance(workload_type, str) and workload_type == 'trace') or \
           (hasattr(workload_type, 'value') and workload_type.value == 'trace'):
            self._initialize_trace_workload()
        else:
            self._initialize_synthetic_workload()
    
    def _initialize_synthetic_workload(self):
        """Initialize synthetic workload generation."""
        try:
            from ..workload.trace_reader import TaskConfigReader, WorkloadGenerator
        except ImportError:
            from workload.trace_reader import TaskConfigReader, WorkloadGenerator
        
        # Use default task types unless an explicit JSON profile is provided
        task_reader = None
        try:
            tf = getattr(self.config.workload, 'trace_file', None)
            if tf and isinstance(tf, str) and tf.endswith('.json'):
                task_reader = TaskConfigReader(tf)
        except Exception:
            task_reader = None
        
        synthetic_config = self.config.workload.synthetic
        
        # Create workload generator
        if task_reader:
            self.workload_generator = WorkloadGenerator(
                task_reader=task_reader,
                arrival_rate=synthetic_config.arrival_rate,
                task_mix=synthetic_config.task_mix,
                seed=self.config.experiment.seed
            )
        else:
            # Use simplified workload generation
            self.workload_generator = None
            
        # Schedule workload events
        self._schedule_synthetic_tasks()
    
    def _schedule_synthetic_tasks(self):
        """Schedule synthetic task arrivals."""
        try:
            from ..schedulers.base_scheduler import Task, ResourceVector
        except ImportError:
            from schedulers.base_scheduler import Task, ResourceVector
        
        import random
        task_random = random.Random(self.config.experiment.seed)
        
        # Generate task arrival times using Poisson process
        current_time = self.config.experiment.warmup_duration_ms
        end_time = self.config.experiment.duration_ms
        arrival_rate = self.config.workload.synthetic.arrival_rate / 1000.0  # Convert to per-ms
        
        task_id_counter = 0
        
        while current_time < end_time:
            # Sample inter-arrival time (exponential distribution)
            inter_arrival = task_random.expovariate(arrival_rate)
            current_time += inter_arrival
            
            if current_time >= end_time:
                break
            
            # Create task
            task_id_counter += 1
            task_id = f"task_{task_id_counter:06d}"
            
            # Simple task generation
            task = Task(
                task_id=task_id,
                task_type="default_task",
                resource_request=ResourceVector(
                    cores=task_random.uniform(0.5, 2.0),
                    memory=task_random.randint(512, 2048),
                    disk=0
                ),
                duration_ms=task_random.randint(100, 1000),
                submission_time=current_time
            )
            
            # Schedule task submission event
            self.event_scheduler.schedule_event_at(
                current_time,
                EventType.TASK_SUBMISSION,
                "workload_generator",
                data={'task': task}
            )
        
        logger.info(f"Scheduled {task_id_counter} synthetic tasks")
    
    def _initialize_trace_workload(self):
        """Initialize Azure trace-based workload."""
        try:
            from ..workload.azure_trace_reader import AzureTraceReader, AzureTraceWorkloadGenerator
        except ImportError:
            from workload.azure_trace_reader import AzureTraceReader, AzureTraceWorkloadGenerator
        
        # Get trace file path - default to test_data if not specified
        trace_file = self.config.workload.trace_file
        if not trace_file:
            trace_file = "deploy/resources/data/azure_data/test_data"

        # Resolve path robustly relative to project root or current working directory
        from pathlib import Path
        trace_path = Path(trace_file)
        if not trace_path.is_absolute():
            # If provided relative path exists from current working directory, use it
            if not trace_path.exists():
                # Try relative to repository root (three levels up from this file)
                repo_root = Path(__file__).resolve().parents[2]
                candidate = repo_root / trace_path
                trace_path = candidate if candidate.exists() else trace_path

        logger.info(f"Loading Azure trace workload from {trace_path}")
        
        try:
            # Load Azure trace with replay_with_disk setting (matches Java TaskTracePlayer.java)
            replay_with_disk = getattr(self.config.experiment, 'replay_with_disk', False)
            trace_reader = AzureTraceReader(str(trace_path), replay_with_disk=replay_with_disk)
            stats = trace_reader.get_task_stats()
            logger.info(f"Azure trace stats: {stats}")
            
            # Get target QPS from synthetic config if available
            target_qps = None
            if self.config.workload.synthetic:
                target_qps = self.config.workload.synthetic.arrival_rate
            
            # Create workload generator
            self.azure_workload_generator = AzureTraceWorkloadGenerator(
                trace_reader=trace_reader,
                target_qps=target_qps,
                seed=self.config.experiment.seed,
                simulation_duration_ms=self.config.experiment.duration_ms
            )
            
            # Schedule Azure trace tasks
            self._schedule_azure_trace_tasks()
            
        except Exception as e:
            logger.error(f"Failed to load Azure trace: {e}")
            logger.info("Falling back to synthetic workload")
            self._initialize_synthetic_workload()
    
    def _schedule_azure_trace_tasks(self):
        """Schedule Azure trace task arrivals."""
        # Generate tasks for the simulation duration
        start_time = self.config.experiment.warmup_duration_ms
        duration = self.config.experiment.duration_ms - start_time
        
        target_tasks = getattr(self.config.experiment, 'target_completed_tasks', None)
        tasks = self.azure_workload_generator.generate_workload(
            duration, start_time, target_tasks=target_tasks
        )

        if target_tasks is not None and len(tasks) < target_tasks:
            logger.warning(
                f"Azure workload generated only {len(tasks)} tasks (< target {target_tasks})."
            )

        # Schedule each task as an event
        for task in tasks:
            self.event_scheduler.schedule_event_at(
                task.submission_time,
                EventType.TASK_SUBMISSION,
                "azure_workload_generator",
                data={'task': task}
            )
        
        logger.info(f"Scheduled {len(tasks)} Azure trace tasks")
    
    def _schedule_metrics_collection(self):
        """Schedule periodic metrics collection events."""
        # Use 10-second intervals to match physical system (MetricsTrackerService and Slf4jReporter)
        interval_ms = 10000.0  # 10 seconds like the physical system DEFAULT_TRACKING_INTERVAL
        current_time = interval_ms
        
        while current_time <= self.config.experiment.duration_ms:
            self.event_scheduler.schedule_event_at(
                current_time,
                EventType.METRICS_COLLECTION,
                "simulation_engine",
                data={'collection_time': current_time}
            )
            current_time += interval_ms
        
        logger.info(f"Scheduled metrics collection every {interval_ms}ms")
    
    def _register_event_handlers(self):
        """Register event handlers for simulation events."""
        self.event_scheduler.register_handler(EventType.TASK_SUBMISSION, self)
        self.event_scheduler.register_handler(EventType.TASK_SCHEDULED, self)
        self.event_scheduler.register_handler(EventType.TASK_STARTED, self)
        self.event_scheduler.register_handler(EventType.TASK_COMPLETED, self)
        self.event_scheduler.register_handler(EventType.METRICS_COLLECTION, self)
        self.event_scheduler.register_handler(EventType.SIMULATION_END, self)

        # Sparrow late-binding event handlers
        self.event_scheduler.register_handler(EventType.SPARROW_CONFIRM_REQUEST, self)
        self.event_scheduler.register_handler(EventType.SPARROW_CONFIRM_RESPONSE, self)
        self.event_scheduler.register_handler(EventType.SPARROW_CANCEL_RESERVATION, self)
        
        # REMOVED: Fictional message-passing protocol handlers
        # These events don't exist in the Java system - scheduling is done via direct method calls
        # with overhead modeling preserved in UnifiedScheduler._calculate_scheduling_overhead()
        
        logger.info("Registered event handlers")
    
    def handle_event(self, event):
        """Handle simulation events."""
        if event.event_type == EventType.TASK_SUBMISSION:
            return self._handle_task_submission(event)
        elif event.event_type == EventType.TASK_SCHEDULED:
            return self._handle_task_scheduled(event)
        elif event.event_type == EventType.TASK_STARTED:
            return self._handle_task_started(event)
        elif event.event_type == EventType.TASK_COMPLETED:
            return self._handle_task_completion(event)
        elif event.event_type == EventType.METRICS_COLLECTION:
            return self._handle_metrics_collection(event)
        elif event.event_type == EventType.SIMULATION_END:
            return self._handle_simulation_end(event)
        elif event.event_type == EventType.SPARROW_CONFIRM_REQUEST:
            return self._handle_sparrow_confirm_request(event)
        elif event.event_type == EventType.SPARROW_CONFIRM_RESPONSE:
            return self._handle_sparrow_confirm_response(event)
        elif event.event_type == EventType.SPARROW_CANCEL_RESERVATION:
            return self._handle_sparrow_cancel_reservation(event)

        # REMOVED: Fictional message-passing protocol event handlers
        # Java system uses direct method calls with scheduling overhead modeling instead

        return None
    
    def _handle_task_submission(self, event):
        """Handle task submission event."""
        task = event.data['task']

        logger.info(f"DEBUG _handle_task_submission ENTRY: task={task.task_id}, event_id={event.event_id}, time={event.timestamp}")

        # Record task submission in metrics
        self.metrics.record_task_submission(task.task_id, task.task_type, event.timestamp)

        # Schedule task using scheduler (returns events for message-passing protocols)
        scheduler_events = self.scheduler.schedule_tasks([task], self.nodes, event.timestamp)

        logger.info(f"DEBUG _handle_task_submission: scheduler returned {len(scheduler_events)} events for task={task.task_id}")

        # Return scheduler events - event loop will schedule them automatically
        # DON'T manually schedule them here or they'll be scheduled twice!
        return scheduler_events if scheduler_events else None
    
    def _handle_task_scheduled(self, event):
        """Handle task scheduled event using slot-based executor."""
        task = event.data['task']
        assigned_node = event.target_id

        logger.info(f"DEBUG _handle_task_scheduled ENTRY: event_id={event.event_id}, task={task.task_id}, node={assigned_node}, timestamp={event.timestamp}")

        if assigned_node not in self.node_executors:
            logger.warning(f"Task assigned to unknown node {assigned_node}")
            return None

        node_executor = self.node_executors[assigned_node]

        # CRITICAL: Always enqueue task - never reject (matches Java behavior)
        logger.info(f"DEBUG ENGINE: Sending task {task.task_id} to node {assigned_node}")
        launched_tasks = node_executor.enqueue_task_reservation(task, event.timestamp)
        logger.info(f"DEBUG ENGINE: Node {assigned_node} returned {len(launched_tasks)} launched tasks")

        # Handle CONFIRM_REQUEST signals from late-binding nodes (Sparrow)
        # IMPORTANT: Only return events; do not schedule here to avoid duplicates.
        confirm_requests = []
        for item in launched_tasks:
            if isinstance(item, tuple) and len(item) == 2 and item[0] == 'CONFIRM_REQUEST':
                confirm_task_id = item[1]
                logger.info(f"SPARROW: Node {assigned_node} requesting confirm for task {confirm_task_id}")

                # Directly request confirm (avoid pre-allocation deadlocks in simulation)
                confirm_delay_ms = 1.0
                confirm_requests.append(Event(
                    event_id=0,
                    timestamp=event.timestamp + confirm_delay_ms,
                    event_type=EventType.SPARROW_CONFIRM_REQUEST,
                    source_id=assigned_node,
                    target_id=self.scheduler.scheduler_id,
                    data={
                        'task_id': confirm_task_id,
                        'node_id': assigned_node,
                        'request_time': event.timestamp
                    }
                ))

        # Filter out CONFIRM_REQUEST signals from launched_tasks for completion scheduling
        launched_tasks_filtered = [item for item in launched_tasks
                                   if not (isinstance(item, tuple) and len(item) == 2 and item[0] == 'CONFIRM_REQUEST')]
        if len(launched_tasks) > 0 and len(launched_tasks_filtered) != len(launched_tasks):
            logger.info(f"Filtered {len(launched_tasks) - len(launched_tasks_filtered)} CONFIRM_REQUEST from {len(launched_tasks)} items")
        launched_tasks = launched_tasks_filtered
        
        # Record scheduling decision
        self.metrics.record_task_scheduled(
            task.task_id, event.timestamp, assigned_node
        )
        
        # Record network messages and scheduling overhead if present
        if 'network_messages' in event.data and 'scheduling_overhead_ms' in event.data:
            network_messages = event.data['network_messages']
            overhead_ms = event.data['scheduling_overhead_ms']
            
            # Record each network message with the scheduling overhead as latency
            for _ in range(network_messages):
                self.metrics.record_network_message(
                    size_bytes=1024,  # Default message size
                    latency_ms=overhead_ms / network_messages  # Distributed latency
                )
            
            # Record scheduling latency for this task
            self.metrics.record_scheduling_latency(
                task.task_id, overhead_ms, event.timestamp, assigned_node
            )
            
            logger.debug(f"Recorded {network_messages} network messages with {overhead_ms:.2f}ms total overhead")
        
        # Update NodeState for scheduler compatibility
        self.nodes[assigned_node] = node_executor.get_node_state()

        logger.debug(f"Task {task.task_id} enqueued to {assigned_node} at {event.timestamp}")

        # If Sparrow: perform confirm handshake before launching
        new_events = []
        try:
            is_sparrow = self.scheduler.get_scheduler_type() == 'sparrow'
        except Exception:
            is_sparrow = False

        if not is_sparrow:
            # Schedule completion events for any tasks that were launched immediately
            if len(launched_tasks) > 0:
                logger.info(f"Scheduling {len(launched_tasks)} completion events for non-Sparrow scheduler")
            for launched_task_id, completion_time in launched_tasks:
                completion_key = (launched_task_id, assigned_node, completion_time)
                if completion_key in self.scheduled_completions:
                    logger.info(f"Completion already scheduled for task {launched_task_id} at {completion_time}")
                    continue
                self.scheduled_completions.add(completion_key)

                if launched_task_id == task.task_id:
                    launched_task = task
                else:
                    launched_task = self._find_task_in_executor(launched_task_id, node_executor)
                    if not launched_task or launched_task.task_id == "unknown":
                        logger.warning(f"Could not find task {launched_task_id} for completion scheduling")
                        continue

                new_events.append(Event(
                    event_id=0,
                    timestamp=completion_time,
                    event_type=EventType.TASK_COMPLETED,
                    source_id=assigned_node,
                    target_id=None,
                    data={'task': launched_task, 'node_id': assigned_node}
                ))
                logger.info(f"Created TASK_COMPLETED event for {launched_task_id} at time {completion_time}")

            # Guarded: keep debug safe
            try:
                if 'launched_task_id' in locals():
                    logger.debug(f"Scheduled completion for task {launched_task_id} at {completion_time}")
            except Exception:
                pass
        # Note: For Sparrow, CONFIRM_REQUEST events will handle confirmation
        # Do NOT attempt immediate confirm here to avoid race condition where
        # preserved_nodes gets cleared before all CONFIRM_REQUEST events fire

        # Add confirm request events
        new_events.extend(confirm_requests)

        if new_events:
            logger.info(f"_handle_task_scheduled returning {len(new_events)} new events")
        return new_events if new_events else None
    
    def _handle_task_started(self, event):
        """Handle task started event."""
        task = event.data['task']
        node_id = event.data['node_id']
        
        # Record task start in metrics
        self.metrics.record_task_started(task.task_id, event.timestamp, node_id)
        
        logger.debug(f"Task {task.task_id} started on {node_id} at {event.timestamp}")
        
        return None
    
    def _handle_task_completion(self, event):
        """Handle task completion event using slot-based executor."""
        task = event.data['task']
        node_id = event.data['node_id']

        # FIXED: Remove from scheduled completions set
        completion_key = (task.task_id, node_id, event.timestamp)
        self.scheduled_completions.discard(completion_key)

        # Handle completion through slot-based executor FIRST
        # to verify task actually completed on this node
        if node_id not in self.node_executors:
            logger.warning(f"Task {task.task_id} completion event for unknown node {node_id}")
            return None

        node_executor = self.node_executors[node_id]

        # Check if this task is actually executing on this node
        # node_executor.task_completed() will return [] if task not found
        task_found_on_node = any(
            et.task.task_id == task.task_id
            for et in node_executor.executing_tasks.values()
        )

        if not task_found_on_node:
            # Task not executing on this node - this is a spurious completion event
            logger.debug(f"Task {task.task_id} completion event on {node_id} but task not executing there, ignoring")
            return None

        # Process task completion (frees resources, launches next tasks)
        newly_launched = node_executor.task_completed(task.task_id, event.timestamp)

        # Now handle metrics and deduplication
        if not hasattr(self, 'completed_task_ids'):
            self.completed_task_ids = set()

        # Check if this is the first completion of this task globally
        is_first_completion = task.task_id not in self.completed_task_ids

        if is_first_completion:
            # First completion: record in metrics and increment counter
            self.completed_task_ids.add(task.task_id)
            self.metrics.record_task_completed(task.task_id, event.timestamp, node_id)
            self.completed_tasks += 1

            # Check if we should end simulation due to task completion target
            if (self.target_completed_tasks is not None and
                self.completed_tasks >= self.target_completed_tasks):
                logger.info(f"Target completed tasks ({self.target_completed_tasks}) reached, ending simulation")
                # Schedule immediate simulation end
                return [self.event_scheduler.schedule_event_at(
                    event.timestamp,
                    EventType.SIMULATION_END,
                    "simulation_engine",
                    data={'reason': 'target_tasks_completed'}
                )]
        else:
            # Duplicate completion: already counted elsewhere, just log
            logger.debug(f"Task {task.task_id} already completed elsewhere, freed resources on {node_id} but not double-counting")

        # Update NodeState for scheduler compatibility
        self.nodes[node_id] = node_executor.get_node_state()

        # If Sparrow: attempt confirm to launch next task; otherwise, schedule immediate launches
        # CRITICAL FIX: This must happen for BOTH first and duplicate completions!
        new_events = []
        try:
            is_sparrow = self.scheduler.get_scheduler_type() == 'sparrow'
        except Exception:
            is_sparrow = False

        if is_sparrow:
            confirm_events = self._sparrow_attempt_confirm(node_id, event.timestamp)
            if confirm_events:
                new_events.extend(confirm_events)
        else:
            for launched_task_id, completion_time in newly_launched:
                completion_key = (launched_task_id, node_id, completion_time)
                if completion_key in self.scheduled_completions:
                    logger.debug(f"Completion already scheduled for task {launched_task_id} at {completion_time}")
                    continue
                self.scheduled_completions.add(completion_key)

                launched_task = self._find_task_by_id(launched_task_id, node_executor)
                if not launched_task or launched_task.task_id == "unknown":
                    logger.warning(f"Could not find task {launched_task_id} for completion scheduling")
                    continue

                new_events.append(self.event_scheduler.schedule_event_at(
                    completion_time,
                    EventType.TASK_COMPLETED,
                    node_id,
                    data={'task': launched_task, 'node_id': node_id}
                ))
            if newly_launched:
                # Only log when at least one task was launched
                last_launched_id, last_completion_time = newly_launched[-1]
                logger.debug(
                    f"Scheduled completion for newly launched task {last_launched_id} at {last_completion_time}"
                )

        logger.debug(f"Task {task.task_id} completed on {node_id} (total: {self.completed_tasks}), "
                    f"launched {len(newly_launched)} new tasks")

        return new_events if new_events else None

    def _sparrow_attempt_confirm(self, node_id: str, current_time: float, preferred_task_id: str | None = None):
        """Attempt late-binding confirm for Sparrow on a given node."""
        node_executor = self.node_executors.get(node_id)
        if not node_executor:
            return []

        # Prefer confirming the reservation for the specific task we just enqueued
        task_id = preferred_task_id
        if not task_id:
            # Fallback: Check if there is a runnable reservation for this node
            task_id = node_executor.select_runnable_reservation()
        if not task_id:
            # Instrumentation: no runnable reservation at confirm time
            try:
                self.metrics.get_or_create_component("main_scheduler", "scheduler").increment_counter(
                    "sparrow.no_runnable_reservation", 1
                )
            except Exception:
                pass
            return []

        # Ask scheduler to confirm
        try:
            confirmed, nodes_to_cancel = self.scheduler.confirm_task_ready(task_id, node_id, current_time)
        except Exception as e:
            logger.error(f"Scheduler confirm failed for task {task_id} on {node_id}: {e}")
            return []

        # Instrumentation: track confirm attempts
        try:
            sched_comp = self.metrics.get_or_create_component("main_scheduler", "scheduler")
            sched_comp.increment_counter("sparrow.confirm_attempts", 1)
            if confirmed:
                sched_comp.increment_counter("sparrow.confirm_success", 1)
            else:
                sched_comp.increment_counter("sparrow.confirm_rejected", 1)
            if nodes_to_cancel:
                sched_comp.increment_counter("sparrow.cancel_count", len(nodes_to_cancel))
        except Exception:
            pass

        events = []
        if confirmed:
            completion_time = node_executor.launch_confirmed_reservation(task_id, current_time)
            if completion_time is not None:
                # Prevent duplicate scheduling
                completion_key = (task_id, node_id, completion_time)
                if completion_key not in self.scheduled_completions:
                    self.scheduled_completions.add(completion_key)
                    # Retrieve task object
                    launched_task = self._find_task_by_id(task_id, node_executor)
                    events.append(self.event_scheduler.schedule_event_at(
                        completion_time,
                        EventType.TASK_COMPLETED,
                        node_id,
                        data={'task': launched_task, 'node_id': node_id}
                    ))
                # Divergence check (replay): if an expected confirm exists and differs
                try:
                    exp = None
                    if hasattr(self.scheduler, 'get_expected_confirm'):
                        exp = self.scheduler.get_expected_confirm(task_id)
                    if exp and exp != node_id:
                        self.metrics.get_or_create_component("main_scheduler", "scheduler").increment_counter(
                            "replay.divergent_confirm", 1
                        )
                        logger.warning(f"Replay divergence: expected confirm {exp} but confirmed {node_id} for task {task_id}")
                    if hasattr(self.scheduler, 'clear_expected_confirm'):
                        self.scheduler.clear_expected_confirm(task_id)
                except Exception:
                    pass
            # Cancel other reservations
            for other_node in nodes_to_cancel:
                other_executor = self.node_executors.get(other_node)
                if other_executor:
                    other_executor.cancel_reservation(task_id)
                    # Update cached state
                    self.nodes[other_node] = other_executor.get_node_state()
        else:
            # Not confirmed: record a generic rejection without additional inference
            self.metrics.get_or_create_component("main_scheduler", "scheduler").increment_counter(
                "sparrow.confirm_rejected", 1
            )

        # Update node state for this node
        self.nodes[node_id] = node_executor.get_node_state()

        return events
    
    def _find_task_by_id(self, task_id: str, node_executor):
        """Find task object by ID in the node executor."""
        # Search in executing tasks
        for executing_task in node_executor.executing_tasks.values():
            if executing_task.task.task_id == task_id:
                return executing_task.task
        
        # If not found, create a minimal task object (shouldn't happen in normal flow)
        logger.warning(f"Task {task_id} not found in node executor")
        try:
            from ..schedulers.base_scheduler import ResourceVector, Task
        except ImportError:
            from schedulers.base_scheduler import ResourceVector, Task
        return Task(task_id=task_id, task_type="unknown", 
                   resource_request=ResourceVector(), duration_ms=0, submission_time=0)
    
    def _find_task_in_executor(self, task_id: str, node_executor):
        """Find task object by ID in the node executor."""
        # Search in executing tasks
        for executing_task in node_executor.executing_tasks.values():
            if executing_task.task.task_id == task_id:
                return executing_task.task
        
        # Search in queued tasks
        for reservation in node_executor.task_reservations:
            if reservation.task.task_id == task_id:
                return reservation.task
        
        # If not found, create a minimal task object (shouldn't happen in normal flow)
        logger.warning(f"Task {task_id} not found in node executor")
        try:
            from ..schedulers.base_scheduler import ResourceVector, Task
        except ImportError:
            from schedulers.base_scheduler import ResourceVector, Task
        return Task(task_id=task_id, task_type="unknown", 
                   resource_request=ResourceVector(), duration_ms=0, submission_time=0)
    
    def _handle_metrics_collection(self, event):
        """Handle periodic metrics collection and logging (every 10 seconds like physical system)."""
        collection_time = event.data['collection_time']
        
        print(f"DEBUG: Handling metrics collection at time {collection_time}")
        
        # Collect and log scheduler metrics periodically
        self.metrics.log_scheduler_periodic_report(collection_time)
        
        # Collect resource usage metrics from all slot-based executors
        new_events = []
        for node_id, node_executor in self.node_executors.items():
            node_state = node_executor.get_node_state()
            cpu_util = node_state.allocated.cores / max(node_state.capacity.cores, 1e-6)
            mem_util = node_state.allocated.memory / max(node_state.capacity.memory, 1e-6)
            
            # Record metrics for summary (but this doesn't log immediately)
            self.metrics.record_resource_usage(
                node_id, collection_time, cpu_util, mem_util, 0.0
            )
            self.metrics.record_waiting_tasks(node_id, collection_time, node_executor.queue_length)
            
            # Log node metrics periodically
            self.metrics.log_node_periodic_report(node_id, collection_time, cpu_util, mem_util, 0.0)
            
            # Update cached NodeState for scheduler compatibility
            self.nodes[node_id] = node_state

            # Liveness guard for Sparrow late-binding: if FIFO head is runnable and there are
            # free slots, proactively trigger a confirm request to keep nodes busy.
            try:
                if self.scheduler.get_scheduler_type() == 'sparrow' and node_executor.late_binding:
                    if node_executor.available_slots > 0:
                        runnable_task_id = node_executor.select_runnable_reservation()
                        if runnable_task_id and runnable_task_id not in node_executor.pending_confirms:
                            node_executor.pending_confirms.add(runnable_task_id)
                            new_events.append(Event(
                                event_id=0,
                                timestamp=collection_time + 1.0,
                                event_type=EventType.SPARROW_CONFIRM_REQUEST,
                                source_id=node_id,
                                target_id=self.scheduler.scheduler_id,
                                data={
                                    'task_id': runnable_task_id,
                                    'node_id': node_id,
                                    'request_time': collection_time
                                }
                            ))
            except Exception:
                pass

        return new_events if new_events else None
    
    def _handle_simulation_end(self, event):
        """Handle simulation end event."""
        logger.info(f"Simulation ended: {event.data.get('reason', 'unknown')}")
        # Signal early termination so _run_simulation breaks out
        self._ended = True
        return None
    
    # Sparrow late-binding event handlers

    def _handle_sparrow_confirm_request(self, event):
        """Handle Sparrow confirm request from node to scheduler."""
        task_id = event.data['task_id']
        node_id = event.data['node_id']
        request_time = event.data['request_time']
        current_time = event.timestamp

        logger.info(f"SPARROW CONFIRM REQUEST: task={task_id}, node={node_id}, time={current_time}")

        # Ask scheduler to confirm (matches Java SchedulerImpl.confirmTaskReadyToExecute)
        confirmed, nodes_to_cancel = self.scheduler.confirm_task_ready(task_id, node_id, current_time)

        # Add small delay for confirm response (realistic network RTT)
        response_delay_ms = 1.0
        confirm_response = Event(
            event_id=0,
            timestamp=current_time + response_delay_ms,
            event_type=EventType.SPARROW_CONFIRM_RESPONSE,
            source_id=self.scheduler.scheduler_id,
            target_id=node_id,
            data={
                'task_id': task_id,
                'node_id': node_id,
                'confirmed': confirmed,
                'nodes_to_cancel': nodes_to_cancel,
                'request_time': request_time
            }
        )

        return [confirm_response]

    def _handle_sparrow_confirm_response(self, event):
        """Handle Sparrow confirm response from scheduler to node."""
        task_id = event.data['task_id']
        node_id = event.data['node_id']
        confirmed = event.data['confirmed']
        nodes_to_cancel = event.data['nodes_to_cancel']
        current_time = event.timestamp

        logger.info(f"SPARROW CONFIRM RESPONSE: task={task_id}, node={node_id}, confirmed={confirmed}, time={current_time}")

        node_executor = self.node_executors.get(node_id)
        if not node_executor:
            logger.warning(f"Node {node_id} not found for confirm response")
            return []

        events = []

        if confirmed:
            # Launch the confirmed task
            completion_time = node_executor.launch_confirmed_reservation(task_id, current_time)
            if completion_time is not None:
                # Schedule completion
                completion_key = (task_id, node_id, completion_time)
                if completion_key not in self.scheduled_completions:
                    self.scheduled_completions.add(completion_key)
                    launched_task = self._find_task_by_id(task_id, node_executor)
                    events.append(Event(
                        event_id=0,
                        timestamp=completion_time,
                        event_type=EventType.TASK_COMPLETED,
                        source_id=node_id,
                        target_id=None,
                        data={'task': launched_task, 'node_id': node_id}
                    ))

                logger.info(f"SPARROW CONFIRMED: task={task_id} launched on {node_id}, completion at {completion_time}")

            # Send cancellation messages to other nodes
            for cancel_node_id in nodes_to_cancel:
                cancel_delay_ms = 1.0  # Network delay for cancel message
                events.append(Event(
                    event_id=0,
                    timestamp=current_time + cancel_delay_ms,
                    event_type=EventType.SPARROW_CANCEL_RESERVATION,
                    source_id=self.scheduler.scheduler_id,
                    target_id=cancel_node_id,
                    data={
                        'task_id': task_id,
                        'cancel_node_id': cancel_node_id
                    }
                ))
        else:
            # Confirmation rejected - free pre-allocated resources
            # CRITICAL: Do NOT clear pending_confirms here! Task is still in queue.
            # Only clear when task is actually removed (via cancel or launch).
            node_executor.free_pre_allocated_resources(task_id)
            logger.info(f"SPARROW REJECTED: task={task_id} confirmation rejected for {node_id}")

        # Update node state
        self.nodes[node_id] = node_executor.get_node_state()

        # After confirm response, node may try to confirm next task
        if node_executor.late_binding and node_executor.available_slots > 0:
            next_runnable = node_executor.select_runnable_reservation()
            logger.info(f"DEBUG NEXT_RUNNABLE: task={task_id}, node={node_id}, next={next_runnable}, pending={node_executor.pending_confirms}")
            if next_runnable and next_runnable not in node_executor.pending_confirms:
                # Mark as pending and schedule confirm request
                node_executor.pending_confirms.add(next_runnable)
                logger.info(f"DEBUG SENDING_NEXT_CONFIRM: task={next_runnable}, node={node_id}, pending_after={node_executor.pending_confirms}")
                next_confirm_delay_ms = 1.0
                events.append(Event(
                    event_id=0,
                    timestamp=current_time + next_confirm_delay_ms,
                    event_type=EventType.SPARROW_CONFIRM_REQUEST,
                    source_id=node_id,
                    target_id=self.scheduler.scheduler_id,
                    data={
                        'task_id': next_runnable,
                        'node_id': node_id,
                        'request_time': current_time
                    }
                ))

        return events

    def _handle_sparrow_cancel_reservation(self, event):
        """Handle Sparrow cancel reservation from scheduler to node."""
        task_id = event.data['task_id']
        cancel_node_id = event.data['cancel_node_id']
        current_time = event.timestamp

        logger.info(f"SPARROW CANCEL: task={task_id}, node={cancel_node_id}, time={current_time}")

        node_executor = self.node_executors.get(cancel_node_id)
        if node_executor:
            # Cancel the reservation
            cancelled = node_executor.cancel_reservation(task_id)
            if cancelled:
                logger.info(f"SPARROW CANCELLED: task={task_id} reservation cancelled on {cancel_node_id}")

                # Update node state
                self.nodes[cancel_node_id] = node_executor.get_node_state()

                # After cancellation, node may try to confirm next task
                if node_executor.late_binding and node_executor.available_slots > 0:
                    next_runnable = node_executor.select_runnable_reservation()
                    if next_runnable and next_runnable not in node_executor.pending_confirms:
                        # Mark as pending and schedule confirm request
                        node_executor.pending_confirms.add(next_runnable)
                        next_confirm_delay_ms = 1.0
                        return [Event(
                            event_id=0,
                            timestamp=current_time + next_confirm_delay_ms,
                            event_type=EventType.SPARROW_CONFIRM_REQUEST,
                            source_id=cancel_node_id,
                            target_id=self.scheduler.scheduler_id,
                            data={
                                'task_id': next_runnable,
                                'node_id': cancel_node_id,
                                'request_time': current_time
                            }
                        )]

        return []

    # Message-passing protocol event handlers
    
