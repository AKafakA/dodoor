"""
Slot-based node executor implementation matching Java NodeImpl and FifoTaskScheduler.

This module implements the critical slot-based execution model with FIFO waiting queues
that exists in the physical Java system but was missing from the Python simulator.
"""

import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from collections import deque
import time

try:
    from ..schedulers.base_scheduler import Task, ResourceVector, NodeState
    from .events import EventScheduler, EventType
except ImportError:
    from schedulers.base_scheduler import Task, ResourceVector, NodeState
    from core.events import EventScheduler, EventType

logger = logging.getLogger(__name__)


@dataclass
class TaskReservation:
    """Task reservation in the waiting queue (matches Java TaskSpec)."""
    task: Task
    submission_time: float
    previous_task_id: Optional[str] = None


@dataclass 
class ExecutingTask:
    """Currently executing task in a slot."""
    task: Task
    start_time: float
    completion_time: float


class SlotBasedNodeExecutor:
    """
    Slot-based node executor matching Java NodeImpl and FifoTaskScheduler behavior.
    
    Key characteristics:
    - Fixed number of execution slots (typically 4-8)
    - FIFO waiting queue for tasks when slots full  
    - Resource-based head-of-line blocking
    - Tasks are NEVER rejected, always queued
    - Automatic queue processing when slots become available
    """
    
    def __init__(self, node_id: str, node_type: str, capacity: ResourceVector,
                 num_slots: int = 4, restrict_fifo: bool = False, replay_with_disk: bool = False,
                 late_binding: bool = False):
        """Initialize slot-based node executor."""
        self.node_id = node_id
        self.node_type = node_type
        self.capacity = capacity
        self.num_slots = num_slots
        self.restrict_fifo = restrict_fifo
        self.replay_with_disk = replay_with_disk
        
        # Execution slots (matches Java _taskLauncherService active tasks)
        self.executing_tasks: Dict[int, ExecutingTask] = {}  # slot_id -> ExecutingTask
        
        # FIFO waiting queue (matches Java _taskReservations)
        self.task_reservations: deque[TaskReservation] = deque()
        
        # Dual resource tracking (matches Java NodeImpl exactly)
        # 1. requested_resources: ALL enqueued tasks (matches Java _requestedCores/Memory/Disk)
        self.requested_resources = ResourceVector()
        # 2. allocated_resources: Only EXECUTING tasks (matches Java NodeResources)
        self.allocated_resources = ResourceVector()
        self.total_duration_ms = 0.0
        self.waiting_or_running_tasks_counter = 0
        
        # Head-of-line blocking tracking (matches Java _holBlockedTaskId/Ms)
        self.hol_blocked_task_id: Optional[str] = None
        self.hol_blocked_since_ms: Optional[float] = None
        
        # Late-binding mode (Sparrow)
        self.late_binding = late_binding

        # Track tasks awaiting cancel RPC (for late-binding confirmation rejection handling)
        self.awaiting_cancel: Set[str] = set()

        # Track tasks with pending confirmation requests (to prevent duplicate confirms)
        self.pending_confirms: Set[str] = set()

        # Track tasks with pre-allocated resources (confirmed but not yet launched)
        self.pre_allocated_tasks: Set[str] = set()

        logger.info(f"Initialized slot-based executor for {node_id}: "
                   f"{num_slots} slots, restrict_fifo={restrict_fifo}, replay_with_disk={replay_with_disk}")
    
    @property
    def available_slots(self) -> int:
        """Number of available execution slots."""
        return max(self.num_slots - len(self.executing_tasks), 0)
    
    @property
    def queue_length(self) -> int:
        """Length of waiting queue."""
        return len(self.task_reservations)
    
    @property 
    def available_resources(self) -> ResourceVector:
        """Available resources after current allocations."""
        return self.capacity - self.allocated_resources
    
    def can_run_task(self, task: Task) -> bool:
        """Check if task can run given current resource allocations."""
        available = self.available_resources
        cpu_ok = available.cores >= task.resource_request.cores
        mem_ok = available.memory >= task.resource_request.memory
        disk_ok = available.disk >= task.resource_request.disk

        result = cpu_ok and mem_ok and disk_ok

        logger.info(f"CAN_RUN_TASK DEBUG: {task.task_id} -> "
                   f"cpu_ok={cpu_ok} ({available.cores:.1f} >= {task.resource_request.cores:.1f}), "
                   f"mem_ok={mem_ok} ({available.memory:.0f} >= {task.resource_request.memory:.0f}), "
                   f"disk_ok={disk_ok} (node_disk={available.disk:.0f} >= task_disk={task.resource_request.disk:.0f}), "
                   f"replay_with_disk={self.replay_with_disk}, result={result}")

        return result
    
    def enqueue_task_reservation(self, task: Task, current_time: float) -> List[Tuple[str, float]]:
        """
        Enqueue task reservation (matches Java handleSubmitTaskReservation).

        CRITICAL: Tasks are NEVER rejected, always added to queue.
        This matches the Java behavior where line 76 ALWAYS adds to _taskReservations.

        Returns:
            List of (task_id, completion_time) for tasks that were launched immediately
        """
        logger.info(f"ENQUEUE START: Node {self.node_id} received task {task.task_id}, late_binding={self.late_binding}")
        reservation = TaskReservation(task=task, submission_time=current_time)
        launched_tasks = []

        # Update counters (matches Java NodeImpl atomic updates exactly)
        # Track requested resources immediately (matches Java lines 190-194)
        self.requested_resources += task.resource_request
        self.total_duration_ms += task.duration_ms
        self.waiting_or_running_tasks_counter += 1

        # Check if we can launch immediately (matches Java slot availability check)
        logger.info(f"DEBUG ENQUEUE: Node {self.node_id} received task {task.task_id}, "
                    f"available_slots={self.available_slots}, "
                    f"queue_length={len(self.task_reservations)}, "
                    f"restrict_fifo={self.restrict_fifo}, late_binding={self.late_binding}")

        if self.late_binding:
            # In late-binding mode, do NOT launch immediately. Just enqueue reservation.
            # Matches Java LateBindTaskScheduler: _taskReservations.add(taskReservation) ALWAYS happens
            self.task_reservations.append(reservation)
            logger.info(f"LATE_BINDING: Task {task.task_id} enqueued as reservation, "
                       f"will wait for scheduler confirm")

            # In Java, if slots are available and this could be run, node immediately tries to confirm
            # Matches Java LateBindTaskScheduler.handleSubmitTaskReservation line 64-77
            if self.available_slots > 0:
                runnable_task_id = self.select_runnable_reservation()
                if runnable_task_id and runnable_task_id not in self.pending_confirms:
                    if self.pre_allocate_for_confirm(runnable_task_id):
                        # Mark as pending to prevent duplicate confirms
                        self.pending_confirms.add(runnable_task_id)
                        logger.debug(f"PRE_ALLOCATE SUCCESS: task={runnable_task_id}, node={self.node_id}")
                        # Return confirmation request instead of immediate launch
                        # This will be handled by simulation engine to send SPARROW_CONFIRM_REQUEST event
                        return [('CONFIRM_REQUEST', runnable_task_id)]
                    else:
                        logger.debug(f"PRE_ALLOCATE FAILED: task={runnable_task_id}, node={self.node_id}")
        elif self.available_slots > 0:
            if self.restrict_fifo and self.task_reservations:
                # Restricted FIFO: must process head first
                first_reservation = self.task_reservations.popleft()
                self.task_reservations.append(reservation)

                if self.can_run_task(first_reservation.task):
                    slot_id = self._launch_task(first_reservation, current_time)
                    if slot_id >= 0:
                        completion_time = current_time + first_reservation.task.duration_ms
                        launched_tasks.append((first_reservation.task.task_id, completion_time))

                    # Try to launch more tasks greedily
                    additional_launched = self._attempt_task_launch(current_time, first_reservation.task.task_id)
                    launched_tasks.extend(additional_launched)
                else:
                    # Put first task back at head
                    self.task_reservations.appendleft(first_reservation)
                    logger.info(f"Restricted FIFO: head task {first_reservation.task.task_id} "
                               f"blocked, queued new task {task.task_id}")

            elif self.can_run_task(task):
                # Can launch immediately
                logger.info(f"DEBUG CAN_RUN: Task {task.task_id} can run, attempting launch")
                slot_id = self._launch_task(reservation, current_time)
                if slot_id >= 0:
                    completion_time = current_time + task.duration_ms
                    launched_tasks.append((task.task_id, completion_time))
                    logger.info(f"DEBUG LAUNCHED: Task {task.task_id} launched in slot {slot_id}")

                # CRITICAL FIX: Always attempt to launch more tasks when slots available
                # This applies to both restrict_fifo=True and restrict_fifo=False modes
                additional_launched = self._attempt_task_launch(current_time, task.task_id)
                launched_tasks.extend(additional_launched)
            else:
                # Insufficient resources, add to queue
                available = self.available_resources
                required = task.resource_request
                logger.info(f"DEBUG CANNOT_RUN: Task {task.task_id} queued - "
                           f"available: cpu={available.cores:.1f}, mem={available.memory:.0f}, "
                           f"required: cpu={required.cores:.1f}, mem={required.memory:.0f}")
                self.task_reservations.append(reservation)
        else:
            # No slots available, add to queue (CRITICAL: NEVER reject)
            self.task_reservations.append(reservation)
            logger.info(f"Task {task.task_id} queued: all {self.num_slots} slots occupied")

        logger.info(f"Node {self.node_id}: enqueued task {task.task_id}, "
                    f"launched {len(launched_tasks)} tasks immediately, "
                    f"queue_length={len(self.task_reservations)}, "
                    f"active_tasks={len(self.executing_tasks)}")

        return launched_tasks
    
    def _launch_task(self, reservation: TaskReservation, current_time: float,
                    preallocated: bool = False) -> int:
        """Launch task in available slot."""
        if self.available_slots == 0:
            logger.error(f"Attempted to launch task {reservation.task.task_id} with no slots")
            return -1
        
        # Find free slot
        slot_id = None
        for i in range(self.num_slots):
            if i not in self.executing_tasks:
                slot_id = i
                break
        
        if slot_id is None:
            logger.error(f"No free slot found despite available_slots > 0")
            return -1
        
        # Create executing task
        executing_task = ExecutingTask(
            task=reservation.task,
            start_time=current_time,
            completion_time=current_time + reservation.task.duration_ms
        )
        
        self.executing_tasks[slot_id] = executing_task

        # Allocate executing resources when task actually starts (matches Java NodeResources.runTaskIfPossible)
        # If resources were pre-allocated during confirm, skip double counting
        if reservation.task.task_id in self.pre_allocated_tasks or preallocated:
            self.pre_allocated_tasks.discard(reservation.task.task_id)
        else:
            self.allocated_resources += reservation.task.resource_request

        logger.debug(f"Launched task {reservation.task.task_id} in slot {slot_id} "
                    f"on node {self.node_id}, completion at {executing_task.completion_time}")

        return slot_id
    
    def task_completed(self, task_id: str, current_time: float) -> List[Tuple[str, float]]:
        """
        Handle task completion (matches Java taskFinished + attemptTaskLaunch).
        
        Returns list of (task_id, completion_time) for newly launched tasks.
        """
        # Find and remove completed task
        completed_slot = None
        completed_task = None
        
        for slot_id, executing_task in self.executing_tasks.items():
            if executing_task.task.task_id == task_id:
                completed_slot = slot_id
                completed_task = executing_task.task
                break
        
        if completed_slot is None:
            logger.warning(f"Task {task_id} completed but not found in executing tasks")
            return []
        
        # Remove from executing tasks
        del self.executing_tasks[completed_slot]
        
        # Update resource counters (matches Java taskFinished exactly - lines 140-145)
        # 1. Free requested resources (matches Java _requestedCores/Memory/Disk)
        self.requested_resources -= completed_task.resource_request
        # 2. Free allocated resources (matches Java NodeResources.freeTask)
        self.allocated_resources -= completed_task.resource_request
        self.total_duration_ms -= completed_task.duration_ms
        self.waiting_or_running_tasks_counter -= 1
        
        logger.debug(f"Task {task_id} completed on node {self.node_id}, "
                    f"freed slot {completed_slot}")
        
        # Attempt to launch waiting tasks (matches Java attemptTaskLaunch)
        if self.late_binding:
            # In late-binding mode, try to get scheduler confirmation
            # Matches Java LateBindTaskScheduler.handleTaskFinished -> attemptConfirmNextTaskReadyToRun
            runnable_task_id = self.select_runnable_reservation()
            if runnable_task_id:
                # Signal that confirmation is needed
                return [('CONFIRM_REQUEST', runnable_task_id)]
            else:
                return []
        else:
            newly_launched = self._attempt_task_launch(current_time, task_id)
            return newly_launched
    
    def _attempt_task_launch(self, current_time: float, 
                           last_executed_task_id: str) -> List[Tuple[str, float]]:
        """
        Attempt to launch tasks from waiting queue (matches Java attemptTaskLaunch).
        
        Returns list of (task_id, completion_time) for newly launched tasks.
        """
        newly_launched = []
        available_slots = self.available_slots
        
        if available_slots == 0:
            logger.debug(f"No free slots to launch new tasks "
                        f"({len(self.executing_tasks)} of {self.num_slots} filled)")
            return newly_launched
        
        if self.restrict_fifo:
            # Restricted FIFO: must process head-of-line only
            while available_slots > 0 and self.task_reservations:
                head_reservation = self.task_reservations[0]  # Peek at head
                
                if self.can_run_task(head_reservation.task):
                    # Remove and launch head task
                    self.task_reservations.popleft()
                    head_reservation.previous_task_id = last_executed_task_id
                    
                    slot_id = self._launch_task(head_reservation, current_time)
                    if slot_id >= 0:
                        completion_time = current_time + head_reservation.task.duration_ms
                        newly_launched.append((head_reservation.task.task_id, completion_time))
                        
                        # Clear head-of-line blocking if this was the blocked task
                        if (self.hol_blocked_task_id == head_reservation.task.task_id):
                            self.hol_blocked_task_id = None
                            self.hol_blocked_since_ms = None
                        
                        available_slots = self.available_slots
                    else:
                        break
                else:
                    # Head cannot run due to resources, block queue
                    if self.hol_blocked_task_id != head_reservation.task.task_id:
                        self.hol_blocked_task_id = head_reservation.task.task_id
                        self.hol_blocked_since_ms = current_time
                        logger.debug(f"Head-of-line blocked by task {head_reservation.task.task_id}")
                    break
        else:
            # No restriction: scan queue for runnable tasks
            i = 0
            while i < len(self.task_reservations) and available_slots > 0:
                reservation = self.task_reservations[i]
                
                if self.can_run_task(reservation.task):
                    # Remove and launch this task
                    del self.task_reservations[i]  # Remove by index
                    reservation.previous_task_id = last_executed_task_id
                    
                    slot_id = self._launch_task(reservation, current_time)
                    if slot_id >= 0:
                        completion_time = current_time + reservation.task.duration_ms
                        newly_launched.append((reservation.task.task_id, completion_time))
                        available_slots = self.available_slots
                        # Don't increment i since we removed an element
                    else:
                        break
                else:
                    i += 1  # Try next task
        
        logger.debug(f"Launch attempt complete: {len(newly_launched)} new tasks launched, "
                    f"{len(self.executing_tasks)} of {self.num_slots} slots filled")
        
        return newly_launched

    # --- Late-binding helpers ---
    def select_runnable_reservation(self) -> Optional[str]:
        """Select next reservation that could run given FIFO policy and resources.

        Matches Java LateBindTaskScheduler:
        - If restrict_fifo is true, only the head-of-line may be confirmed.
        - Otherwise, scan the queue for the first runnable reservation.
        """
        if self.available_slots <= 0 or not self.task_reservations:
            return None

        if self.restrict_fifo:
            head = self.task_reservations[0]
            # Skip if task is awaiting cancel RPC
            if head.task.task_id in self.awaiting_cancel:
                logger.debug(f"Head task {head.task.task_id} awaiting cancel RPC, skipping")
                return None
            return head.task.task_id if self.can_run_task(head.task) else None

        # No FIFO restriction: scan for the first runnable reservation
        for res in self.task_reservations:
            # Skip if task is awaiting cancel RPC
            if res.task.task_id in self.awaiting_cancel:
                continue
            if self.can_run_task(res.task):
                return res.task.task_id
        return None

    def pre_allocate_for_confirm(self, task_id: str) -> bool:
        """
        Pre-allocate resources for confirm attempt (matches Java LateBindTaskScheduler.confirmTaskReadyToRun).

        Java behavior: _nodeResources.runTaskIfPossible() is called BEFORE sending confirm to scheduler.
        If resources can't be allocated, confirm is not sent.

        Returns True if resources were successfully pre-allocated, False otherwise.
        """
        if task_id in self.pre_allocated_tasks:
            logger.debug(f"PRE_ALLOCATE: Task {task_id} already pre-allocated on node {self.node_id}")
            return True

        for res in self.task_reservations:
            if res.task.task_id == task_id:
                if self.restrict_fifo and self.task_reservations[0].task.task_id != task_id:
                    logger.debug(f"PRE_ALLOCATE: FIFO restriction - task {task_id} not at head")
                    return False

                if self.can_run_task(res.task):
                    # Pre-allocate resources (matches Java NodeResources.runTaskIfPossible)
                    self.allocated_resources += res.task.resource_request
                     
                    self.pre_allocated_tasks.add(task_id)
                    logger.debug(f"PRE_ALLOCATE: Resources pre-allocated for task {task_id} on node {self.node_id}")
                    return True
                else:
                    logger.debug(f"PRE_ALLOCATE: Insufficient resources for task {task_id} on node {self.node_id}")
                    return False

        logger.debug(f"PRE_ALLOCATE: Task {task_id} not found in reservations on node {self.node_id}")
        return False

    def free_pre_allocated_resources(self, task_id: str) -> bool:
        """Free pre-allocated resources if confirm was rejected."""
        for res in self.task_reservations:
            if res.task.task_id == task_id:
                self.allocated_resources -= res.task.resource_request
                # Mark task as awaiting cancel RPC to prevent immediate retry
                self.awaiting_cancel.add(task_id)
                self.pre_allocated_tasks.discard(task_id)
                logger.debug(f"FREE_PRE_ALLOC: Resources freed for task {task_id} on node {self.node_id}, awaiting cancel RPC")
                return True
        return False

    def launch_confirmed_reservation(self, task_id: str, current_time: float) -> Optional[float]:
        """Launch a reservation after scheduler confirmation. Returns completion time if launched.

        Enforce head-of-line when restrict_fifo is true, matching Java.
        """
        for i, res in enumerate(self.task_reservations):
            if res.task.task_id == task_id:
                if self.restrict_fifo and i != 0:
                    return None
                if self.available_slots <= 0 or not self.can_run_task(res.task):
                    return None
                reservation = self.task_reservations[i]
                del self.task_reservations[i]
                # Clear pending confirm flag AFTER removing from queue
                # This prevents re-confirming the same task while it's being removed
                self.pending_confirms.discard(task_id)
                slot_id = self._launch_task(reservation, current_time)
                if slot_id >= 0:
                    return current_time + reservation.task.duration_ms
                return None
        return None

    def cancel_reservation(self, task_id: str) -> bool:
        """Cancel a pending reservation (free requested resources)."""
        for i, res in enumerate(self.task_reservations):
            if res.task.task_id == task_id:
                del self.task_reservations[i]
                self.requested_resources -= res.task.resource_request
                self.total_duration_ms -= res.task.duration_ms
                self.waiting_or_running_tasks_counter -= 1
                # Clear from awaiting_cancel set when cancel RPC arrives
                self.awaiting_cancel.discard(task_id)
                # Clear pending confirm flag AFTER removing from queue
                # This prevents re-confirming the same task while it's still in queue
                self.pending_confirms.discard(task_id)
                logger.debug(f"Cancelled reservation for task {task_id} on node {self.node_id}")
                return True
        return False
    
    def get_node_state(self) -> NodeState:
        """Get current node state for reporting (matches Java getRequestedResourceVector)."""
        return NodeState(
            node_id=self.node_id,
            node_type=self.node_type,
            capacity=self.capacity,
            allocated=self.requested_resources,  # Use requested (all enqueued) not allocated (executing only)
            num_tasks=self.waiting_or_running_tasks_counter,  # Total tasks (waiting + executing)
            total_duration_ms=self.total_duration_ms,
            queue_length=len(self.task_reservations),
            last_update_time=time.time() * 1000  # Convert to milliseconds
        )
    
    def get_statistics(self) -> Dict[str, any]:
        """Get executor statistics."""
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'num_slots': self.num_slots,
            'restrict_fifo': self.restrict_fifo,
            'executing_tasks': len(self.executing_tasks),
            'queued_tasks': len(self.task_reservations),
            'available_slots': self.available_slots,
            'requested_resources': {
                'cores': self.requested_resources.cores,
                'memory': self.requested_resources.memory,
                'disk': self.requested_resources.disk
            },
            'allocated_resources': {
                'cores': self.allocated_resources.cores,
                'memory': self.allocated_resources.memory,
                'disk': self.allocated_resources.disk
            },
            'capacity': {
                'cores': self.capacity.cores,
                'memory': self.capacity.memory,
                'disk': self.capacity.disk
            },
            'hol_blocked_task': self.hol_blocked_task_id,
            'total_duration_ms': self.total_duration_ms,
            'waiting_or_running_counter': self.waiting_or_running_tasks_counter
        }
