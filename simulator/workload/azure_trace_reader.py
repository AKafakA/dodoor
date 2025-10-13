"""
Azure trace reader for Dodoor simulator.

Reads Azure VM placement traces in the same format as TaskTracePlayer.java:
taskId,cores,memory,disks,durationInMs,startTime,taskType,mode
"""

import logging
from typing import List, Iterator
from pathlib import Path

try:
    from ..schedulers.base_scheduler import Task, ResourceVector
except ImportError:
    from schedulers.base_scheduler import Task, ResourceVector

logger = logging.getLogger(__name__)


class AzureTraceReader:
    """
    Reads Azure VM placement traces in TaskTracePlayer format.
    
    Each line: taskId,cores,memory,disks,durationInMs,startTime,taskType,mode
    Example: 0,2.0,8563,9046,265027,31,simulated,medium
    """
    
    def __init__(self, trace_file_path: str, replay_with_disk: bool = True):
        self.trace_file_path = Path(trace_file_path)
        self.replay_with_disk = replay_with_disk
        self.tasks = []
        self._load_trace()
    
    def _load_trace(self):
        """Load tasks from Azure trace file."""
        if not self.trace_file_path.exists():
            raise FileNotFoundError(f"Azure trace file not found: {self.trace_file_path}")
        
        logger.info(f"Loading Azure trace from {self.trace_file_path}")
        
        with open(self.trace_file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    parts = line.split(',')
                    if len(parts) < 7:
                        logger.warning(f"Skipping malformed line {line_num}: {line}")
                        continue
                    
                    # Parse Azure trace format
                    task_id = parts[0]
                    cores = float(parts[1])
                    memory = int(parts[2])  # in MB
                    disks = int(parts[3]) if self.replay_with_disk else 0  # Set to 0 when replay_with_disk=False (matches Java)
                    duration_ms = int(parts[4])
                    start_time = int(parts[5])  # in ms
                    task_type = parts[6]
                    mode = parts[7] if len(parts) > 7 else "medium"
                    
                    # Create task object
                    task = Task(
                        task_id=f"azure_{task_id}",
                        task_type=task_type,
                        resource_request=ResourceVector(
                            cores=cores,
                            memory=memory,
                            disk=disks
                        ),
                        duration_ms=duration_ms,
                        submission_time=float(start_time)  # Convert to float for simulation
                    )
                    
                    self.tasks.append(task)
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing line {line_num}: {line}. Error: {e}")
                    continue
        
        logger.info(f"Loaded {len(self.tasks)} tasks from Azure trace")
    
    def get_tasks(self) -> List[Task]:
        """Get all tasks from the trace."""
        return self.tasks.copy()
    
    def get_tasks_in_time_range(self, start_time: float, end_time: float) -> List[Task]:
        """Get tasks that should be submitted within the given time range."""
        return [
            task for task in self.tasks 
            if start_time <= task.submission_time < end_time
        ]
    
    def get_max_submission_time(self) -> float:
        """Get the maximum submission time in the trace."""
        if not self.tasks:
            return 0.0
        return max(task.submission_time for task in self.tasks)
    
    def get_task_stats(self) -> dict:
        """Get statistics about the loaded tasks."""
        if not self.tasks:
            return {}
        
        cores_list = [task.resource_request.cores for task in self.tasks]
        memory_list = [task.resource_request.memory for task in self.tasks]
        duration_list = [task.duration_ms for task in self.tasks]
        
        return {
            "total_tasks": len(self.tasks),
            "cores": {
                "min": min(cores_list),
                "max": max(cores_list),
                "avg": sum(cores_list) / len(cores_list)
            },
            "memory_mb": {
                "min": min(memory_list),
                "max": max(memory_list), 
                "avg": sum(memory_list) / len(memory_list)
            },
            "duration_ms": {
                "min": min(duration_list),
                "max": max(duration_list),
                "avg": sum(duration_list) / len(duration_list)
            },
            "time_range_ms": {
                "start": min(task.submission_time for task in self.tasks),
                "end": self.get_max_submission_time()
            }
        }


class AzureTraceWorkloadGenerator:
    """
    Workload generator that replays Azure VM traces with optional QPS control.
    
    This mimics the behavior of TaskTracePlayer.java when using external QPS.
    """
    
    def __init__(self, trace_reader: AzureTraceReader, target_qps: float = None, 
                 seed: int = None, simulation_duration_ms: float = None):
        self.trace_reader = trace_reader
        self.target_qps = target_qps
        self.simulation_duration_ms = simulation_duration_ms
        self._random = None
        
        if seed is not None:
            import random
            self._random = random.Random(seed)
    
    def generate_workload(self, duration_ms: float, start_time: float = 0.0,
                          target_tasks: int | None = None) -> List[Task]:
        """
        Generate workload for the specified duration.
        
        If target_qps is specified, tasks are respaced using Poisson process.
        Otherwise, original Azure trace timing is preserved.
        """
        if self.target_qps is not None:
            return self._generate_qps_controlled_workload(
                duration_ms, start_time, target_tasks
            )
        else:
            return self._generate_trace_timed_workload(
                duration_ms, start_time, target_tasks
            )

    def _generate_qps_controlled_workload(self, duration_ms: float, start_time: float,
                                          target_tasks: int | None) -> List[Task]:
        """Generate workload respaced according to target QPS using Poisson process."""
        tasks = self.trace_reader.get_tasks()

        if not tasks:
            return []

        # Calculate inter-arrival time for Poisson process
        inter_arrival_ms = 1000.0 / self.target_qps  # Convert QPS to ms

        respaced_tasks: List[Task] = []
        current_time = start_time
        total_needed = target_tasks if target_tasks is not None else len(tasks)
        if total_needed == 0:
            return respaced_tasks

        idx = 0
        while idx < total_needed:
            original_task = tasks[idx % len(tasks)]
            # If we are not targeting a specific count, stop when we run out of time
            if target_tasks is None and current_time >= start_time + duration_ms:
                break

            # Sample next inter-arrival time (exponential distribution)
            if self._random:
                # Exponential distribution with rate = target_qps/1000 (per ms)
                wait_time = self._random.expovariate(self.target_qps / 1000.0)
            else:
                wait_time = inter_arrival_ms

            current_time += wait_time

            if target_tasks is None and current_time >= start_time + duration_ms:
                break

            # Create new task with respaced timing
            respaced_task = Task(
                task_id=original_task.task_id,
                task_type=original_task.task_type,
                resource_request=original_task.resource_request,
                duration_ms=original_task.duration_ms,
                submission_time=current_time
            )
            respaced_tasks.append(respaced_task)
            idx += 1

        logger.info(
            f"Generated {len(respaced_tasks)} QPS-controlled tasks over {duration_ms/1000:.1f}s "
            f"(target QPS: {self.target_qps}, target_tasks={target_tasks})"
        )

        return respaced_tasks

    def _generate_trace_timed_workload(self, duration_ms: float, start_time: float,
                                       target_tasks: int | None) -> List[Task]:
        """Generate workload preserving original Azure trace timing."""
        end_time = start_time + duration_ms

        if target_tasks is not None:
            base_tasks = self.trace_reader.get_tasks()
            if not base_tasks:
                return []
            tasks = []
            loops = (target_tasks + len(base_tasks) - 1) // len(base_tasks)
            for loop_idx in range(loops):
                offset = loop_idx * (end_time - start_time)
                for original_task in base_tasks:
                    submission_time = start_time + (original_task.submission_time - start_time) + offset
                    cloned_task = Task(
                        task_id=f"{original_task.task_id}_loop{loop_idx}",
                        task_type=original_task.task_type,
                        resource_request=original_task.resource_request,
                        duration_ms=original_task.duration_ms,
                        submission_time=submission_time
                    )
                    tasks.append(cloned_task)
                    if len(tasks) >= target_tasks:
                        break
                if len(tasks) >= target_tasks:
                    break
        else:
            tasks = self.trace_reader.get_tasks_in_time_range(start_time, end_time)

        logger.info(f"Generated {len(tasks)} trace-timed tasks "
                   f"over {duration_ms/1000:.1f}s")

        return tasks
