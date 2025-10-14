package org.anon.scheduler.node;

import org.anon.scheduler.thrift.TEnqueueTaskReservationRequest;
import org.anon.scheduler.thrift.TFullTaskId;
import org.anon.scheduler.thrift.TSchedulingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class FifoTaskScheduler extends TaskScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(FifoTaskScheduler.class);
    private final List<TaskSpec> _taskReservations;
    private final boolean _restricFifo;
    private String _holBlockedTaskId;
    private Long _holBlockedSinceMs;

    public FifoTaskScheduler(int numSlots, NodeResources nodeResources) {
        this(numSlots, nodeResources, false);
    }

    public FifoTaskScheduler(int numSlots, NodeResources nodeResources, boolean restrictFifo) {
        super(numSlots, nodeResources);
        _taskReservations = Collections.synchronizedList(new ArrayList<>());
        _restricFifo = restrictFifo;
    }

    @Override
    synchronized int handleSubmitTaskReservation(TEnqueueTaskReservationRequest request) {
        // This method, cancelTaskReservations(), and handleTaskCompleted() are synchronized to avoid
        // race conditions between updating activeTasks and taskReservations.
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        TaskSpec taskReservation = new TaskSpec(request, System.currentTimeMillis());
        // REPLAY: reservation enqueued (DEBUG)
        LOG.debug("REPLAY reservation_enqueued t={} task={} node={} qlen={} slots={} restrict_fifo={}",
                new Object[]{System.currentTimeMillis(), request.taskId, "local", _taskReservations.size()+1, _numSlots, _restricFifo});
        if (currentActiveTasks < _numSlots) {
            if (_restricFifo && !_taskReservations.isEmpty()) {
                TaskSpec firstTask = _taskReservations.remove(0);
                _taskReservations.add(taskReservation);
                boolean launched = false;
                if (_nodeResources.runTaskIfPossible(firstTask._resourceVector.cores,
                        firstTask._resourceVector.memory, firstTask._resourceVector.disks)) {
                    // REPLAY: task runnable (DEBUG)
                    LOG.debug("REPLAY task_runnable t={} task={} node={} prev={}",
                            new Object[]{System.currentTimeMillis(), firstTask._taskId, "local",
                                    firstTask._previousTaskId == null ? "-" : firstTask._previousTaskId});
                    makeTaskRunnable(firstTask);
                    LOG.debug("Due to restricted FIFO, making first task {} runnable ({} of {} task slots currently filled)," +
                                    "and putting new task {} into reservation",
                            new Object[]{firstTask._taskId, currentActiveTasks, _numSlots});
                    launched = true;
                } else {
                    LOG.warn("Failed to run first task {} due to insufficient resources, " +
                                    "will put it back into head of task queue and the new task {} at end",
                            firstTask._taskId, taskReservation._taskId);
                    _taskReservations.add(0, firstTask);
                }
                if (launched) {
                    // Greedily attempt to launch subsequent head tasks if capacity remains
                    attemptTaskLaunch(firstTask._taskId);
                }
            } else if (_nodeResources.runTaskIfPossible(taskReservation._resourceVector.cores,
                    taskReservation._resourceVector.memory, taskReservation._resourceVector.disks)) {
                LOG.debug("REPLAY task_runnable t={} task={} node={} prev={}",
                        new Object[]{System.currentTimeMillis(), taskReservation._taskId, "local",
                                taskReservation._previousTaskId == null ? "-" : taskReservation._previousTaskId});
                makeTaskRunnable(taskReservation);
                LOG.debug("Making task for task {} runnable ({} of {} task slots currently filled)",
                        new Object[]{taskReservation._taskId, currentActiveTasks, _numSlots});
                if (_restricFifo) {
                    attemptTaskLaunch(taskReservation._taskId);
                }
            } else {
                LOG.warn("Failed to run task for task {} because resources are not available, will put into reservation",
                        taskReservation._taskId);
                _taskReservations.add(taskReservation);
            }
        } else {
            LOG.warn("Cannot enqueue task reservation for task {} because all {} task slots are filled",
                    taskReservation._taskId, _numSlots);
            _taskReservations.add(taskReservation);
        }
        return _taskReservations.size();
    }

    @Override
    protected void handleTaskFinished(TFullTaskId finishedTask) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        LOG.debug("Task {} finished, freeing resources and attempting to launch new task and" +
                "current filled slots before freeing this: {} ", finishedTask.taskId, currentActiveTasks);
        attemptTaskLaunch(finishedTask.taskId);
    }

    /**
     * Attempts to launch a new task.
     *
     * The parameters {@code lastExecutedTaskId} are used purely
     * for logging purposes, to determine how long the node monitor spends trying to find a new
     * task to execute. This method needs to be synchronized to prevent a race condition.
     */
    private synchronized void attemptTaskLaunch(String lastExecutedTaskId) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        int availableSlots = Math.max(_numSlots - currentActiveTasks, 0);
        if (availableSlots == 0) {
            LOG.debug("No free slots to launch new tasks ({} of {} filled)", currentActiveTasks, _numSlots);
            return;
        }
        if (_restricFifo) {
            // Strict head-of-line: repeatedly try the head as long as it can run and we have slots
            while (availableSlots > 0 && !_taskReservations.isEmpty()) {
                TaskSpec head = _taskReservations.get(0);
                LOG.debug("Restricting FIFO, trying head-of-line task {}", head._taskId);
                boolean canRun = _nodeResources.runTaskIfPossible(head._resourceVector.cores,
                        head._resourceVector.memory, head._resourceVector.disks);
                if (canRun) {
                    // Remove by index 0 to avoid O(n) search anomalies if equals/hash not stable
                    _taskReservations.remove(0);
                    LOG.debug("REPLAY task_runnable t={} task={} node={} prev={}",
                            new Object[]{System.currentTimeMillis(), head._taskId, "local",
                                    head._previousTaskId == null ? "-" : head._previousTaskId});
                    makeTaskRunnable(head);
                    head._previousTaskId = lastExecutedTaskId;
                    // If we were blocked on this head, close the block duration
                    if (_holBlockedTaskId != null && _holBlockedTaskId.equals(head._taskId) && _holBlockedSinceMs != null) {
                        long blockedMs = Math.max(0, System.currentTimeMillis() - _holBlockedSinceMs);
                        _taskLauncherService.recordHolBlockedDuration(blockedMs);
                        _holBlockedTaskId = null;
                        _holBlockedSinceMs = null;
                    }
                    currentActiveTasks = _taskLauncherService.getActiveTasks();
                    availableSlots = Math.max(_numSlots - currentActiveTasks, 0);
                } else {
                    // Head cannot run; must wait, do not skip ahead
                    if (_holBlockedTaskId == null || !_holBlockedTaskId.equals(head._taskId)) {
                        _holBlockedTaskId = head._taskId;
                        _holBlockedSinceMs = System.currentTimeMillis();
                        _taskLauncherService.recordHolBlocked();
                    }
                    break;
                }
            }
        } else {
            // No restriction: try to fill up to available slots scanning the queue
            for (int i = 0; i < _taskReservations.size() && availableSlots > 0; i++) {
                TaskSpec taskSpec = _taskReservations.get(i);
                LOG.debug("Trying to run {}th task {} due to no-FIFO", i, taskSpec._taskId);
                if (_nodeResources.runTaskIfPossible(taskSpec._resourceVector.cores,
                        taskSpec._resourceVector.memory, taskSpec._resourceVector.disks)) {
                    if (_taskReservations.remove(taskSpec)) {
                        LOG.debug("REPLAY task_runnable t={} task={} node={} prev={}",
                                new Object[]{System.currentTimeMillis(), taskSpec._taskId, "local",
                                        taskSpec._previousTaskId == null ? "-" : taskSpec._previousTaskId});
                        makeTaskRunnable(taskSpec);
                        taskSpec._previousTaskId = lastExecutedTaskId;
                        currentActiveTasks = _taskLauncherService.getActiveTasks();
                        availableSlots = Math.max(_numSlots - currentActiveTasks, 0);
                        i--; // queue shrank
                    } else {
                        LOG.error("Failed to remove task reservation for task {} from task reservations queue and put it back.",
                                taskSpec._taskId);
                        _nodeResources.freeTask(taskSpec._resourceVector.cores,
                                taskSpec._resourceVector.memory,
                                taskSpec._resourceVector.disks);
                    }
                }
            }
        }
        LOG.debug("Done attempting launch; {} of {} task slots currently filled", _taskLauncherService.getActiveTasks(), _numSlots);
    }

    @Override
    protected boolean cancelTaskReservation(TFullTaskId taskId) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
