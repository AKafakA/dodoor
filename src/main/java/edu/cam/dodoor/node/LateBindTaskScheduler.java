package edu.cam.dodoor.node;

import edu.cam.dodoor.thrift.SchedulerService;
import edu.cam.dodoor.thrift.TEnqueueTaskReservationRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A task scheduler that binds tasks to slots only when they are ready to run.
 */
public class LateBindTaskScheduler extends TaskScheduler{

    private final static Logger LOG = LoggerFactory.getLogger(LateBindTaskScheduler.class);
    private final List<TaskSpec> _taskReservations;
    private final Map<String, InetSocketAddress> _taskToSchedulerMap;
    private final ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    private final String _nodeAddressStr;
    private final boolean _restrictFifo; // Late binding does not restrict FIFO
    private String _holBlockedTaskId;
    private Long _holBlockedSinceMs;

    public LateBindTaskScheduler(int numSlots, NodeResources nodeResources,
                                 ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool,
                                 String nodeAddressStr,
                                 boolean restrictFifo) {
        super(numSlots, nodeResources);
        _taskToSchedulerMap = new ConcurrentHashMap<>();
        _schedulerClientPool = schedulerClientPool;
        _nodeAddressStr = nodeAddressStr;
        _taskReservations = Collections.synchronizedList(new ArrayList<>());
        _restrictFifo = restrictFifo;
    }

    public LateBindTaskScheduler(int numSlots, NodeResources nodeResources,
                                 ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool,
                                 String nodeAddressStr) {
        this(numSlots, nodeResources, schedulerClientPool, nodeAddressStr, false);
    }

    @Override
    synchronized int handleSubmitTaskReservation(TEnqueueTaskReservationRequest request) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        boolean noEnoughResources = false;
        TaskSpec taskReservation = new TaskSpec(request, System.currentTimeMillis());
        InetSocketAddress schedulerAddress = Network.thriftToSocket(request.getSchedulerAddress());
        _taskToSchedulerMap.put(taskReservation._taskId, schedulerAddress);
        boolean reservationEmpty = _taskReservations.isEmpty();
        _taskReservations.add(taskReservation);
        if (currentActiveTasks < _numSlots) {
            TaskSpec taskToRun;
            if (_restrictFifo && !reservationEmpty) {
                taskToRun = _taskReservations.get(0);
                LOG.debug("Restricting FIFO, try to run the head task {} instead of {}",
                        taskToRun._taskId, taskReservation._taskId);
            } else {
                taskToRun = taskReservation;
            }
            if (confirmTaskReadyToRun(taskToRun, taskToRun._previousTaskId)) {
                LOG.debug("Task {} is ready to run with {} active tasks and {} slots available. " +
                                "Confirming task ready to run from scheduler.",
                        new Object[]{taskToRun._taskId, currentActiveTasks, _numSlots});
                return 0;
            } else {
                noEnoughResources = true;
                LOG.warn("Failed to run task for task {} because resources are not available, will put into reservation",
                        taskToRun._taskId);
            }
        }
        LOG.debug("Enqueueing task reservation with task id {} with {} slots filled and no enough resources: {}. Currently " +
                        "{} already enqueued reservations.",
                new Object[] {taskReservation._taskId, currentActiveTasks, noEnoughResources,
                        _taskReservations.size()});
        return _taskReservations.size();
    }

    @Override
    protected void handleTaskFinished(TFullTaskId finishedTask) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        LOG.debug("Task {} finished, freeing resources and attempting to launch new task and" +
                "current filled slots before freeing this: {} ", finishedTask.taskId, currentActiveTasks);
        attemptConfirmNextTaskReadyToRun(finishedTask);
    }


    @Override
    protected synchronized boolean cancelTaskReservation(TFullTaskId taskId) {
        for (int i = 0; i < _taskReservations.size(); i++) {
            TaskSpec taskSpec = _taskReservations.get(i);
            if (taskSpec._taskId.equals(taskId.taskId)) {
                _taskReservations.remove(taskSpec);
                LOG.debug("Task reservation for task {} has been cancelled", taskId.taskId);
                if (i == 0) {
                    // if the cancelled task is the first in the queue, so it could cause unblocking of following tasks
                    attemptConfirmNextTaskReadyToRun(taskId);
                }
                return true;
            }
        }
        LOG.debug("Failed to find task reservation for task {} to cancel", taskId.taskId);
        return false;
    }

    /**
     * Attempts to confirm the next task ready to run from preservation queues to scheduler.
     */
    private synchronized void attemptConfirmNextTaskReadyToRun(TFullTaskId finishedTask) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        if (currentActiveTasks >= _numSlots) {
            return;
        }
        if (_restrictFifo) {
            if (_taskReservations.isEmpty()) {
                LOG.debug("No tasks in reservation, nothing to run after task {} finished", finishedTask.taskId);
            } else {
                TaskSpec firstTaskSpec = _taskReservations.get(0);
                if (confirmTaskReadyToRun(firstTaskSpec, finishedTask.taskId)) {
                    LOG.debug("First task {} in reservation confirmed ready to run after task {} finished, ",
                            new Object[]{firstTaskSpec._taskId, finishedTask.taskId});
                } else {
                    LOG.debug("The head of line task {} cannot be executed due to resources limitation, " +
                            "and blocking {} in total", firstTaskSpec._taskId, _taskReservations.size());
                    if (_holBlockedTaskId == null || !_holBlockedTaskId.equals(firstTaskSpec._taskId)) {
                        _holBlockedTaskId = firstTaskSpec._taskId;
                        _holBlockedSinceMs = System.currentTimeMillis();
                        _taskLauncherService.recordHolBlocked();
                    }
                }
            }
        } else {
            int i = 0;
            for (TaskSpec taskSpec : _taskReservations) {
                if (confirmTaskReadyToRun(taskSpec, finishedTask.taskId)) {
                    LOG.debug("i-th task {} in reservation confirmed ready to run after task {} finished, ",
                            new Object[]{i, taskSpec._taskId});
                    return;
                }
                i++;
            }
            LOG.debug("No tasks which current resource enough to run, " +
                    "{} running and {} pending", currentActiveTasks, _taskReservations.size());
        }
    }

    private synchronized boolean confirmTaskReadyToRun(TaskSpec taskSpec,
                                                       String lastExecutedTaskId) {
        // Enforce head-of-line when restrict FIFO is enabled: only the head task may be confirmed
        if (_restrictFifo) {
            if (_taskReservations.isEmpty() || !_taskReservations.get(0)._taskId.equals(taskSpec._taskId)) {
                LOG.warn("Restricting FIFO: refusing to confirm non-head task {} (head is {})",
                        taskSpec._taskId, _taskReservations.isEmpty() ? "<empty>" : _taskReservations.get(0)._taskId);
                return false;
            }
        }
        TFullTaskId taskId = taskSpec.getFullTaskId();
        InetSocketAddress schedulerAddress = _taskToSchedulerMap.get(taskId.taskId);
        boolean canRun = _nodeResources.runTaskIfPossible(taskSpec._resourceVector.cores,
                taskSpec._resourceVector.memory, taskSpec._resourceVector.disks);
        if (canRun) {
            try {
                SchedulerService.AsyncClient schedulerClient = _schedulerClientPool.borrowClient(schedulerAddress);
                schedulerClient.confirmTaskReadyToExecute(taskId, _nodeAddressStr,
                        new ConfirmTaskReadyToRunCallback(schedulerClient, schedulerAddress,
                                taskSpec,
                                lastExecutedTaskId));
                // either task is confirmed to run or not, both remove from reservation
            } catch (Exception e) {
                _nodeResources.freeTask(taskSpec._resourceVector.cores,
                        taskSpec._resourceVector.memory,
                        taskSpec._resourceVector.disks);
                LOG.error("Failed to confirm task {} ready with error: {}. " ,
                        taskId.taskId, e.getMessage());
            }
        } else {
            LOG.warn("Sparrow confirm rejected pre-check: resource_block task={} node has insufficient resources",
                    taskId.taskId);
        }
        return canRun;
    }

    private class ConfirmTaskReadyToRunCallback implements AsyncMethodCallback<Boolean> {

        private final SchedulerService.AsyncClient _schedulerClient;
        private final InetSocketAddress _schedulerAddress;
        private final TaskSpec _taskReservation;
        private final String _lastExecutedTaskId;

        ConfirmTaskReadyToRunCallback(SchedulerService.AsyncClient schedulerClient,
                                      InetSocketAddress schedulerAddress,
                                      TaskSpec taskReservation,
                                      String lastExecutedTaskId) {
            _schedulerClient = schedulerClient;
            _schedulerAddress = schedulerAddress;
            _taskReservation = taskReservation;
            _lastExecutedTaskId = lastExecutedTaskId;
        }

        @Override
        public void onComplete(Boolean response) {
            // received response from scheduler
            // if yes, make task runnable
            // if no, free resources and remove from reservation

            // Remove from reservation only on either confirm to run or reject due to run at another node
            synchronized (LateBindTaskScheduler.this) {
                _taskReservations.remove(_taskReservation);
            }
            if (response) {
                _taskReservation._previousTaskId = _lastExecutedTaskId;
                makeTaskRunnable(_taskReservation);
                LOG.debug("Task {} confirmed ready to run from scheduler and has been executed.", _taskReservation._taskId);
                if (_holBlockedTaskId != null && _holBlockedTaskId.equals(_taskReservation._taskId) && _holBlockedSinceMs != null) {
                    long blockedMs = Math.max(0, System.currentTimeMillis() - _holBlockedSinceMs);
                    _taskLauncherService.recordHolBlockedDuration(blockedMs);
                    _holBlockedTaskId = null;
                    _holBlockedSinceMs = null;
                }
            } else {
                _nodeResources.freeTask(_taskReservation._resourceVector.cores,
                        _taskReservation._resourceVector.memory,
                        _taskReservation._resourceVector.disks);
                // Keep the reservation in place; scheduler will cancel promptly after confirming a winner elsewhere.
                LOG.warn("Sparrow confirm rejected: keeping HOL reservation task={} (awaiting scheduler cancel)",
                        _taskReservation._taskId);
            }
            try {
                _schedulerClientPool.returnClient(_schedulerAddress, _schedulerClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // After handling this confirmation, immediately try to confirm the next task if resources allow.
            attemptConfirmNextTaskReadyToRun(_taskReservation.getFullTaskId());
        }

        @Override
        public void onError(Exception exception) {
            LOG.error("Error confirming task ready to run from scheduler {} and add this back to reservation",
                    _schedulerAddress, exception);
            _nodeResources.freeTask(_taskReservation._resourceVector.cores,
                    _taskReservation._resourceVector.memory,
                    _taskReservation._resourceVector.disks);
            try {
                _schedulerClientPool.returnClient(_schedulerAddress, _schedulerClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // Try to make forward progress despite the error by attempting the next task.
            attemptConfirmNextTaskReadyToRun(_taskReservation.getFullTaskId());
        }
    }

}
