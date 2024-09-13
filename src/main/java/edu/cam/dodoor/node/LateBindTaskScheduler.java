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
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A task scheduler that binds tasks to slots only when they are ready to run.
 */
public class LateBindTaskScheduler extends TaskScheduler{

    private final static Logger LOG = LoggerFactory.getLogger(LateBindTaskScheduler.class);
    private final List<TaskSpec> _taskReservations;
    private final Map<String, AtomicBoolean> _taskReservationPinedOut;
    private final Map<String, InetSocketAddress> _taskToSchedulerMap;
    private final ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    private final String _nodeAddressStr;

    public LateBindTaskScheduler(int numSlots, NodeResources nodeResources,
                                 ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool,
                                 String nodeAddressStr) {
        super(numSlots, nodeResources);
        _taskToSchedulerMap = new ConcurrentHashMap<>();
        _schedulerClientPool = schedulerClientPool;
        _nodeAddressStr = nodeAddressStr;
        _taskReservationPinedOut = new ConcurrentHashMap<>();
        _taskReservations = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    int handleSubmitTaskReservation(TEnqueueTaskReservationRequest request) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        boolean noEnoughResources = false;
        TaskSpec taskReservation = new TaskSpec(request);
        InetSocketAddress schedulerAddress = Network.thriftToSocket(request.getSchedulerAddress());
        _taskToSchedulerMap.put(taskReservation._taskId, schedulerAddress);
        _taskReservations.add(taskReservation);
        _taskReservationPinedOut.put(taskReservation._taskId, new AtomicBoolean(false));
        if (currentActiveTasks < _numSlots) {
            if (confirmTaskReadyToRun(taskReservation, taskReservation._previousTaskId)) {
                LOG.debug("Task {} is ready to run with {} active tasks and {} slots available. " +
                                "Confirming task ready to run from scheduler.",
                            new Object[]{taskReservation._taskId, currentActiveTasks, _numSlots});
                return 0;
            } else {
                noEnoughResources = true;
                LOG.warn("Failed to run task for task {} because resources are not available, will put into reservation",
                        taskReservation._taskId);
            }
        }
        int queuedReservations = _taskReservations.size();
        LOG.debug("Enqueueing task reservation with task id {} with {} slots filled and no enough resources: {}. Currently " +
                        "{} already enqueued reservations.",
                new Object[] {taskReservation._taskId, currentActiveTasks, noEnoughResources, queuedReservations});
        return queuedReservations;
    }

    @Override
    protected void handleTaskFinished(TFullTaskId finishedTask) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();
        LOG.debug("Task {} finished, freeing resources and attempting to launch new task and" +
                "current filled slots before freeing this: {} ", finishedTask.taskId, currentActiveTasks);
        attemptConfirmNextTaskReadyToRun(finishedTask);
    }


    @Override
    protected boolean cancelTaskReservation(TFullTaskId taskId) {
       for (TaskSpec taskSpec : _taskReservations) {
            if (taskSpec._taskId.equals(taskId.taskId)) {
                _taskReservations.remove(taskSpec);
                _taskReservationPinedOut.remove(taskId.taskId);
                return true;
            }
        }
        LOG.error("Failed to find task reservation for task {} to cancel", taskId.taskId);
        return false;
    }

    @Override
    protected boolean executeTask(TFullTaskId taskId) {
        for (TaskSpec taskSpec : _taskReservations) {
            if (taskSpec._taskId.equals(taskId.taskId)) {
                _taskReservationPinedOut.remove(taskId.taskId);
                _taskReservations.remove(taskSpec);
                makeTaskRunnable(taskSpec);
                return true;
            }
        }
        LOG.error("Failed to find task reservation for task {} to execute", taskId.taskId);
        return false;
    }

    /**
     * Attempts to confirm the next task ready to run from preservation queues to scheduler.
     */
    private void attemptConfirmNextTaskReadyToRun(TFullTaskId finishedTask) {
        int currentActiveTasks = _taskLauncherService.getActiveTasks();

        for (TaskSpec taskSpec : _taskReservations) {
            if (!_taskReservationPinedOut.get(taskSpec._taskId).get()
                    && confirmTaskReadyToRun(taskSpec, finishedTask.taskId)) {
                return;
            }
        }
        LOG.debug("No tasks to run, {} of {} task slots currently filled", currentActiveTasks, _numSlots);
    }

    private boolean confirmTaskReadyToRun(TaskSpec taskSpec,
                                          String lastExecutedTaskId) {
        TFullTaskId taskId = taskSpec.getFullTaskId();
        InetSocketAddress schedulerAddress = _taskToSchedulerMap.get(taskId.taskId);
        boolean canRun = _nodeResources.runTaskIfPossible(taskSpec._resourceVector.cores,
                taskSpec._resourceVector.memory, taskSpec._resourceVector.disks);
        if (canRun) {
            try {
                SchedulerService.AsyncClient schedulerClient = _schedulerClientPool.borrowClient(schedulerAddress);
                _taskReservationPinedOut.get(taskId.taskId).set(true);
                schedulerClient.confirmTaskReadyToExecute(taskId, _nodeAddressStr,
                        new ConfirmTaskReadyToRunCallback(schedulerClient, schedulerAddress, this,
                                taskSpec,
                                lastExecutedTaskId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return canRun;
    }

    private class ConfirmTaskReadyToRunCallback implements AsyncMethodCallback<Boolean> {

        private final SchedulerService.AsyncClient _schedulerClient;
        private final InetSocketAddress _schedulerAddress;
        private final LateBindTaskScheduler _lateBindTaskScheduler;
        private final TaskSpec _taskReservation;
        private final String _lastExecutedTaskId;

        ConfirmTaskReadyToRunCallback(SchedulerService.AsyncClient schedulerClient,
                                      InetSocketAddress schedulerAddress,
                                      LateBindTaskScheduler lateBindTaskScheduler,
                                      TaskSpec taskReservation,
                                      String lastExecutedTaskId) {
            _schedulerClient = schedulerClient;
            _schedulerAddress = schedulerAddress;
            _lateBindTaskScheduler = lateBindTaskScheduler;
            _taskReservation = taskReservation;
            _lastExecutedTaskId = lastExecutedTaskId;
        }

        @Override
        public void onComplete(Boolean response) {
            if (response) {
                TFullTaskId taskId = _taskReservation.getFullTaskId();
                _taskReservation._previousTaskId = _lastExecutedTaskId;
                _lateBindTaskScheduler.executeTask(taskId);
                LOG.debug("Task {} confirmed ready to run from scheduler and has been executed.", taskId.taskId);
            } else {
                LOG.debug("Task {} has been placed already", _taskReservation._taskId);
            }
            try {
                _schedulerClientPool.returnClient(_schedulerAddress, _schedulerClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception exception) {
            LOG.error("Error confirming task ready to run from scheduler {} and add this back to reservation",
                    _schedulerAddress, exception);
            _taskReservationPinedOut.get(_taskReservation._taskId).set(false);
            _nodeResources.freeTask(_taskReservation._resourceVector.cores,
                    _taskReservation._resourceVector.memory,
                    _taskReservation._resourceVector.disks);
            try {
                _schedulerClientPool.returnClient(_schedulerAddress, _schedulerClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
