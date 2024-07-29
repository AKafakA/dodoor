package edu.cam.dodoor.node;

import edu.cam.dodoor.thrift.TFullTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FifoTaskScheduler extends TaskScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(FifoTaskScheduler.class);
    private final List<TaskSpec> _taskReservations =
            Collections.synchronizedList(new ArrayList<>());

    public FifoTaskScheduler(int numSlots, NodeResources nodeResources) {
        super(numSlots, nodeResources);
    }

    @Override
    synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
        // This method, cancelTaskReservations(), and handleTaskCompleted() are synchronized to avoid
        // race conditions between updating activeTasks and taskReservations.
        if (_activeTasks < _numSlots) {
            if (!_taskReservations.isEmpty()) {
                String errorMessage = "activeTasks should be less than maxActiveTasks only " +
                        "when no outstanding reservations.";
                LOG.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            if (_nodeResources.runTaskIfPossible(taskReservation._resourceVector.cores,
                    taskReservation._resourceVector.memory, taskReservation._resourceVector.disks)) {
                makeTaskRunnable(taskReservation);
                ++_activeTasks;
                LOG.debug("Making task for task {} runnable ({} of {} task slots currently filled)", new Object[]{taskReservation._taskId, _activeTasks, _numSlots});
                return 0;
            } else {
                LOG.warn("Failed to run task for task {} because resources are not available, will put into reservation", taskReservation._taskId);
            }
        }
        int queuedReservations = _taskReservations.size();
        LOG.debug("Enqueueing task reservation with task id {} because all task slots filled. {} already enqueued reservations.", taskReservation._taskId, queuedReservations);
        _taskReservations.add(taskReservation);
        return queuedReservations;
    }

    @Override
    protected void handleTaskFinished(TFullTaskId finishedTask) {
        _nodeResources.freeTask(finishedTask.resourceRequest.cores,
                finishedTask.resourceRequest.memory, finishedTask.resourceRequest.disks);
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
        for (TaskSpec taskSpec : _taskReservations) {
            if (_nodeResources.runTaskIfPossible(taskSpec._resourceVector.cores,
                    taskSpec._resourceVector.memory, taskSpec._resourceVector.disks)) {
                if (_taskReservations.remove(taskSpec)) {
                    makeTaskRunnable(taskSpec);
                    taskSpec._previousTaskId = lastExecutedTaskId;
                    return;
                } else {
                    LOG.error(
                            "Failed to remove task reservation for task {} from task reservations queue and put it back.",
                            taskSpec._taskId);
                    _nodeResources.freeTask(taskSpec._resourceVector.cores, taskSpec._resourceVector.memory, taskSpec._resourceVector.disks);
                }
            }
        }
        _activeTasks -= 1;
    }

    @Override
    int getNumSlots() {
        return _numSlots;
    }
}
