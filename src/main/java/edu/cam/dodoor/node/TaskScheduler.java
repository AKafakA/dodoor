package edu.cam.dodoor.node;


import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class TaskScheduler {

    private final static Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);;

    int _numSlots;
    int _activeTasks;

    double _cores_per_slots;
    int _memory_per_slots;
    int _disk_per_slots;

    protected Configuration _conf;
    private final BlockingQueue<TaskSpec> _runnableTaskQueue =
            new LinkedBlockingQueue<>();

    public TaskScheduler(int numSlots) {
        _numSlots = numSlots;
        _activeTasks = 0;
    }

    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration config) {
        _conf = config;

        _cores_per_slots = Resources.getSystemCPUCount(config) / _numSlots;
        _memory_per_slots = Resources.getSystemMemoryMb(config) / _numSlots;
        _disk_per_slots = Resources.getSystemDiskGb(config) / _numSlots;
    }

    TaskSpec getNextTask() {
        TaskSpec task = null;
        try {
            task = _runnableTaskQueue.take();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while trying to get next task.");
        }
        return task;
    }
    /**
     * Returns the current number of runnable tasks (for testing).
     */
    int runnableTasks() {
        return _runnableTaskQueue.size();
    }

    void tasksFinished(TFullTaskId finishedTask) {
        LOG.info(Logging.auditEventString("task_completed", finishedTask.getTaskId()));
        handleTaskFinished(finishedTask.taskId);
    }

    protected void makeTaskRunnable(TaskSpec task) {
        try {
            LOG.debug("Putting reservation for task {} in runnable queue", task._taskId);
            _runnableTaskQueue.put(task);
        } catch (InterruptedException e) {
            LOG.error("Unable to add task to runnable queue: {}", e.getMessage());
        }
    }

    public synchronized void submitTaskReservation(TEnqueueTaskReservationRequest request) {
        TaskSpec reservation = new TaskSpec(request);
        if (!enoughResourcesToRun(request.resourceRequested)){
            LOG.info(Logging.auditEventString("big_task_failed_enqueued",
                    reservation._taskId, request.resourceRequested.cores,
                    request.resourceRequested.memory,
                    request.resourceRequested.disks));
            return;
        }
        int queuedReservations = handleSubmitTaskReservation(reservation);
        LOG.info(Logging.auditEventString("reservation_enqueued", request.taskId,
                queuedReservations));
    }

    boolean enoughResourcesToRun(TResourceVector requestedResources) {
        return requestedResources.cores <= _cores_per_slots
                && requestedResources.memory <= _memory_per_slots
                && requestedResources.disks <= _disk_per_slots;
    }

    // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

    /**
     * Handles a task reservation. Returns the number of queued reservations.
     */
    abstract int handleSubmitTaskReservation(TaskSpec taskReservation);


    /**
     * Handles the completion of a task that has finished executing.
     */
    protected abstract void handleTaskFinished(String taskId);

    /**
     * Returns the maximum number of active tasks allowed (the number of slots).
     *
     * -1 signals that the scheduler does not enforce a maximum number of active tasks.
     */
    abstract int getNumSlots();
}
