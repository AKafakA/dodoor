package edu.cam.dodoor.nodemonitor;


import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;

public abstract class TaskScheduler {

    private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
    private String _ipAddress;

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
    void initialize(Configuration config, int nodeMonitorPort) {
        _conf = config;
        _ipAddress = Network.getIPAddress(config);

        _cores_per_slots = Resources.getSystemCPUCount(config) / _numSlots;
        _memory_per_slots = Resources.getSystemMemoryMb(config) / _numSlots;
        _disk_per_slots = Resources.getSystemDiskMb(config) / _numSlots;
    }

    TaskSpec getNextTask() {
        TaskSpec task = null;
        try {
            task = _runnableTaskQueue.take();
        } catch (InterruptedException e) {
            LOG.fatal(e);
        }
        return task;
    }
    /**
     * Returns the current number of runnable tasks (for testing).
     */
    int runnableTasks() {
        return _runnableTaskQueue.size();
    }

    void tasksFinished(List<TFullTaskId> finishedTasks) {
        for (TFullTaskId t : finishedTasks) {
            AUDIT_LOG.info(Logging.auditEventString("task_completed", t.getRequestId(), t.getTaskId()));
            handleTaskFinished(t.getRequestId(), t.getTaskId());
        }
    }

    protected void makeTaskRunnable(TaskSpec task) {
        try {
            LOG.debug("Putting reservation for request " + task._requestId + " in runnable queue");
            _runnableTaskQueue.put(task);
        } catch (InterruptedException e) {
            LOG.fatal("Unable to add task to runnable queue: " + e.getMessage());
        }
    }

    public synchronized void submitTaskReservations(TEnqueueTaskReservationsRequest request) {
        TaskSpec reservation = new TaskSpec(request);
        if (!enoughResourcesToRun(reservation._cores, reservation._memory, reservation._disks)){
            AUDIT_LOG.info(Logging.auditEventString("big_task_failed_enqueued",
                    reservation._requestId, reservation._cores, reservation._memory, reservation._disks));
        }
        int queuedReservations = handleSubmitTaskReservation(reservation);
        AUDIT_LOG.info(Logging.auditEventString("reservation_enqueued", _ipAddress, request.taskId,
                queuedReservations));
    }

    boolean enoughResourcesToRun(int cores, long memory, long disk) {
        return cores <= _cores_per_slots && memory <= _memory_per_slots && disk <= _disk_per_slots;
    }

    // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

    /**
     * Handles a task reservation. Returns the number of queued reservations.
     */
    abstract int handleSubmitTaskReservation(TaskSpec taskReservation);

    /**
     * Cancels all task reservations with the given request id. Returns the number of task
     * reservations cancelled.
     */
    abstract void cancelTaskReservations(String requestId);

    /**
     * Handles the completion of a task that has finished executing.
     */
    protected abstract void handleTaskFinished(String requestId, String taskId);

    /**
     * Handles the case when the node monitor tried to launch a task for a reservation, but
     * the corresponding scheduler didn't return a task (typically because all of the corresponding
     * job's tasks have been launched).
     */
    protected abstract void handleNoTaskForReservation(TaskSpec taskSpec);

    /**
     * Returns the maximum number of active tasks allowed (the number of slots).
     *
     * -1 signals that the scheduler does not enforce a maximum number of active tasks.
     */
    abstract int getMaxActiveTasks();
}
