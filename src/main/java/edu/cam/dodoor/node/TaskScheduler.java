package edu.cam.dodoor.node;


import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TaskScheduler {

    private final static Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);;

    int _numSlots;

    final NodeResources _nodeResources;

    protected Configuration _conf;

    protected TaskLauncherService _taskLauncherService;

    public TaskScheduler(int numSlots, NodeResources nodeResources) {
        _numSlots = numSlots;
        _nodeResources = nodeResources;
    }

    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration config, TaskLauncherService taskLauncherService) {
        _conf = config;
        _taskLauncherService = taskLauncherService;
    }


    void tasksFinished(TFullTaskId finishedTask) {
        LOG.info(Logging.auditEventString("task_completed", finishedTask.getTaskId()));
        handleTaskFinished(finishedTask);
    }

    protected void makeTaskRunnable(TaskSpec task) {
        LOG.debug("Making task {} runnable", task._taskId);
        _taskLauncherService.tryToLaunchTask(task);
    }

    public synchronized void submitTaskReservation(TEnqueueTaskReservationRequest request) {
        TaskSpec reservation = new TaskSpec(request);
        int queuedReservations = handleSubmitTaskReservation(reservation);
        LOG.info(Logging.auditEventString("reservation_enqueued", request.taskId,
                queuedReservations));
    }

    // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

    /**
     * Handles a task reservation. Returns the number of queued reservations.
     */
    abstract int handleSubmitTaskReservation(TaskSpec taskReservation);


    /**
     * Handles the completion of a task that has finished executing.
     */
    protected abstract void handleTaskFinished(TFullTaskId finishedTask);

    /**
     * Returns the maximum number of active tasks allowed (the number of slots).
     *
     * -1 signals that the scheduler does not enforce a maximum number of active tasks.
     */
    abstract int getNumSlots();
}
