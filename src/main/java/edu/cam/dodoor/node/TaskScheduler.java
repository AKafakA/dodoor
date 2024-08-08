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
    AtomicInteger _activeTasks;

    final NodeResources _nodeResources;

    protected Configuration _conf;
    private final BlockingQueue<TaskSpec> _runnableTaskQueue =
            new LinkedBlockingQueue<>();

    public TaskScheduler(int numSlots, NodeResources nodeResources) {
        _numSlots = numSlots;
        _nodeResources = nodeResources;
        _activeTasks = new AtomicInteger(0);
    }

    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration config) {
        _conf = config;
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
        handleTaskFinished(finishedTask);
    }

    protected void makeTaskRunnable(TaskSpec task) {
        synchronized(_runnableTaskQueue) {
            try {
                LOG.debug("Putting reservation for task {} in runnable queue size {}", task._taskId, _runnableTaskQueue.size());
                _runnableTaskQueue.put(task);
                if (_runnableTaskQueue.size() == 1) {
                    LOG.debug("Notifying runnable queue");
                    _runnableTaskQueue.notify();
                }
            } catch (InterruptedException e) {
                LOG.error("Unable to add task to runnable queue: {}", e.getMessage());
            }
        }
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
