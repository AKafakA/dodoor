package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

public class FifoTaskScheduler extends TaskScheduler {
    private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);
    public LinkedBlockingQueue<TaskSpec> _taskReservations =
            new LinkedBlockingQueue<>();

    public FifoTaskScheduler(int numSlots) {
        super(numSlots);
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
            makeTaskRunnable(taskReservation);
            ++_activeTasks;
            LOG.debug("Making task for request " + taskReservation._requestId + " runnable (" +
                    _activeTasks + " of " + _numSlots + " task slots currently filled)");
            return 0;
        }
        LOG.debug("All " + _numSlots + " task slots filled.");
        int queuedReservations = _taskReservations.size();
        try {
            LOG.debug("Enqueueing task reservation with request id " + taskReservation._requestId +
                    " because all task slots filled. " + queuedReservations +
                    " already enqueued reservations.");
            _taskReservations.put(taskReservation);
        } catch (InterruptedException e) {
            LOG.fatal(e);
        }
        return queuedReservations;
    }

    @Override
    synchronized TResourceVector cancelTaskReservations(String requestId) {
        Iterator<TaskSpec> reservationsIterator = _taskReservations.iterator();
        TaskSpec reservation;
        while (reservationsIterator.hasNext()) {
            reservation = reservationsIterator.next();
            if (reservation._requestId.equals(requestId)) {
                reservationsIterator.remove();
            }
            return reservation._resourceVector;
        }
        return new TResourceVector(0, 0, 0);
    }

    @Override
    protected void handleTaskFinished(String requestId, String taskId) {
        attemptTaskLaunch(requestId, taskId);
    }

    @Override
    protected void handleNoTaskForReservation(TaskSpec taskSpec) {
        attemptTaskLaunch(taskSpec._previousRequestId, taskSpec._previousTaskId);
    }

    /**
     * Attempts to launch a new task.
     *
     * The parameters {@code lastExecutedRequestId} and {@code lastExecutedTaskId} are used purely
     * for logging purposes, to determine how long the node monitor spends trying to find a new
     * task to execute. This method needs to be synchronized to prevent a race condition.
     */
    private synchronized void attemptTaskLaunch(
            String lastExecutedRequestId, String lastExecutedTaskId) {
        TaskSpec reservation = _taskReservations.poll();
        if (reservation != null) {
            reservation._previousRequestId = lastExecutedRequestId;
            reservation._previousTaskId = lastExecutedTaskId;
            makeTaskRunnable(reservation);
        } else {
            _activeTasks -= 1;
        }
    }

    @Override
    int getNumSlots() {
        return _numSlots;
    }
}
