package org.anon.scheduler.scheduler;

import com.codahale.metrics.*;
import org.anon.scheduler.DodoorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;


public class SchedulerServiceMetrics {

    private final static Logger LOG = LoggerFactory.getLogger(SchedulerServiceMetrics.class);

    private final Histogram _endToEndLatencyHistogram;
    private final Histogram _endToEndMakespanHistogram;
    private final Meter _tasksRate;
    private final Meter _loadUpdateRate;
    private final Counter _totalMessages;
    private final Counter _numFinishedTasks;
    private final Counter _numFailedToSchedule;

    private final Histogram _endToLateBindingEnqueueLatencyHistogram;
    private final Histogram _endToLateBindingConfirmLatencyHistogram;
    private final int _startTrackingTaskId; // This is used to begin tracking tasks for end-to-end latency.
    private final int _endTrackingTaskId; // This is used to end tracking tasks for end-to-end latency.

    private final long _startTime;
    private boolean _updatedThroughput;
    private final Set<Integer> _scheduledTaskIds;
    private final Set<Integer> _completedTaskIds;
    private boolean _logPerTaskMetrics = false;

    public SchedulerServiceMetrics(MetricRegistry metrics, boolean isLateBinding,
                                   int startTrackingTaskId, int endTrackingTaskId,
                                   boolean logPerTaskMetrics) {
        _endToEndLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_SELECTED_TASK_SCHEDULING_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _endToEndMakespanHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_SELECTED_TASK_MAKESPAN_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));

        if (isLateBinding) {
            _endToLateBindingEnqueueLatencyHistogram = metrics.histogram(
                    DodoorConf.SCHEDULER_METRICS_END_TO_END_LATE_BINDING_ENQUEUE_LATENCY_HISTOGRAMS,
                    () -> new Histogram(new UniformReservoir()));
            _endToLateBindingConfirmLatencyHistogram = metrics.histogram(
                    DodoorConf.SCHEDULER_METRICS_END_TO_END_LATE_BINDING_CONFIRM_LATENCY_HISTOGRAMS,
                    () -> new Histogram(new UniformReservoir())
            );
        } else {
            _endToLateBindingEnqueueLatencyHistogram = null;
            _endToLateBindingConfirmLatencyHistogram = null;
        }
        _tasksRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_TASK_RATE);
        _loadUpdateRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_LOAD_UPDATE_RATE);
        _totalMessages = metrics.counter(DodoorConf.SCHEDULER_METRICS_NUM_MESSAGES);
        _numFinishedTasks = metrics.counter(DodoorConf.SCHEDULER_METRICS_FINISHED_TASKS);
        _numFailedToSchedule = metrics.counter(DodoorConf.SCHEDULER_METRICS_FAILURE_COUNT);
        _startTrackingTaskId = startTrackingTaskId;
        _endTrackingTaskId = endTrackingTaskId;
        _startTime = System.currentTimeMillis();
        _updatedThroughput = false;
        _scheduledTaskIds = new HashSet<>();
        _completedTaskIds = new HashSet<>();
        _logPerTaskMetrics = logPerTaskMetrics;
    }

    public void taskSubmitted(int numTasks) {
        _tasksRate.mark(numTasks);
    }

    public void loadUpdated() {
        _loadUpdateRate.mark();
    }

    public Counter getTotalMessages() {
        return _totalMessages;
    }

    public void probeNode() {
        _totalMessages.inc();
    }

    public void updateToDataStore() {
        _totalMessages.inc();
    }

    public void taskScheduled(long latency, String taskId) {
        if (_logPerTaskMetrics) {
            LOG.info("Task {} scheduled, end-to-end latency: {} ms", taskId, latency);
        }
        int taskIdInt = Integer.parseInt(taskId);
        if (taskIdInt >= _startTrackingTaskId && taskIdInt < _endTrackingTaskId &&
                !_scheduledTaskIds.contains(taskIdInt)) {
            _endToEndLatencyHistogram.update(latency);
            _scheduledTaskIds.add(taskIdInt);
        }
    }

    public void lateBindingEnqueue(long latency) {
        _endToLateBindingEnqueueLatencyHistogram.update(latency);
    }

    public void lateBindingConfirm(long latency) {
        _endToLateBindingConfirmLatencyHistogram.update(latency);
    }


    public void taskFinished(String taskId,
                             long makespan, long nodeWallTime, long taskDuration) {
        if (_logPerTaskMetrics) {
            LOG.info("Task {} finished, makespan: {} ms, node wall time: {} ms, task duration: {} ms",
                    new Object[]{taskId, makespan, nodeWallTime, taskDuration});
        }
        _numFinishedTasks.inc();
        int taskIdInt = Integer.parseInt(taskId);
        if (taskIdInt >= _startTrackingTaskId && taskIdInt < _endTrackingTaskId && !_completedTaskIds.contains(taskIdInt)) {
            _completedTaskIds.add(taskIdInt);
            _endToEndMakespanHistogram.update(makespan);
            if (_endToEndMakespanHistogram.getCount() >= (_endTrackingTaskId - _startTrackingTaskId) &&
                    !_updatedThroughput) {
                // all results collected, calculate and update the throughput metrics for the first time
                _updatedThroughput = true;
                long endTime = System.currentTimeMillis();
                long elapsedTime = endTime - _startTime;
                double throughput = (double) ( _numFinishedTasks.getCount()) / (elapsedTime / 1000.0);
                LOG.info("Finished all tracked tasks, within elapsed time: {} ms, leads to throughput as {} tasks/s",
                        new Object[]{elapsedTime, throughput});
            }
        }
    }

    public void failedToScheduling() {
        _numFailedToSchedule.inc();
    }

    public void taskReadyToExecute() {
        _totalMessages.inc();
    }

    public void infoNodeToCancel() {
        _totalMessages.inc();
    }
}
