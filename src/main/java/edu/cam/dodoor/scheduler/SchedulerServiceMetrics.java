package edu.cam.dodoor.scheduler;

import com.codahale.metrics.*;
import edu.cam.dodoor.DodoorConf;


public class SchedulerServiceMetrics {

    private final Histogram _endToEndLatencyHistogram;
    private final Histogram _endToEndMakespanHistogram;
    private final Meter _tasksRate;
    private final Meter _loadUpdateRate;
    private final Counter _totalMessages;
    private final Counter _numFinishedTasks;
    private final Counter _numFailedToSchedule;

    private final Histogram _endToLateBindingEnqueueLatencyHistogram;
    private final Histogram _endToLateBindingConfirmLatencyHistogram;

    public SchedulerServiceMetrics(MetricRegistry metrics, boolean isLateBinding) {
        _endToEndLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_SCHEDULING_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _endToEndMakespanHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_MAKESPAN_LATENCY_HISTOGRAMS,
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

    public void taskScheduled(long latency) {
        _endToEndLatencyHistogram.update(latency);
    }

    public void lateBindingEnqueue(long latency) {
        _endToLateBindingEnqueueLatencyHistogram.update(latency);
    }

    public void lateBindingConfirm(long latency) {
        _endToLateBindingConfirmLatencyHistogram.update(latency);
    }


    public void taskFinished(long makespan, long nodeWallTime, long taskDuration) {
        _numFinishedTasks.inc();
        _endToEndMakespanHistogram.update(makespan);
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
