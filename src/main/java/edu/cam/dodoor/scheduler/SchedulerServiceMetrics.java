package edu.cam.dodoor.scheduler;

import com.codahale.metrics.*;
import edu.cam.dodoor.DodoorConf;


public class SchedulerServiceMetrics {

    private final Histogram _endToEndLatencyHistogram;
    private final Histogram _endToEndMakespanHistogram;
    private final Meter _tasksRate;
    private final Meter _loadUpdateRate;
    private final Counter _totalMessages;

    public SchedulerServiceMetrics(MetricRegistry metrics) {
        _endToEndLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_SCHEDULING_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _tasksRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_TASK_RATE);
        _loadUpdateRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_LOAD_UPDATE_RATE);
        _totalMessages = metrics.counter(DodoorConf.SCHEDULER_METRICS_NUM_MESSAGES);
        _endToEndMakespanHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_MAKESPAN_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
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

    public void taskFinished(long makespan, long nodeWallTime) {
        _endToEndMakespanHistogram.update(makespan);
        _endToEndLatencyHistogram.update(makespan - nodeWallTime);
    }
}
