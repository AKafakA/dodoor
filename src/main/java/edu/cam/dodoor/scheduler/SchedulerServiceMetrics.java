package edu.cam.dodoor.scheduler;

import com.codahale.metrics.*;
import edu.cam.dodoor.DodoorConf;


public class SchedulerServiceMetrics {

    private final Histogram _endToEndSchedulingLatencyHistogram;
    private final Histogram _endToEndLatencyHistogram;
    private final Meter _tasksRate;
    private final Meter _loadUpdateRate;
    private final Counter _numSchedulingMessages;

    public SchedulerServiceMetrics(MetricRegistry metrics) {
        _endToEndSchedulingLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_SCHEDULING_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _endToEndLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _tasksRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_TASK_RATE);
        _loadUpdateRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_LOAD_UPDATE_RATE);
        _numSchedulingMessages = metrics.counter(DodoorConf.SCHEDULER_METRICS_NUM_MESSAGES);
    }

    public Counter getSchedulerNumMessages() {
        return _numSchedulingMessages;
    }

    public void taskSubmitted(int numTasks) {
        _tasksRate.mark(numTasks);
    }

    public void loadUpdated() {
        _loadUpdateRate.mark();
    }

    public void taskScheduled(long latency) {
        _numSchedulingMessages.inc();
        _endToEndSchedulingLatencyHistogram.update(latency);
    }

    public void taskFinished(long latency) {
        _endToEndLatencyHistogram.update(latency);
    }
}
