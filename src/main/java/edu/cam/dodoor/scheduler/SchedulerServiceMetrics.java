package edu.cam.dodoor.scheduler;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import edu.cam.dodoor.DodoorConf;


public class SchedulerServiceMetrics {

    private final Histogram _endToEndLatencyHistogram;
    private final Meter _tasksRate;
    private final Meter _loadUpdateRate;

    public SchedulerServiceMetrics(MetricRegistry metrics) {
        _endToEndLatencyHistogram = metrics.histogram(
                DodoorConf.SCHEDULER_METRICS_END_TO_END_TASK_SCHEDULING_LATENCY_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _tasksRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_TASK_RATE);
        _loadUpdateRate = metrics.meter(DodoorConf.SCHEDULER_METRICS_LOAD_UPDATE_RATE);
    }

    public void taskSubmitted(int numTasks) {
        _tasksRate.mark(numTasks);
    }

    public void loadUpdated() {
        _loadUpdateRate.mark();
    }

    public void taskScheduled(long latency) {
        _endToEndLatencyHistogram.update(latency);
    }
}
