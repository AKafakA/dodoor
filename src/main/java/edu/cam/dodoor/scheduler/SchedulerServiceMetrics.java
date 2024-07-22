package edu.cam.dodoor.scheduler;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.cam.dodoor.DodoorConf;


public class SchedulerServiceMetrics {

    private final Histogram _endToEndLatencyHistogram;
    private Meter _tasksRate;
    private Meter _loadUpdateRate;

    public SchedulerServiceMetrics(MetricRegistry metrics) {
        _endToEndLatencyHistogram = metrics.histogram(DodoorConf.SCHEDULER_END_TO_END_TASK_LATENCY );
        _tasksRate = metrics.meter(DodoorConf.SCHEDULER_TASK_RATE);
        _loadUpdateRate = metrics.meter(DodoorConf.SCHEDULER_LOAD_UPDATE_RATE);
    }

    public void taskSubmitted(int numTasks) {
        _tasksRate.mark(numTasks);
    }

    public void loadUpdated() {
        _loadUpdateRate.mark();
    }

    public void taskFinished(long latency) {
        _endToEndLatencyHistogram.update(latency);
    }
}
