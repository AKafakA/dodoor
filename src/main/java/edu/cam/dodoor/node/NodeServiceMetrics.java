package edu.cam.dodoor.node;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.cam.dodoor.DodoorConf;
import com.codahale.metrics.UniformReservoir;

public class NodeServiceMetrics {

    private final Meter _tasksRate;
    private final Counter _waitingTasksCounter;
    private final Counter _finishedTasksCounter;
    private final Histogram _taskWaitTimeHistogram;
    MetricRegistry _metrics;

    public NodeServiceMetrics(MetricRegistry metrics) {
        _metrics = metrics;
        _tasksRate = _metrics.meter(DodoorConf.NODE_METRICS_TASKS_RATE);
        _waitingTasksCounter = _metrics.counter(DodoorConf.NODE_METRICS_WAITING_TASKS);
        _finishedTasksCounter = _metrics.counter(DodoorConf.NODE_METRICS_FINISHED_TASKS);
        _taskWaitTimeHistogram = _metrics.histogram(DodoorConf.NODE_METRICS_TASKS_WAIT_TIME_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
    }

    public void taskEnqueued() {
        _waitingTasksCounter.inc();
        _tasksRate.mark();
    }

    public void taskLaunched() {
        _waitingTasksCounter.dec();
        _finishedTasksCounter.inc();
    }

    public void taskFinished(long endToEndDuration) {
        _taskWaitTimeHistogram.update(endToEndDuration);
        _finishedTasksCounter.inc();
    }
}
