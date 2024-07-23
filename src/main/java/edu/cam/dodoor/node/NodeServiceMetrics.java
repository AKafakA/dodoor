package edu.cam.dodoor.node;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class NodeServiceMetrics {

    private final Meter _tasksRate;
    private final Counter _waitingTasksCounter;
    private final Counter _finishedTasksCounter;
    MetricRegistry _metrics;

    public NodeServiceMetrics(MetricRegistry metrics) {
        _metrics = metrics;
        _tasksRate = _metrics.meter("node.tasks.rate");
        _waitingTasksCounter = _metrics.counter("node.waiting.tasks");
        _finishedTasksCounter = _metrics.counter("node.finished.tasks");
    }

    public void taskEnqueued() {
        _waitingTasksCounter.inc();
        _tasksRate.mark();
    }

    public void taskLaunched() {
        _waitingTasksCounter.dec();
        _finishedTasksCounter.inc();
    }

    public void taskFinished() {
        _finishedTasksCounter.inc();
    }
}
