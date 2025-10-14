package org.anon.scheduler.node;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.anon.scheduler.DodoorConf;
import com.codahale.metrics.UniformReservoir;

public class NodeServiceMetrics {

    private final Meter _tasksRate;
    private final Counter _waitingTasksCounter;
    private final Counter _finishedTasksCounter;
    private final Histogram _taskWaitTimeHistogram;
    private final Counter _holBlockedCounter;
    private final Counter _holBlockedDurationMs;
    MetricRegistry _metrics;

    public NodeServiceMetrics(MetricRegistry metrics) {
        _metrics = metrics;
        _tasksRate = _metrics.meter(DodoorConf.NODE_METRICS_TASKS_RATE);
        _waitingTasksCounter = _metrics.counter(DodoorConf.NODE_METRICS_WAITING_TASKS);
        _finishedTasksCounter = _metrics.counter(DodoorConf.NODE_METRICS_FINISHED_TASKS);
        _taskWaitTimeHistogram = _metrics.histogram(DodoorConf.NODE_METRICS_TASKS_WAIT_TIME_HISTOGRAMS,
                () -> new Histogram(new UniformReservoir()));
        _holBlockedCounter = _metrics.counter(DodoorConf.NODE_METRICS_HOL_BLOCKED_COUNT);
        _holBlockedDurationMs = _metrics.counter(DodoorConf.NODE_METRICS_HOL_BLOCKED_DURATION_MS);
    }

    public void taskEnqueued() {
        _waitingTasksCounter.inc();
        _tasksRate.mark();
    }

    public void taskCancelled() {
        _waitingTasksCounter.dec();
        _tasksRate.mark();
    }

    public void taskLaunched(long waitingDuration) {
        _waitingTasksCounter.dec();
        _taskWaitTimeHistogram.update(waitingDuration);
    }

    public void taskFinished() {
        _finishedTasksCounter.inc();
    }

    public void holBlockedOnce() {
        _holBlockedCounter.inc();
    }

    public void holBlockedFor(long blockedMs) {
        if (blockedMs > 0) {
            _holBlockedDurationMs.inc(blockedMs);
        }
    }
}
