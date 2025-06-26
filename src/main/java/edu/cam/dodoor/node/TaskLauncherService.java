package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskLauncherService {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLauncherService.class);
    private Node _node;
    private NodeServiceMetrics _nodeServiceMetrics;
    private ThreadPoolExecutor _executor;
    private TimeUnit _timeUnit;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {
        TaskSpec _task;
        TimeUnit _timeUnit;
        public TaskLaunchRunnable(TaskSpec task) {
            this(task, TimeUnit.MILLISECONDS);
        }

        public TaskLaunchRunnable(TaskSpec task, TimeUnit timeUnit) {
            _task = task;
            _timeUnit = timeUnit;
        }

        @Override
        public void run() {
            TaskSpec task = _task; // blocks until task is ready
            long waitingDuration = System.currentTimeMillis() - task._enqueuedTime;
            _nodeServiceMetrics.taskLaunched(waitingDuration);
            LOG.debug("Received task {}", task._taskId);
            try {
                TaskSimulator tasks = executeLaunchTask(task);
                long triggerTime = System.currentTimeMillis();
                tasks.simulate();
                long durationInMs = System.currentTimeMillis() - triggerTime;
                LOG.debug("Task {} completed in {} ms", task._taskId, durationInMs);
                _node.taskFinished(task.getFullTaskId());
                _nodeServiceMetrics.taskFinished();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.debug("Completed task {} on application backend at system time {}", task._taskId, System.currentTimeMillis());
        }

        /** Executes to launch a task */
        private TaskSimulator executeLaunchTask(TaskSpec task) throws IOException{
            long memory = task._resourceVector.memory;
            long disks = task._resourceVector.disks;
            int cpu = task._resourceVector.cores;
            long duration = task._duration;
            long durationInMs;
            if (_timeUnit == TimeUnit.MILLISECONDS) {
                durationInMs = duration;
            } else if (_timeUnit == TimeUnit.NANOSECONDS) {
                durationInMs = duration / 1000000;
            } else if (_timeUnit == TimeUnit.SECONDS) {
                durationInMs = duration * 1000;
            } else {
                throw new RuntimeException("Unsupported time unit " + _timeUnit);
            }
            LOG.debug("Launching task {} with {} duration in Ms", task._taskId, durationInMs);

            return new TaskSimulator(cpu, (int) (memory / (1024 * 1024 * 1024)),
                    (int) duration);
        }
    }

    public void initialize(Configuration conf, int numSlot, NodeThrift nodeThrift) {
        /* The number of threads used by the service. */
        int numSlots = numSlot;
        if (numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            numSlots = Resources.getSystemCoresCapacity(conf);
        }
        _node = nodeThrift._node;
        _nodeServiceMetrics = nodeThrift.getNodeServiceMetrics();
        LOG.debug("Initializing task launcher service with {} slots", numSlots);
        _executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numSlots);
        String timeUnitStr = conf.getString(DodoorConf.TASK_DURATION_TIME_UNIT, "MILLISECONDS");
        if (timeUnitStr.equals("MILLISECONDS")) {
            _timeUnit = TimeUnit.MILLISECONDS;
        } else if (timeUnitStr.equals("NANOSECONDS")) {
            _timeUnit = TimeUnit.NANOSECONDS;
        } else if (timeUnitStr.equals("SECONDS")) {
            _timeUnit = TimeUnit.SECONDS;
        } else {
            throw new RuntimeException("Unsupported time unit " + timeUnitStr);
        }
        LOG.debug("Task duration time unit is {}", _timeUnit);
    }

    public void tryToLaunchTask(TaskSpec task) {
        TaskLaunchRunnable taskLaunchRunnable = new TaskLaunchRunnable(task, _timeUnit);
        _executor.submit(taskLaunchRunnable);
    }

    public int getActiveTasks() {
        return _executor.getActiveCount();
    }

    public long getTaskCount() {
        return _executor.getTaskCount();
    }

    public long getCompletedTaskCount() {
        return _executor.getCompletedTaskCount();
    }

    public long getQueuedTaskCount() {
        return _executor.getQueue().size();
    }
}
