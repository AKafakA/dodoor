package edu.cam.dodoor.node;

import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TaskLauncherService {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLauncherService.class);
    private Node _node;
    private NodeServiceMetrics _nodeServiceMetrics;
    ThreadPoolExecutor _executor;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {
        TaskSpec _task;
        public TaskLaunchRunnable(TaskSpec task) {
            _task = task;
        }

        @Override
        public void run() {
            TaskSpec task = _task; // blocks until task is ready
            long waitingDuration = System.currentTimeMillis() - task._enqueuedTime;
            _nodeServiceMetrics.taskLaunched(waitingDuration);
            LOG.debug("Received task {}", task._taskId);
            try {
                Process process = executeLaunchTask(task);
                long pid = process.pid();
                LOG.debug("Task {} launched with pid {}", task._taskId, pid);
                Thread.sleep(task._duration);
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
                LOG.debug("Task {} completed", task._taskId);
                _node.taskFinished(task.getFullTaskId());
                _nodeServiceMetrics.taskFinished();
                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(process.getErrorStream()));
                if (!stdError.readLine().isEmpty()) {
                    LOG.error("Task {} failed to execute with error {}", task._taskId, stdError.readLine());
                }
            } catch (IOException | InterruptedException | TException e) {
                throw new RuntimeException(e);
            }
            LOG.debug("Completed task {} on application backend at system time {}", task._taskId, System.currentTimeMillis());
        }

        /** Executes to launch a task */
        private Process executeLaunchTask(TaskSpec task) throws IOException, InterruptedException {
            Runtime rt = Runtime.getRuntime();
            long memory = task._resourceVector.memory;
            long disks = task._resourceVector.disks;
            int cpu = task._resourceVector.cores;
            long duration = task._duration;
            long durationInSec = duration / 1000;
            return rt.exec(
                    generateStressCommand(cpu, memory, disks, durationInSec));
        }
    }

    public void initialize(Configuration conf, int numSlot, NodeThrift nodeThrift) {
        /* The number of threads used by the service. */
        int numSlots = numSlot;
        if (numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            numSlots = Resources.getSystemCores(conf);
        }
        _node = nodeThrift._node;
        _nodeServiceMetrics = nodeThrift._nodeServiceMetrics;
        LOG.debug("Initializing task launcher service with {} slots", numSlots);
        _executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numSlots);
    }

    public void tryToLaunchTask(TaskSpec task) {
        TaskLaunchRunnable taskLaunchRunnable = new TaskLaunchRunnable(task);
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

    public long getCorePoolSize() {
        return _executor.getCorePoolSize();
    }

    private String generateStressCommand(int cores, long memory, long disks, long durationInSec) {
        if (disks > 0) {
            cores = cores - 1; // hdd itself is cpu intensive and need to consume one core
            if (cores < 1) {
                cores = 1;
            }
        }

        if (disks <= 0 && memory <= 0) {
            return String.format("stress -c %d --timeout %d", cores, durationInSec);
        } else if (disks <= 0) {
            return String.format("stress -c %d --vm 1 --vm-bytes %dM --vm-hang %d --timeout %d",
                    cores, memory, durationInSec, durationInSec);
        } else if (memory <= 0) {
            return String.format("stress -c %d --hdd 1 --hdd-bytes %dM --timeout %d", cores, disks, durationInSec);
        } else {
            return String.format("stress -c %d --vm 1 --vm-bytes %dM --vm-hang %d -d 1 --hdd-bytes %dM --timeout %d",
                    cores, memory, durationInSec, disks, durationInSec);
        }
    }
}
