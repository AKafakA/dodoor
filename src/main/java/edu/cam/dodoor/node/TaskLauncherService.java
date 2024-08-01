package edu.cam.dodoor.node;

import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskLauncherService {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLauncherService.class);

    private TaskScheduler _taskScheduler;
    private Node _node;
    private NodeServiceMetrics _nodeServiceMetrics;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                TaskSpec task = _taskScheduler.getNextTask(); // blocks until task is ready
                _nodeServiceMetrics.taskLaunched();
                LOG.debug("Received task{}", task._taskId);
                try {
                    Process process = executeLaunchTask(task);
                    Thread.sleep(task._duration);
                    process.destroy();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    _node.taskFinished(task.getFullTaskId());
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                long endToEndDuration = System.currentTimeMillis() - task._enqueuedTime;
                _nodeServiceMetrics.taskFinished(endToEndDuration);
                LOG.debug("Completed task {} on application backend at system time {}", task._taskId, System.currentTimeMillis());
            }

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

    public void initialize(Configuration conf, TaskScheduler taskScheduler, NodeThrift nodeThrift) {
        /* The number of threads used by the service. */
        int _numSlots = taskScheduler.getNumSlots();
        if (_numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            _numSlots = Resources.getSystemCores(conf);
        }
        _taskScheduler = taskScheduler;
        _node = nodeThrift._node;
        _nodeServiceMetrics = nodeThrift._nodeServiceMetrics;
        ExecutorService service = Executors.newFixedThreadPool(_numSlots);
        for (int i = 0; i < _numSlots; i++) {
            service.submit(new TaskLaunchRunnable());
        }
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
