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
    private NodeThrift _nodeThrift;

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
                    _nodeThrift.tasksFinished(task.getFullTaskId());
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                _nodeServiceMetrics.taskFinished();
                LOG.debug("Completed task {} on application backend at system time {}", task._taskId, System.currentTimeMillis());
            }

        }

        /** Executes to launch a task */
        private Process executeLaunchTask(TaskSpec task) throws IOException, InterruptedException {
            Runtime rt = Runtime.getRuntime();
            int cpu = task._resourceVector.cores;
            long memory = task._resourceVector.memory;
            long disks = task._resourceVector.disks;
            long duration = task._duration;
            return rt.exec(
                    String.format("stress -c %d --vm 1 --vm-bytes %dM -d 1 --hdd-bytes %dM --timeout %d",
                            cpu, memory, disks, duration));
        }
    }

    public void initialize(Configuration conf, TaskScheduler taskScheduler, NodeThrift nodeThrift) {
        /* The number of threads used by the service. */
        int _numSlots = taskScheduler.getNumSlots();
        if (_numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            _numSlots = (int) Resources.getSystemCPUCount(conf);
        }
        _taskScheduler = taskScheduler;
        _nodeThrift = nodeThrift;
        _nodeServiceMetrics = _nodeThrift._nodeServiceMetrics;
        ExecutorService service = Executors.newFixedThreadPool(_numSlots);
        for (int i = 0; i < _numSlots; i++) {
            service.submit(new TaskLaunchRunnable());
        }
    }
}
