package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskLauncherService {
    private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);

    private TaskScheduler _taskScheduler;
    private NodeMonitorThrift _nodeMonitorThrift;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                TaskSpec task = _taskScheduler.getNextTask(); // blocks until task is ready
                LOG.debug("Received task" + task._taskId);
                try {
                    Process process = executeLaunchTask(task);
                    Thread.sleep(task._duration);
                    process.destroy();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    _nodeMonitorThrift.tasksFinished(task.getFullTaskId());
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                LOG.debug("Completed task " + task._taskId +
                        " on application backend at system time " + System.currentTimeMillis());
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

    public void initialize(Configuration conf, TaskScheduler taskScheduler, NodeMonitorThrift nodeMonitorThrift) {
        /* The number of threads used by the service. */
        int _numSlots = taskScheduler.getNumSlots();
        if (_numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            _numSlots = (int) Resources.getSystemCPUCount(conf);
        }
        _taskScheduler = taskScheduler;
        _nodeMonitorThrift = nodeMonitorThrift;
        ExecutorService service = Executors.newFixedThreadPool(_numSlots);
        for (int i = 0; i < _numSlots; i++) {
            service.submit(new TaskLaunchRunnable());
        }
    }
}
