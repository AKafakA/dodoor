package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Logging;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskLauncherService {
    private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskLauncherService.class);

    private THostPort _nodeMonitorInternalAddress;

    private TaskScheduler _taskScheduler;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                TaskSpec task = _taskScheduler.getNextTask(); // blocks until task is ready
                AUDIT_LOG.info(Logging.auditEventString("node_monitor_get_task_complete", task._requestId,
                        _nodeMonitorInternalAddress.getHost()));

                LOG.debug("Received task for request " + task._requestId + ", task " +
                        task._requestId);

                // Launch the task on the backend.
                AUDIT_LOG.info(Logging.auditEventString("node_monitor_task_launch",
                        task._requestId,
                        _nodeMonitorInternalAddress.getHost(),
                        task._requestId,
                        task._previousRequestId,
                        task._previousTaskId));
                try {
                    executeLaunchTask(task);
                    Thread.sleep(task._duration);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                LOG.debug("Launched task " + task._requestId + " for request " + task._requestId +
                        " on application backend at system time " + System.currentTimeMillis());
            }

        }

        /** Executes to launch a task */
        private void executeLaunchTask(TaskSpec task) throws IOException {
            Runtime rt = Runtime.getRuntime();
            int cpu = task._resourceVector.cores;
            long memory = task._resourceVector.memory;
            long disks = task._resourceVector.disks;
            long duration = task._duration;
            rt.exec(
                    String.format("stress -c %d --vm 1 --vm-bytes %dM -d 1 --hdd-bytes %dM --timeout %d",
                            cpu, memory, disks, duration));
        }
    }

    public void initialize(Configuration conf, TaskScheduler taskScheduler,
                           int nodeMonitorPort) {
        /* The number of threads used by the service. */
        int _numThreads = taskScheduler.getMaxActiveTasks();
        if (_numThreads <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            _numThreads = (int) Resources.getSystemCPUCount(conf);
        }
        this._taskScheduler = taskScheduler;
        _nodeMonitorInternalAddress = new THostPort(Network.getIPAddress(conf), nodeMonitorPort);
        ExecutorService service = Executors.newFixedThreadPool(_numThreads);
        for (int i = 0; i < _numThreads; i++) {
            service.submit(new TaskLaunchRunnable());
        }
    }
}
