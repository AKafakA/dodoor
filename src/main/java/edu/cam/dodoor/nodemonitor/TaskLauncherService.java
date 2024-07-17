package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskLauncherService {
    private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);

    /** Thrift client pool for async communicating with node monitors */
    private final ThriftClientPool<NodeMonitorService.AsyncClient> _nodeMonitorAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.NodeMonitorServiceMakerFactory());
    private TaskScheduler _taskScheduler;
    InetSocketAddress _nmAddress;

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
                    NodeMonitorService.AsyncClient client = _nodeMonitorAsyncClientPool.borrowClient(_nmAddress);
                    client.tasksFinished(task.getFullTaskId(), new FinishedTaskCallBack(task._taskId, _nmAddress, client));
                } catch (Exception e) {
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

    public void initialize(Configuration conf, TaskScheduler taskScheduler, InetSocketAddress nodeMonitorAddress) {
        /* The number of threads used by the service. */
        int _numSlots = taskScheduler.getNumSlots();
        if (_numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            _numSlots = (int) Resources.getSystemCPUCount(conf);
        }
        _taskScheduler = taskScheduler;
        _nmAddress = nodeMonitorAddress;
        ExecutorService service = Executors.newFixedThreadPool(_numSlots);
        for (int i = 0; i < _numSlots; i++) {
            service.submit(new TaskLaunchRunnable());
        }
    }


    private class FinishedTaskCallBack implements AsyncMethodCallback<Void> {
        String _taskId;
        InetSocketAddress _nodeMonitorAddress;
        NodeMonitorService.AsyncClient _client;

        public FinishedTaskCallBack(String taskId, InetSocketAddress nodeMonitorAddress,
                                    NodeMonitorService.AsyncClient client) {
            _taskId = taskId;
            _nodeMonitorAddress = nodeMonitorAddress;
            _client = client;
        }

        @Override
        public void onComplete(Void response) {
            try {
                _nodeMonitorAsyncClientPool.returnClient(_nodeMonitorAddress, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool for task executors: " + e);
            }
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing finishTask RPC:" + exception);
        }
    }
}
