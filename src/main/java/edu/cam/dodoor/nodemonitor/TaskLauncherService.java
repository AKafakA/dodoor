package edu.cam.dodoor.nodemonitor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Logging;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import edu.cam.dodoor.utils.TClients;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskLauncherService {
    private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskLauncherService.class);

    private THostPort _nodeMonitorInternalAddress;

    private TaskScheduler _taskScheduler;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {

        /** Client to use to communicate with each scheduler (indexed by scheduler hostname). */
        private final HashMap<String, GetTaskService.Client> _schedulerClients = Maps.newHashMap();

        /** Client to use to communicate with each application backend (indexed by backend address). */
        private final HashMap<InetSocketAddress, BackendService.Client> _backendClients = Maps.newHashMap();

        @Override
        public void run() {
            while (true) {
                TaskSpec task = _taskScheduler.getNextTask(); // blocks until task is ready

                List<TTaskLaunchSpec> taskLaunchSpecs = executeGetTaskRpc(task);
                AUDIT_LOG.info(Logging.auditEventString("node_monitor_get_task_complete", task._requestId,
                        _nodeMonitorInternalAddress.getHost()));

                if (taskLaunchSpecs.isEmpty()) {
                    LOG.debug("Didn't receive a task for request " + task._requestId);
                    _taskScheduler.noTaskForReservation(task);
                    continue;
                }
                if (taskLaunchSpecs.size() > 1) {
                    LOG.warn("Received " + taskLaunchSpecs +
                            " task launch specifications; ignoring all but the first one.");
                }
                task._taskSpec = taskLaunchSpecs.get(0);
                LOG.debug("Received task for request " + task._requestId + ", task " +
                        task._taskSpec.getTaskId());

                // Launch the task on the backend.
                AUDIT_LOG.info(Logging.auditEventString("node_monitor_task_launch",
                        task._requestId,
                        _nodeMonitorInternalAddress.getHost(),
                        task._taskSpec.getTaskId(),
                        task._previousRequestId,
                        task._previousTaskId));
                executeLaunchTaskRpc(task);
                LOG.debug("Launched task " + task._taskSpec.getTaskId() + " for request " + task._requestId +
                        " on application backend at system time " + System.currentTimeMillis());
            }

        }

        /** Uses a getTask() RPC to get the task specification from the appropriate scheduler. */
        private List<TTaskLaunchSpec> executeGetTaskRpc(TaskSpec task) {
            String schedulerAddress = task._schedulerAddress.getAddress().getHostAddress();
            if (!_schedulerClients.containsKey(schedulerAddress)) {
                try {
                    _schedulerClients.put(schedulerAddress,
                            TClients.createBlockingGetTaskClient(
                                    task._schedulerAddress.getAddress().getHostAddress(),
                                    DodoorConf
                                            .DEFAULT_GET_TASK_PORT));
                } catch (Exception e) {
                    LOG.error("Error creating thrift client: " + e.getMessage());
                    return Lists.newArrayList();
                }
            }
            GetTaskService.Client getTaskClient = _schedulerClients.get(schedulerAddress);

            long startTimeMillis = System.currentTimeMillis();
            long startGCCount = Logging.getGCCount();

            LOG.debug("Attempting to get task for request " + task._requestId);
            AUDIT_LOG.debug(Logging.auditEventString("node_monitor_get_task_launch", task._requestId,
                    _nodeMonitorInternalAddress.getHost()));
            List<TTaskLaunchSpec> taskLaunchSpecs;
            try {
                taskLaunchSpecs = getTaskClient.getTask(task._requestId, _nodeMonitorInternalAddress);
            } catch (TException e) {
                LOG.error("Error when launching getTask RPC:" + e.getMessage());
                return Lists.newArrayList();
            }

            long rpcTime = System.currentTimeMillis() - startTimeMillis;
            long numGarbageCollections = Logging.getGCCount() - startGCCount;
            LOG.debug("GetTask() RPC for request " + task._requestId + " completed in " +  rpcTime +
                    "ms (" + numGarbageCollections + "GCs occured during RPC)");
            return taskLaunchSpecs;
        }

        /** Executes an RPC to launch a task on an application backend. */
        private void executeLaunchTaskRpc(TaskSpec task) {
            if (!_backendClients.containsKey(task._appBackendAddress)) {
                try {
                    _backendClients.put(task._appBackendAddress,
                            TClients.createBlockingBackendClient(task._appBackendAddress));
                } catch (Exception e) {
                    LOG.error("Error creating thrift client: " + e.getMessage());
                    return;
                }
            }
            BackendService.Client backendClient = _backendClients.get(task._appBackendAddress);
            THostPort schedulerHostPort = Network.socketAddressToThrift(task._schedulerAddress);
            TFullTaskId taskId = new TFullTaskId(task._taskSpec.getTaskId(), task._requestId,
                    schedulerHostPort, task._appId, task._taskSpec.getResourceRequested());
            try {
                backendClient.launchTask(task._taskSpec.bufferForMessage(), taskId, task._user,
                        task._taskSpec.resourceRequested);
            } catch (TException e) {
                LOG.error("Unable to launch task on backend " + task._appBackendAddress + ":" + e);
            }
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
