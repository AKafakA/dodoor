package edu.cam.dodoor.nodemonitor;


import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskScheduler {

    /**
     * Class define a task to be scheduled to be run within the nodes
     */
    protected static class TaskSpec {
        public String _appId;
        public TUserGroupInfo _user;
        public String _requestId;

        public InetSocketAddress _schedulerAddress;
        public InetSocketAddress _appBackendAddress;

        /**
         * ID of the task that previously ran in the slot this task is using. Used
         * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
         * immediately, because there were empty slots available on the slave.  Filled in when
         * the task is launched.
         */
        public String _previousRequestId;
        public String _previousTaskId;

        /** Filled in after the getTask() RPC completes. */
        public TTaskLaunchSpec _taskSpec;

        public TaskSpec(TEnqueueTaskReservationsRequest request, InetSocketAddress appBackendAddress) {
            _appId = request.getAppId();
            _user = request.getUser();
            _requestId = request.getRequestId();
            _schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                    request.getSchedulerAddress().getPort());
            _appBackendAddress = appBackendAddress;
            _previousRequestId = "";
            _previousTaskId = "";
        }
    }

    private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
    private String _ipAddress;

    protected Configuration _conf;
    private final BlockingQueue<TaskSpec> _runnableTaskQueue =
            new LinkedBlockingQueue<>();

    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration conf, int nodeMonitorPort) {
        _conf = conf;
        _ipAddress = Network.getIPAddress(conf);
    }

    TaskSpec getNextTask() {
        TaskSpec task = null;
        try {
            task = _runnableTaskQueue.take();
        } catch (InterruptedException e) {
            LOG.fatal(e);
        }
        return task;
    }
}
