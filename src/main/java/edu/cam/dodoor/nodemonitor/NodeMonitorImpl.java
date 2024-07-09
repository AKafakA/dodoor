package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeMonitorImpl implements NodeMonitor{
    private final static Logger LOG = Logger.getLogger(NodeMonitorImpl.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

    private TaskScheduler _taskScheduler;
    private String _ipAddress;
    TResourceVector _requestedResources;
    private AtomicInteger _requested_cores;
    private AtomicLong _requested_memory;
    private AtomicLong _requested_disk;

    @Override
    public void initialize(Configuration config, int internalPort) {
        _ipAddress = Network.getIPAddress(config);
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
        // TODO(wda): add more task scheduler
        _taskScheduler = new FifoTaskScheduler(numSlots);
        _taskScheduler.initialize(config, internalPort);
        TaskLauncherService taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(config, _taskScheduler, internalPort);

        _requested_cores = new AtomicInteger();
        _requested_memory = new AtomicLong();
        _requested_disk = new AtomicLong();
    }

    @Override
    public void taskFinished(List<TFullTaskId> tasks) {
        _taskScheduler.tasksFinished(tasks);

        for (TFullTaskId task : tasks) {
            _requested_cores.getAndAdd(-task.resourceRequest.cores);
            _requested_memory.getAndAdd(-task.resourceRequest.memory);
            _requested_disk.getAndAdd(-task.resourceRequest.disks);
        }
    }


    @Override
    public void cancelTaskReservations(TCancelTaskReservationsRequest request) throws TException {
        TResourceVector resourceVector = _taskScheduler.cancelTaskReservations(request.requestId);
        _requested_cores.getAndAdd(-resourceVector.cores);
        _requested_memory.getAndAdd(-resourceVector.memory);
        _requested_disk.getAndAdd(-resourceVector.disks);
    }

    @Override
    public TResourceVector getRequestedResourceVector() {
        return _requestedResources;
    }

    @Override
    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation",
                _ipAddress, request.taskId));
        LOG.info("Received enqueue task reservation request from " + _ipAddress + " for request " +
                request.taskId);

        _taskScheduler.submitTaskReservations(request);

        _requested_cores.getAndAdd(request.resourceRequested.cores);
        _requested_memory.getAndAdd(request.resourceRequested.memory);
        _requested_disk.getAndAdd(request.resourceRequested.disks);
        return true;
    }
}
