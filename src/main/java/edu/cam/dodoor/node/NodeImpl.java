package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeImpl implements Node{
    private final static Logger LOG = Logger.getLogger(NodeImpl.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

    private TaskScheduler _taskScheduler;
    TResourceVector _requestedResources;
    private AtomicInteger _requested_cores;
    private AtomicLong _requested_memory;
    private AtomicLong _requested_disk;

    // count number of enqueued tasks
    private AtomicInteger _counter;

    @Override
    public void initialize(Configuration config, NodeThrift nodeThrift) {
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
        // TODO(wda): add more task scheduler
        _taskScheduler = new FifoTaskScheduler(numSlots);
        _taskScheduler.initialize(config);
        TaskLauncherService taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(config, _taskScheduler, nodeThrift);

        if (config.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED)) {
            int trackingInterval = config.getInt(DodoorConf.TRACKING_INTERVAL_IN_MS,
                    DodoorConf.DEFAULT_TRACKING_INTERVAL);
            MetricsTrackerService metricsTrackerService = new MetricsTrackerService(trackingInterval, config);
            metricsTrackerService.start();
        }

        _requested_cores = new AtomicInteger();
        _requested_memory = new AtomicLong();
        _requested_disk = new AtomicLong();
        _counter = new AtomicInteger(0);
    }

    @Override
    public void taskFinished(TFullTaskId task) {
        _taskScheduler.tasksFinished(task);
        _requested_cores.getAndAdd(-task.resourceRequest.cores);
        _requested_memory.getAndAdd(-task.resourceRequest.memory);
        _requested_disk.getAndAdd(-task.resourceRequest.disks);
        _counter.getAndAdd(-1);
    }


    @Override
    public TResourceVector getRequestedResourceVector() {
        return _requestedResources;
    }

    @Override
    public int getNumTasks() throws TException {
        return _counter.get();
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation", request.taskId));

        _taskScheduler.submitTaskReservation(request);
        _requested_cores.getAndAdd(request.resourceRequested.cores);
        _requested_memory.getAndAdd(request.resourceRequested.memory);
        _requested_disk.getAndAdd(request.resourceRequested.disks);

        _counter.getAndAdd(1);
        return true;
    }
}
