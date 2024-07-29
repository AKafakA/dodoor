package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeImpl implements Node{
    private final static Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    private TaskScheduler _taskScheduler;
    TResourceVector _requestedResources;
    private AtomicInteger _requested_cores;
    private AtomicLong _requested_memory;
    private AtomicLong _requested_disk;

    // count number of enqueued tasks
    private AtomicInteger _waitingOrRunningTasksCounter;
    private AtomicInteger _finishedTasksCounter;
    ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    private int _numTasksToUpdate;
    List<InetSocketAddress> _dataStoreAddress;
    private String _neAddress;

    @Override
    public void initialize(Configuration config, NodeThrift nodeThrift) {
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
        // TODO(wda): add more task scheduler
        _taskScheduler = new FifoTaskScheduler(numSlots);
        _taskScheduler.initialize(config);
        TaskLauncherService taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(config, _taskScheduler, nodeThrift);

        if (config.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED)) {
            int trackingInterval = config.getInt(DodoorConf.TRACKING_INTERVAL_IN_SECONDS,
                    DodoorConf.DEFAULT_TRACKING_INTERVAL);
            MetricsTrackerService metricsTrackerService = new MetricsTrackerService(trackingInterval, config,
                    nodeThrift._nodeServiceMetrics);
            metricsTrackerService.start();
        }

        _requested_cores = new AtomicInteger();
        _requested_memory = new AtomicLong();
        _requested_disk = new AtomicLong();
        _waitingOrRunningTasksCounter = new AtomicInteger(0);
        _finishedTasksCounter = new AtomicInteger(0);
        _dataStoreClientPool = nodeThrift._dataStoreClientPool;

        _numTasksToUpdate = config.getInt(DodoorConf.NODE_NUM_TASKS_TO_UPDATE,
                DodoorConf.DEFAULT_NODE_NUM_TASKS_TO_UPDATE);
        _dataStoreAddress = nodeThrift._dataStoreAddress;
        _neAddress = nodeThrift._neAddress;
    }

    @Override
    public synchronized void taskFinished(TFullTaskId task) throws TException {
        _taskScheduler.tasksFinished(task);
        _requested_cores.getAndAdd(-task.resourceRequest.cores);
        _requested_memory.getAndAdd(-task.resourceRequest.memory);
        _requested_disk.getAndAdd(-task.resourceRequest.disks);

        _waitingOrRunningTasksCounter.getAndAdd(-1);

        int numFinishedTasks = _finishedTasksCounter.incrementAndGet();

        if (numFinishedTasks % _numTasksToUpdate == 0) {
            for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
                DataStoreService.AsyncClient dataStoreClient = null;
                try {
                    dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreAddress);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                TNodeState nodeState = new TNodeState(this.getRequestedResourceVector(), this.getNumTasks());
                dataStoreClient.overrideNodeState(_neAddress, nodeState,
                        new UpdateNodeLoadCallBack(dataStoreAddress, dataStoreClient));
                LOG.debug(Logging.auditEventString("update_node_load_to_datastore",
                        dataStoreAddress.getAddress(), dataStoreAddress.getPort()));
            }
        }
    }


    @Override
    public TResourceVector getRequestedResourceVector() {
        return _requestedResources;
    }

    @Override
    public int getNumTasks() {
        return _waitingOrRunningTasksCounter.get();
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation", request.taskId));

        _taskScheduler.submitTaskReservation(request);
        _requested_cores.getAndAdd(request.resourceRequested.cores);
        _requested_memory.getAndAdd(request.resourceRequested.memory);
        _requested_disk.getAndAdd(request.resourceRequested.disks);

        _waitingOrRunningTasksCounter.getAndAdd(1);
        return true;
    }

    private class UpdateNodeLoadCallBack implements AsyncMethodCallback<Void> {
        private final DataStoreService.AsyncClient _client;
        private final InetSocketAddress _address;

        public UpdateNodeLoadCallBack(InetSocketAddress address, DataStoreService.AsyncClient client) {
            _client = client;
            _address = address;
        }

        @Override
        public void onComplete(Void unused) {
            LOG.info(Logging.auditEventString("deliver_nodes_load_to_scheduler",
                    _address.getHostName()));
            try {
                _dataStoreClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception e) {
            LOG.warn(Logging.auditEventString("failed_deliver_nodes_load_to_scheduler",
                    _address.getHostName()));
        }

    }
}
