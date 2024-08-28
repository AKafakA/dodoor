package edu.cam.dodoor.node;

import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeImpl implements Node {
    private final static Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    private volatile TaskScheduler _taskScheduler;
    private AtomicInteger _requestedCores;
    private AtomicLong _requestedMemory;
    private AtomicLong _requestedDisk;
    private AtomicLong _totalDurations;

    // count number of enqueued tasks
    private AtomicInteger _waitingOrRunningTasksCounter;
    ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    private volatile NodeResources _nodeResources;
    private Map<String, InetSocketAddress> _taskSourceSchedulerAddress;
    private NodeThrift _nodeThrift;
    private Map<String, Long> _taskReceivedTime;

    @Override
    public void initialize(Configuration config, NodeThrift nodeThrift) {
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
        _taskReceivedTime = Maps.newConcurrentMap();
        // TODO(wda): add more task scheduler
        _nodeResources = new NodeResources(Resources.getSystemCoresCapacity(config),
                Resources.getMemoryMbCapacity(config), Resources.getSystemDiskGbCapacity(config));
        _taskScheduler = new FifoTaskScheduler(numSlots, _nodeResources);
        TaskLauncherService taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(config, numSlots, nodeThrift);
        _taskScheduler.initialize(config, taskLauncherService);

        if (config.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED)) {
            int trackingInterval = config.getInt(DodoorConf.TRACKING_INTERVAL_IN_SECONDS,
                    DodoorConf.DEFAULT_TRACKING_INTERVAL);
            MetricsTrackerService metricsTrackerService = new MetricsTrackerService(trackingInterval, config,
                    nodeThrift.getNodeServiceMetrics(), taskLauncherService);
            metricsTrackerService.start();
        }

        _requestedCores = new AtomicInteger();
        _requestedMemory = new AtomicLong();
        _requestedDisk = new AtomicLong();
        _totalDurations = new AtomicLong();
        _waitingOrRunningTasksCounter = new AtomicInteger(0);
        _dataStoreClientPool = new ThriftClientPool<>(new ThriftClientPool.DataStoreServiceMakerFactory());
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());
        _taskSourceSchedulerAddress = Maps.newConcurrentMap();
        _nodeThrift = nodeThrift;
    }

    @Override
    public void taskFinished(TFullTaskId task) throws TException {
        LOG.debug(Logging.functionCall(task));
        _requestedCores.getAndAdd(-task.resourceRequest.cores);
        _requestedMemory.getAndAdd(-task.resourceRequest.memory);
        _requestedDisk.getAndAdd(-task.resourceRequest.disks);
        _nodeResources.freeTask(task.resourceRequest.cores, task.resourceRequest.memory, task.resourceRequest.disks);
        _waitingOrRunningTasksCounter.getAndAdd(-1);
        _totalDurations.getAndAdd(task.durationInMs);
        sendRequestsPostTaskFinished(task);
        _taskScheduler.tasksFinished(task);
    }


    @Override
    public TResourceVector getRequestedResourceVector() {
        return new TResourceVector(_requestedCores.get(), _requestedMemory.get(), _requestedDisk.get());
    }

    @Override
    public TNodeState getNodeState() {
        return new TNodeState(getRequestedResourceVector(), _waitingOrRunningTasksCounter.get(), _totalDurations.get());
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation", request.taskId));
        _taskSourceSchedulerAddress.put(request.taskId, Network.thriftToSocket(request.schedulerAddress));
        _taskReceivedTime.put(request.taskId, System.currentTimeMillis());
        _taskScheduler.submitTaskReservation(request);
        _requestedCores.getAndAdd(request.resourceRequested.cores);
        _requestedMemory.getAndAdd(request.resourceRequested.memory);
        _requestedDisk.getAndAdd(request.resourceRequested.disks);

        _waitingOrRunningTasksCounter.getAndAdd(1);
        return true;
    }

    private void sendRequestsPostTaskFinished(TFullTaskId task) throws TException {
        LOG.debug(Logging.functionCall(task));
        for (InetSocketAddress dataStoreSocket : _nodeThrift.getDataStoreAddress()) {
            DataStoreService.AsyncClient dataStoreClient;
            LOG.debug("Update to the datastore service {} after task {} ",
                    new Object[] {dataStoreSocket.getHostName(), dataStoreSocket.getPort(), task.taskId});
            try {
                dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreSocket);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            String neAddress = _nodeThrift.getNeAddressStr();
            if (neAddress == null) {
                throw new TException("Node enqueue address is not set");
            }
            TNodeState nodeState = this.getNodeState();
            dataStoreClient.overrideNodeState(neAddress, nodeState,
                    new UpdateNodeLoadCallBack(dataStoreSocket, dataStoreClient));
        }

        InetSocketAddress schedulerSocketAddress = _taskSourceSchedulerAddress.get(task.taskId);
        if (schedulerSocketAddress != null) {
            SchedulerService.AsyncClient schedulerClient;
            try {
                LOG.debug("Task {} finished, sending task finished signal to scheduler {}",
                        new Object[] { task.taskId, schedulerSocketAddress});
                schedulerClient = _schedulerClientPool.borrowClient(schedulerSocketAddress);
                long nodeWallTime = System.currentTimeMillis() - _taskReceivedTime.get(task.taskId);
                schedulerClient.taskFinished(task, nodeWallTime,
                        new TaskFinishedCallBack(schedulerSocketAddress, schedulerClient));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.warn("Task {} finished, but no scheduler address found", task.taskId);
            LOG.debug("Current task source scheduler address map: {}", _taskSourceSchedulerAddress);
        }
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
            LOG.info(Logging.auditEventString("deliver_nodes_load_to_datastore",
                    _address.getHostName()));
            returnClient();
        }

        @Override
        public void onError(Exception e) {
            LOG.warn(Logging.auditEventString("failed_deliver_nodes_load_to_datastore",
                    _address.getHostName()));
            returnClient();
        }

        private void returnClient() {
            try {
                _dataStoreClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class TaskFinishedCallBack implements AsyncMethodCallback<Void> {
        private final SchedulerService.AsyncClient _client;
        private final InetSocketAddress _address;

        public TaskFinishedCallBack(InetSocketAddress address, SchedulerService.AsyncClient client) {
            _client = client;
            _address = address;
        }

        @Override
        public void onComplete(Void unused) {
            LOG.info(Logging.auditEventString("deliver_task_finished_to_scheduler",
                    _address.getHostName()));
            returnClient();
        }

        @Override
        public void onError(Exception e) {
            LOG.warn(Logging.auditEventString("failed_deliver_task_finished_to_scheduler",
                    _address.getHostName()));
            returnClient();
        }

        private void returnClient() {
            try {
                _schedulerClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
