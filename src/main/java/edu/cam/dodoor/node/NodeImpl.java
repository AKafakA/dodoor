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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeImpl implements Node {
    private final static Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    private volatile TaskScheduler _taskScheduler;
    TResourceVector _requestedResources;
    private AtomicInteger _requested_cores;
    private AtomicLong _requested_memory;
    private AtomicLong _requested_disk;

    // count number of enqueued tasks
    private AtomicInteger _waitingOrRunningTasksCounter;
    ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _dataStoreAddress;
    private String _neAddress;
    private volatile NodeResources _nodeResources;
    private Map<String, InetSocketAddress> _taskSourceSchedulerAddress;

    @Override
    public void initialize(Configuration config, NodeThrift nodeThrift) {
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
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
                    nodeThrift._nodeServiceMetrics, taskLauncherService);
            metricsTrackerService.start();
        }

        _requested_cores = new AtomicInteger();
        _requested_memory = new AtomicLong();
        _requested_disk = new AtomicLong();
        _waitingOrRunningTasksCounter = new AtomicInteger(0);
        _dataStoreClientPool = new ThriftClientPool<>(new ThriftClientPool.DataStoreServiceMakerFactory());
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());
        _taskSourceSchedulerAddress = Maps.newConcurrentMap();
        _dataStoreAddress = nodeThrift._dataStoreAddress;
        _neAddress = nodeThrift._neAddress;
    }

    @Override
    public void taskFinished(TFullTaskId task) throws TException {
        LOG.debug(Logging.functionCall(task));
        _requested_cores.getAndAdd(-task.resourceRequest.cores);
        _requested_memory.getAndAdd(-task.resourceRequest.memory);
        _requested_disk.getAndAdd(-task.resourceRequest.disks);
        _nodeResources.freeTask(task.resourceRequest.cores, task.resourceRequest.memory, task.resourceRequest.disks);
        _waitingOrRunningTasksCounter.getAndAdd(-1);
        sendRequestsPostTaskFinished(task);
        _taskScheduler.tasksFinished(task);
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
        _taskSourceSchedulerAddress.put(request.taskId, Network.thriftToSocket(request.schedulerAddress));

        _taskScheduler.submitTaskReservation(request);
        _requested_cores.getAndAdd(request.resourceRequested.cores);
        _requested_memory.getAndAdd(request.resourceRequested.memory);
        _requested_disk.getAndAdd(request.resourceRequested.disks);

        _waitingOrRunningTasksCounter.getAndAdd(1);
        return true;
    }

    private void sendRequestsPostTaskFinished(TFullTaskId task) throws TException {
        LOG.debug(Logging.functionCall(task));
        LOG.debug("Current data store address: {}", _dataStoreAddress);
        for (InetSocketAddress dataStoreSocket : _dataStoreAddress) {
            DataStoreService.AsyncClient dataStoreClient;
            LOG.debug("Update to the datastore service {} after task {} ",
                    new Object[] {dataStoreSocket.getHostName(), dataStoreSocket.getPort(), task.taskId});
            try {
                dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreSocket);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            TNodeState nodeState = new TNodeState(this.getRequestedResourceVector(), this.getNumTasks());
            dataStoreClient.overrideNodeState(_neAddress, nodeState,
                    new UpdateNodeLoadCallBack(dataStoreSocket, dataStoreClient));
            LOG.debug(Logging.auditEventString("update_node_load_to_datastore",
                    dataStoreSocket.getAddress(), dataStoreSocket.getPort()));
        }

        InetSocketAddress schedulerSocketAddress = _taskSourceSchedulerAddress.get(task.taskId);
        if (schedulerSocketAddress != null) {
            SchedulerService.AsyncClient schedulerClient;
            try {
                LOG.debug("Task {} finished, sending task finished signal to scheduler {}",
                        new Object[] { task.taskId, schedulerSocketAddress});
                schedulerClient = _schedulerClientPool.borrowClient(schedulerSocketAddress);
                schedulerClient.taskFinished(task, new TaskFinishedCallBack(schedulerSocketAddress, schedulerClient));
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
