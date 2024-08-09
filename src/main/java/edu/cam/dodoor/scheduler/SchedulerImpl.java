package edu.cam.dodoor.scheduler;


import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.scheduler.taskplacer.TaskPlacer;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerImpl implements Scheduler{

    private final static Logger LOG = LoggerFactory.getLogger(SchedulerImpl.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private final AtomicInteger _counter = new AtomicInteger(0);


    /** Thrift client pool for async communicating with node monitors */
    private final ThriftClientPool<NodeEnqueueService.AsyncClient> _nodeEnqueueServiceAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.NodeEnqueuServiceMakerFactory());

    private final ThriftClientPool<DataStoreService.AsyncClient> _dataStoreAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.DataStoreServiceMakerFactory());
    
    private Map<InetSocketAddress, NodeMonitorService.Client> _nodeEqueueSocketToNodeMonitorClients;
    private List<InetSocketAddress> _dataStoreAddress;

    private THostPort _address;
    private TaskPlacer _taskPlacer;
    private SchedulerServiceMetrics _schedulerServiceMetrics;
    private int _numTasksToUpdateDataStore;
    private Map<String, TNodeState> _nodeLoadChanges;
    private Map<InetSocketAddress, TNodeState> _loadMapEqueueSocketToNodeState;

    @Override
    public void initialize(Configuration config, InetSocketAddress socket,
                           SchedulerServiceMetrics schedulerServiceMetrics) throws IOException {
        _nodeLoadChanges = Maps.newConcurrentMap();
        _schedulerServiceMetrics = schedulerServiceMetrics;
        _address = Network.socketAddressToThrift(socket);
        _loadMapEqueueSocketToNodeState = Maps.newConcurrentMap();
        String schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        double beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
        _nodeEqueueSocketToNodeMonitorClients = Maps.newHashMap();
        _dataStoreAddress = new ArrayList<>();

        List<String> nmPorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_MONITOR_THRIFT_PORTS)));
        List<String> nePorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_ENQUEUE_THRIFT_PORTS)));

        if (nmPorts.size() != nePorts.size()) {
            throw new IllegalArgumentException(DodoorConf.NODE_MONITOR_THRIFT_PORTS + " and " +
                    DodoorConf.NODE_ENQUEUE_THRIFT_PORTS + " not of equal length");
        }
        if (nmPorts.isEmpty()) {
            nmPorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_MONITOR_THRIFT_PORT));
            nePorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_ENQUEUE_THRIFT_PORT));
        }
        for (String nodeIp : config.getStringArray(DodoorConf.STATIC_NODE)) {
            for (int i = 0; i < nmPorts.size(); i++) {
                String nodeFullAddress = nodeIp + ":" + nmPorts.get(i) + ":" + nePorts.get(i);
                try {
                    this.registerNode(nodeFullAddress);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        List<String> dataStorePorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.DATA_STORE_THRIFT_PORTS)));
        boolean isBatchScheduler = SchedulerUtils.isCachedEnabled(config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));
        if (isBatchScheduler) {
            for (String dataStoreIp : config.getStringArray(DodoorConf.STATIC_DATA_STORE)) {
                for (String dataStorePort : dataStorePorts) {
                    String dataStoreFullAddress = dataStoreIp + ":" + dataStorePort;
                    try {
                        this.registerDataStore(dataStoreFullAddress);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        _taskPlacer = TaskPlacer.createTaskPlacer(beta,
                schedulingStrategy, _nodeEqueueSocketToNodeMonitorClients);
        _numTasksToUpdateDataStore = config.getInt(DodoorConf.SCHEDULER_NUM_TASKS_TO_UPDATE,
                DodoorConf.DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE);
    }


    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        if (request.tasks.isEmpty()) {
            return;
        }
        int numTasksBefore = _counter.get();
        Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> mapOfNodesToPlacedTasks = handleJobSubmission(request);
        _counter.getAndAdd(request.tasks.size());
        _schedulerServiceMetrics.taskSubmitted(request.tasks.size());
        boolean needToUpdateDataStore = numTasksBefore / _numTasksToUpdateDataStore != _counter.get() / _numTasksToUpdateDataStore;
        for (InetSocketAddress nodeEnqueueAddress : mapOfNodesToPlacedTasks.keySet()) {
            String nodeEnqueueAddressStr = Serialization.getStrFromSocket(nodeEnqueueAddress);
            TResourceVector newRequestedResources = _nodeLoadChanges.get(nodeEnqueueAddressStr).resourceRequested;
            for (TEnqueueTaskReservationRequest task : mapOfNodesToPlacedTasks.get(nodeEnqueueAddress)) {
                newRequestedResources.cores += task.resourceRequested.cores;
                newRequestedResources.memory += task.resourceRequested.memory;
                newRequestedResources.disks += task.resourceRequested.disks;
            }
            _nodeLoadChanges.get(nodeEnqueueAddressStr).numTasks += mapOfNodesToPlacedTasks.get(nodeEnqueueAddress).size();
            if (needToUpdateDataStore) {
                LOG.debug("{} tasks scheduled. and need to update the datastore from scheduler side", _counter.get());
                for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
                    try {
                        DataStoreService.AsyncClient client = _dataStoreAsyncClientPool.borrowClient(dataStoreAddress);
                        client.addNodeLoads(_nodeLoadChanges, 1,
                                new addNodeLoadsCallback(request.requestId, dataStoreAddress, client));
                        resetNodeLoadChanges();
                    } catch (TException e) {
                        LOG.error("Error updating node state for node: {}", nodeEnqueueAddress.getHostName(), e);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void resetNodeLoadChanges() {
        for (String nodeEnqueueAddress : _nodeLoadChanges.keySet()) {
            _nodeLoadChanges.get(nodeEnqueueAddress).resourceRequested.cores = 0;
            _nodeLoadChanges.get(nodeEnqueueAddress).resourceRequested.memory = 0;
            _nodeLoadChanges.get(nodeEnqueueAddress).resourceRequested.disks = 0;
            _nodeLoadChanges.get(nodeEnqueueAddress).numTasks = 0;
        }
    }

    @Override
    public Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> handleJobSubmission(TSchedulingRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> mapOfNodesToPlacedTasks = Maps.newHashMap();
        long start = System.currentTimeMillis();
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> enqueueTaskReservationRequests
                = _taskPlacer.getEnqueueTaskReservationRequests(request, _loadMapEqueueSocketToNodeState, _address);

        for (Map.Entry<TEnqueueTaskReservationRequest, InetSocketAddress> entry :
                enqueueTaskReservationRequests.entrySet())  {
            try {
                NodeEnqueueService.AsyncClient client = _nodeEnqueueServiceAsyncClientPool.borrowClient(entry.getValue());
                LOG.debug("Launching enqueueTask for request {} on node: {}", request.requestId, entry.getValue().getHostName());
                client.enqueueTaskReservation(entry.getKey(), new EnqueueTaskReservationCallback(
                        entry.getKey().taskId, entry.getValue(), client, _schedulerServiceMetrics));
                if (!mapOfNodesToPlacedTasks.containsKey(entry.getValue())) {
                    mapOfNodesToPlacedTasks.put(entry.getValue(), new ArrayList<>());
                }
                mapOfNodesToPlacedTasks.get(entry.getValue()).add(entry.getKey());
            } catch (Exception e) {
                LOG.error("Error enqueuing task on node {}", entry.getValue().getHostName(), e);
            }
        }
        long end = System.currentTimeMillis();
        LOG.debug("All tasks enqueued for request {}; returning. Total time: {} milliseconds", request.requestId, end - start);
        return mapOfNodesToPlacedTasks;
    }


    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        _schedulerServiceMetrics.loadUpdated();
        for (Map.Entry<String, TNodeState> entry : snapshot.entrySet()) {
            Optional<InetSocketAddress> neAddressOptional = Serialization.strToSocket(entry.getKey());
            if (neAddressOptional.isPresent()) {
                InetSocketAddress nodeEnqueueSocket = neAddressOptional.get();
                if (_loadMapEqueueSocketToNodeState.containsKey(nodeEnqueueSocket)) {
                    LOG.debug("Updating load for node: {}", nodeEnqueueSocket.getHostName());
                } else {
                    LOG.error("Adding load for unregistered node: {}", nodeEnqueueSocket.getHostName());
                }
                _loadMapEqueueSocketToNodeState.put(nodeEnqueueSocket, entry.getValue());
            } else {
                LOG.error("Invalid address: {}", entry.getKey());
            }
        }
    }

    @Override
    public void registerNode(String nodeAddress) throws TException {
        String[] nodeAddressParts = nodeAddress.split(":");
        if (nodeAddressParts.length != 3) {
            throw new TException("Invalid address: " + nodeAddress);
        }
        String nodeIp = nodeAddressParts[0];
        String nodeMonitorPort = nodeAddressParts[1];
        String nodeEnqueuePort = nodeAddressParts[2];
        String nodeMonitorAddress = nodeIp + ":" + nodeMonitorPort;
        Optional<InetSocketAddress> nmAddress = Serialization.strToSocket(nodeMonitorAddress);
        String nodeEnqueueAddress = nodeIp + ":" + nodeEnqueuePort;
        Optional<InetSocketAddress> neAddress = Serialization.strToSocket(nodeEnqueueAddress);
        if (nmAddress.isPresent() && neAddress.isPresent()) {
            InetSocketAddress nmSocket = nmAddress.get();
            InetSocketAddress neSocket = neAddress.get();
            _loadMapEqueueSocketToNodeState.put(neSocket, new TNodeState(
                    new TResourceVector(0, 0, 0), 0));
            _nodeLoadChanges.put(Serialization.getStrFromSocket(neSocket), new TNodeState(
                    new TResourceVector(0, 0, 0), 0));
            // Pre-cache one client to avoid the slow start of the first enqueueTaskReservation RPC
            try {
                NodeEnqueueService.AsyncClient client = _nodeEnqueueServiceAsyncClientPool.borrowClient(neSocket);
                _nodeEnqueueServiceAsyncClientPool.returnClient(neSocket, client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                _nodeEqueueSocketToNodeMonitorClients.put(neSocket,
                        TClients.createBlockingNodeMonitorClient(nmSocket));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Registering node at {}", nmAddress.get().getHostName());
        } else {
            throw new TException("Invalid address: " + nodeMonitorAddress);
        }
    }

    @Override
    public void registerDataStore(String dataStoreAddress) throws TException {
        Optional<InetSocketAddress> dataStoreSocket = Serialization.strToSocket(dataStoreAddress);
        if (dataStoreSocket.isPresent()) {
            _dataStoreAddress.add(dataStoreSocket.get());
            // Pre-cache one client to avoid the slow start of the first addNodeLoads RPC
            try {
                DataStoreService.AsyncClient client = _dataStoreAsyncClientPool.borrowClient(dataStoreSocket.get());
                _dataStoreAsyncClientPool.returnClient(dataStoreSocket.get(), client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new TException("Invalid address: " + dataStoreAddress);
        }
    }

    private class EnqueueTaskReservationCallback implements AsyncMethodCallback<Boolean> {
        String _taskId;
        InetSocketAddress _nodeEnqueueAddress;
        long _startTimeMillis;
        NodeEnqueueService.AsyncClient _client;
        SchedulerServiceMetrics _schedulerServiceMetrics;

        public EnqueueTaskReservationCallback(String taskId, InetSocketAddress nodeEnqueueAddress,
                                              NodeEnqueueService.AsyncClient client,
                                              SchedulerServiceMetrics schedulerServiceMetrics) {
            _taskId = taskId;
            _nodeEnqueueAddress = nodeEnqueueAddress;
            _startTimeMillis = System.currentTimeMillis();
            _client = client;
            _schedulerServiceMetrics = schedulerServiceMetrics;
        }

        @Override
        public void onComplete(Boolean aBoolean) {
            if (!aBoolean) {
                LOG.error("Error enqueuing task on node {}", _nodeEnqueueAddress.getHostName());
            }
            long totalTime = System.currentTimeMillis() - _startTimeMillis;
            _schedulerServiceMetrics.taskScheduled(totalTime);
            LOG.debug("Enqueue Task RPC to {} for request {} completed in {} ms",
                    new Object[]{_nodeEnqueueAddress.getHostName(), _taskId, totalTime});
            returnClient();
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:{}", exception.getMessage());
            returnClient();
        }

        private void returnClient() {
            try {
                _nodeEnqueueServiceAsyncClientPool.returnClient(_nodeEnqueueAddress, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool: {}", e.getMessage());
            }
        }
    }

    private class addNodeLoadsCallback implements AsyncMethodCallback<Void> {
        long _requestId;
        InetSocketAddress _dataStoreAddress;
        DataStoreService.AsyncClient _client;

        public addNodeLoadsCallback(long requestId, InetSocketAddress dataStoreAddress,
                                   DataStoreService.AsyncClient client) {
            _requestId = requestId;
            _dataStoreAddress = dataStoreAddress;
            _client = client;
        }

        @Override
        public void onComplete(Void unused) {
            returnClient();
            LOG.debug(Logging.auditEventString("add_nodes_load_from_scheduler_to",
                    _dataStoreAddress.getHostName()));
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            returnClient();
            LOG.error("Error executing addNodeLoads RPC:{}", exception.getMessage());
        }

        private void returnClient() {
            try {
                _dataStoreAsyncClientPool.returnClient(_dataStoreAddress, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to data store client pool: {}", e.getMessage());
            }
        }
    }
}
