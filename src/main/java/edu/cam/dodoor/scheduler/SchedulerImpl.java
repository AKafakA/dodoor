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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerImpl implements Scheduler{

    private final static Logger LOG = LoggerFactory.getLogger(SchedulerImpl.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private final AtomicInteger _counter = new AtomicInteger(0);


    /** Thrift client pool for async communicating with node monitors */
    private final ThriftClientPool<NodeEnqueueService.AsyncClient> _nodeEnqueueServiceAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.NodeEnqueuServiceMakerFactory());

    private final ThriftClientPool<NodeMonitorService.AsyncClient> _nodeMonitorServiceAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.NodeMonitorServiceMakerFactory());

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
    private Map<String, Long> _taskReceivedTime;
    private Map<String, InetSocketAddress> _nodeAddressToNeSocket;
    private Map<InetSocketAddress, InetSocketAddress> _neSocketToNmSocket;
    private String _schedulingStrategy;
    Map<InetSocketAddress, Integer> _probeReuseCount;
    private int _probeRateForPrequal;
    private int _probePoolSize;

    private int _delta;
    private int _probeDeleteRate;


    @Override
    public void initialize(Configuration config, THostPort localAddress,
                           SchedulerServiceMetrics schedulerServiceMetrics) throws IOException {
        _nodeLoadChanges = Maps.newConcurrentMap();
        _schedulerServiceMetrics = schedulerServiceMetrics;
        _address = localAddress;
        _loadMapEqueueSocketToNodeState = Maps.newConcurrentMap();
        _taskReceivedTime = new HashMap<>();
        _schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        double beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
        _nodeEqueueSocketToNodeMonitorClients = Collections.synchronizedMap(new HashMap<>());
        _dataStoreAddress = new ArrayList<>();
        _nodeAddressToNeSocket = Maps.newHashMap();
        _neSocketToNmSocket = Maps.newHashMap();
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

        if (_schedulingStrategy.equals(DodoorConf.PREQUAL)) {
            _probeRateForPrequal = config.getInt(DodoorConf.PREQUAL_PROBE_RATIO, DodoorConf.DEFAULT_PREQUAL_PROBE_RATIO);
            double _rifQuantile = config.getDouble(DodoorConf.PREQUAL_RIF_QUANTILE, DodoorConf.DEFAULT_PREQUAL_RIF_QUANTILE);
            long _prequalProbeRemoveInterval = config.getLong(DodoorConf.PREQUAL_PROBE_REMOVE_INTERVAL_SECONDS,
                    DodoorConf.DEFAULT_PREQUAL_PROBE_REMOVE_INTERVAL_SECONDS);
            _probeReuseCount = Collections.synchronizedMap(new LinkedHashMap<>());
            _probePoolSize = config.getInt(DodoorConf.PREQUAL_PROBE_POOL_SIZE, DodoorConf.DEFAULT_PREQUAL_PROBE_POOL_SIZE);
            _delta = config.getInt(DodoorConf.PREQUAL_DELTA, DodoorConf.DEFAULT_PREQUAL_DELTA);
            _probeDeleteRate = config.getInt(DodoorConf.PREQUAL_PROBE_DELETE, DodoorConf.DEFAULT_PREQUAL_PROBE_DELETE);
            Thread probePoolRemover = new Thread(new PrequalProbePoolRemover(_probeReuseCount, _probePoolSize,
                    _prequalProbeRemoveInterval, _loadMapEqueueSocketToNodeState, _rifQuantile));
            probePoolRemover.start();
        }

        _taskPlacer = TaskPlacer.createTaskPlacer(beta,
                _nodeEqueueSocketToNodeMonitorClients,
                schedulerServiceMetrics,
                config,
                _nodeMonitorServiceAsyncClientPool,
                _nodeAddressToNeSocket,
                _neSocketToNmSocket,
                _probeReuseCount);
        _numTasksToUpdateDataStore = config.getInt(DodoorConf.SCHEDULER_NUM_TASKS_TO_UPDATE,
                DodoorConf.DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE);
    }

    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        if (!SchedulerUtils.isCachedEnabled(_schedulingStrategy)) {
            throw new RuntimeException("updateNodeState should not be called for non-cached scheduler");
        }
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
                LOG.debug("Current node {} load is {}", nodeEnqueueSocket.getHostName(), _loadMapEqueueSocketToNodeState.get(nodeEnqueueSocket));
            } else {
                LOG.error("Invalid address: {}", entry.getKey());
            }
        }
    }

    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        long start = System.currentTimeMillis();
        if (request.tasks.isEmpty()) {
            return;
        }
        int numTasksBefore = _counter.get();
        for (TTaskSpec task : request.tasks) {
            _taskReceivedTime.put(task.taskId, System.currentTimeMillis());
        }
        Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> mapOfNodesToPlacedTasks = handleJobSubmission(request,
                start);
        _counter.getAndAdd(request.tasks.size());
        _schedulerServiceMetrics.taskSubmitted(request.tasks.size());
        if (SchedulerUtils.isCachedEnabled(_schedulingStrategy)) {
            updateDataStoreLoad(numTasksBefore, request, mapOfNodesToPlacedTasks);
        } else if (_schedulingStrategy.equals(DodoorConf.PREQUAL)) {
            updatePrequalPool();
        }
    }

    private void updatePrequalPool() {
        Random ran = new Random();
        Set<InetSocketAddress> neToProbe = new HashSet<>();
        InetSocketAddress[] neAddresses = _neSocketToNmSocket.keySet().toArray(new InetSocketAddress[0]);
        for (int i = 0; i < _probeRateForPrequal ; i++) {
            int index = ran.nextInt(_neSocketToNmSocket.size());
            neToProbe.add(neAddresses[index]);
        }
        for (InetSocketAddress neSocket : neToProbe) {
            InetSocketAddress nmSocket = _neSocketToNmSocket.get(neSocket);
            try {
                NodeMonitorService.AsyncClient client = _nodeMonitorServiceAsyncClientPool.borrowClient(nmSocket);
                client.getNodeState(new GetNodeStateWithUpdateCallBack(neSocket, nmSocket, client, _probeReuseCount));
                _schedulerServiceMetrics.probeNode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateDataStoreLoad(int numTasksBefore, TSchedulingRequest request,
                                     Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> mapOfNodesToPlacedTasks) {
        boolean needToUpdateDataStore = numTasksBefore / _numTasksToUpdateDataStore != _counter.get() / _numTasksToUpdateDataStore;
        for (InetSocketAddress nodeEnqueueAddress : mapOfNodesToPlacedTasks.keySet()) {
            String nodeEnqueueAddressStr = Serialization.getStrFromSocket(nodeEnqueueAddress);
            TResourceVector newRequestedResources = _nodeLoadChanges.get(nodeEnqueueAddressStr).resourceRequested;
            long newTotalDurations = 0;
            for (TEnqueueTaskReservationRequest task : mapOfNodesToPlacedTasks.get(nodeEnqueueAddress)) {
                newRequestedResources.cores = task.resourceRequested.cores + newRequestedResources.cores;
                newRequestedResources.memory = task.resourceRequested.memory + newRequestedResources.memory;
                newRequestedResources.disks = task.resourceRequested.disks + newRequestedResources.disks;
                newTotalDurations += task.durationInMs;
            }
            _nodeLoadChanges.get(nodeEnqueueAddressStr).totalDurations = newTotalDurations + _nodeLoadChanges.get(nodeEnqueueAddressStr).totalDurations;
            _nodeLoadChanges.get(nodeEnqueueAddressStr).numTasks = mapOfNodesToPlacedTasks.get(nodeEnqueueAddress).size() + _nodeLoadChanges.get(nodeEnqueueAddressStr).numTasks;
            if (needToUpdateDataStore) {
                LOG.debug("{} tasks scheduled. and need to update the datastore from scheduler side", _counter.get());
                for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
                    try {
                        DataStoreService.AsyncClient client = _dataStoreAsyncClientPool.borrowClient(dataStoreAddress);
                        client.addNodeLoads(_nodeLoadChanges, 1,
                                new addNodeLoadsCallback(request.requestId, dataStoreAddress, client));
                        resetNodeLoadChanges();
                        _schedulerServiceMetrics.updateToDataStore();
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
    public Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> handleJobSubmission(TSchedulingRequest request,
                                                                                            long startTime) throws TException {
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
                        entry.getKey().taskId, entry.getValue(), client, _schedulerServiceMetrics, startTime));
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
                    new TResourceVector(0, 0, 0), 0, 0, nodeIp));
            _nodeLoadChanges.put(Serialization.getStrFromSocket(neSocket), new TNodeState(
                    new TResourceVector(0, 0, 0), 0, 0, nodeIp));
            _nodeAddressToNeSocket.put(nodeIp, neSocket);
            _neSocketToNmSocket.put(neSocket, nmSocket);
            if (!SchedulerUtils.isCachedEnabled(_schedulingStrategy) && !SchedulerUtils.isAsyncScheduler(_schedulingStrategy)) {
                try {
                    _nodeEqueueSocketToNodeMonitorClients.put(neSocket,
                            TClients.createBlockingNodeMonitorClient(nmSocket));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                LOG.info("Adding sync node monitor client for node: {} for scheduler {}", nmSocket.getHostName(),
                        _schedulingStrategy);
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
        } else {
            throw new TException("Invalid address: " + dataStoreAddress);
        }
    }

    @Override
    public void taskFinished(TFullTaskId taskId, long nodeWallTime) throws TException {
        LOG.debug("Task {} finished", taskId.taskId);
        if (!_taskReceivedTime.containsKey(taskId.taskId)) {
            LOG.error("Task {} finished but not found in taskReceivedTime", taskId.taskId);
            return;
        }
        long taskDuration = System.currentTimeMillis() - _taskReceivedTime.get(taskId.taskId);
        _schedulerServiceMetrics.taskFinished(taskDuration, nodeWallTime, taskId.durationInMs);
    }

    private class EnqueueTaskReservationCallback implements AsyncMethodCallback<Boolean> {
        String _taskId;
        InetSocketAddress _nodeEnqueueAddress;
        long _startTimeMillis;
        NodeEnqueueService.AsyncClient _client;
        SchedulerServiceMetrics _schedulerServiceMetrics;

        public EnqueueTaskReservationCallback(String taskId, InetSocketAddress nodeEnqueueAddress,
                                              NodeEnqueueService.AsyncClient client,
                                              SchedulerServiceMetrics schedulerServiceMetrics,
                                              long startTimeMillis) {
            _taskId = taskId;
            _nodeEnqueueAddress = nodeEnqueueAddress;
            _startTimeMillis = startTimeMillis;
            _client = client;
            _schedulerServiceMetrics = schedulerServiceMetrics;
        }

        @Override
        public void onComplete(Boolean aBoolean) {
            if (!aBoolean) {
                LOG.error("Error enqueuing task on node {}", _nodeEnqueueAddress.getHostName());
            }
            LOG.debug("Enqueue Task RPC to {} for request {} completed in {} ms",
                    new Object[]{_nodeEnqueueAddress.getHostName(), _taskId, System.currentTimeMillis() - _startTimeMillis});

            long totalTime = System.currentTimeMillis() - _startTimeMillis;
            _schedulerServiceMetrics.taskScheduled(totalTime);
            returnClient();
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:{}", exception.getMessage());
            _schedulerServiceMetrics.failedToScheduling();
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

    private class GetNodeStateWithUpdateCallBack implements AsyncMethodCallback<edu.cam.dodoor.thrift.TNodeState> {
        private final NodeMonitorService.AsyncClient _client;
        private final InetSocketAddress _neAddress;
        private final InetSocketAddress _nmAddress;
        private final Map<InetSocketAddress, Integer> _probeReuseCount;

        public GetNodeStateWithUpdateCallBack(InetSocketAddress neAddress,
                                              InetSocketAddress nmAddress,
                                              NodeMonitorService.AsyncClient client,
                                              Map<InetSocketAddress, Integer> probeReuseCount) {
            if (client == null) {
                throw new IllegalArgumentException("Client cannot be null");
            }
            if (!neAddress.getAddress().equals(nmAddress.getAddress())){
                throw new IllegalArgumentException("Node monitor address and node enqueue address should have the same IP");
            }
            _client = client;
            _neAddress = neAddress;
            _nmAddress = nmAddress;
            _probeReuseCount = probeReuseCount;
        }

        @Override
        public void onComplete(TNodeState nodeState) {
            LOG.info("Node state received from {}", _nmAddress.getHostName());
            _loadMapEqueueSocketToNodeState.put(_neAddress, nodeState);
            _probeReuseCount.remove(_neAddress);
            _probeReuseCount.put(_neAddress, 0);
            returnClient();
        }

        @Override
        public void onError(Exception e) {
            LOG.warn("Failed to get node state from {}", _nmAddress.getHostName());
            returnClient();
        }

        private void returnClient() {
            try {
                _nodeMonitorServiceAsyncClientPool.returnClient(_nmAddress, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class PrequalProbePoolRemover implements Runnable {

        final Map<InetSocketAddress, Integer> _probeReuseCount;
        int _probePoolSize;
        long _prequalProbeRemoveInterval;
        Map<InetSocketAddress, TNodeState> _loadMapEqueueSocketToNodeState;
        double _rifQuantile;

        public PrequalProbePoolRemover(Map<InetSocketAddress, Integer> probeReuseCount,
                                       int probePoolSize,
                                       long prequalProbeRemoveInterval,
                                       Map<InetSocketAddress, TNodeState> loadMapEqueueSocketToNodeState,
                                       double rifQuantile) {
            _probeReuseCount = probeReuseCount;
            _probePoolSize = probePoolSize;
            _prequalProbeRemoveInterval = prequalProbeRemoveInterval;
            _loadMapEqueueSocketToNodeState = loadMapEqueueSocketToNodeState;
            _rifQuantile = rifQuantile;
        }

        @Override
        public void run(){
            do {
                removeNodeFromPrequalPool();
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(_prequalProbeRemoveInterval));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (true);
        }

        private void removeNodeFromPrequalPool() {
            int[] numPendingTasks = _loadMapEqueueSocketToNodeState.values().stream().mapToInt(e -> e.numTasks).toArray();
            int cutoff = MetricsUtils.getQuantile(numPendingTasks, _rifQuantile);
            // The reverse order of the probe addresses by insertion.
            List<InetSocketAddress> reversedProbeAddresses = new ArrayList<>(_probeReuseCount.keySet());
            Collections.reverse(reversedProbeAddresses);
            Map<InetSocketAddress, TNodeState> prequalLoadMaps = new LinkedHashMap<>();
            Set<InetSocketAddress> addressesToRemove = new HashSet<>();
            int probeReuseBudget = SchedulerUtils.getProbeReuseBudget(_loadMapEqueueSocketToNodeState.size(),
                    _probePoolSize,
                    _probeRateForPrequal, _probeDeleteRate, _delta);
            LOG.debug("Clean the probe pool with budget: " + probeReuseBudget);
            for (int i = 0; i < reversedProbeAddresses.size(); i++) {
                InetSocketAddress address = reversedProbeAddresses.get(i);
                if (i < _probePoolSize && _probeReuseCount.get(address) < probeReuseBudget) {
                    prequalLoadMaps.put(reversedProbeAddresses.get(i),
                            _loadMapEqueueSocketToNodeState.get(reversedProbeAddresses.get(i)));
                } else {
                    addressesToRemove.add(address);
                }
            }
            // since the insertion order is reversed, the oldest node is the last one.
            if (!prequalLoadMaps.isEmpty()) {
                InetSocketAddress oldestAddress = prequalLoadMaps.keySet().stream().reduce((first, second) -> second).get();
                addressesToRemove.add(oldestAddress);
                if (addressesToRemove.size() < _probeReuseCount.size()) {
                    java.util.Optional<Map.Entry<InetSocketAddress, TNodeState>> worstNode =  prequalLoadMaps.entrySet().stream()
                            .filter(e -> e.getValue().numTasks >= cutoff)
                            .max(Comparator.comparingLong(e -> e.getValue().totalDurations))
                            .or(() -> prequalLoadMaps.entrySet().stream().max(Comparator.comparingInt(e -> e.getValue().numTasks)));
                    addressesToRemove.add(worstNode.get().getKey());
                }
            }
            for (InetSocketAddress addressToRemove: addressesToRemove) {
                _probeReuseCount.remove(addressToRemove);
            }
        }
    }
}
