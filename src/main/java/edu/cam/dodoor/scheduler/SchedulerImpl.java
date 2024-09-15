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
    Map<InetSocketAddress, Map.Entry<Long, Integer>> _probeInfo;
    private int _probeRateForPrequal;
    private double _rifQuantile;
    private int _probeDeleteRate;
    private int _delta;
    private int _probePoolSize;
    private int _probeAgeBudget;

    private Map<String, Set<InetSocketAddress>> _nodePreservedForTask;

    private int _roundOfReservations;
    private Set<String> _tasks;
    private Map<String, Set<InetSocketAddress>> _nodeAskToExecute;
    private Map<String, TEnqueueTaskReservationRequest> _taskToRequest;
    // Used to track the latency of the task be enqueued
    private Map<String, Long> _taskEnqueueTime;


    @Override
    public void initialize(Configuration config, THostPort localAddress,
                           SchedulerServiceMetrics schedulerServiceMetrics) throws IOException {
        _nodeLoadChanges = Maps.newConcurrentMap();
        _schedulerServiceMetrics = schedulerServiceMetrics;
        _address = localAddress;
        _loadMapEqueueSocketToNodeState = Maps.newConcurrentMap();
        _taskReceivedTime = new HashMap<>();
        _taskEnqueueTime = Maps.newConcurrentMap();
        _schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        double beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
        _nodeEqueueSocketToNodeMonitorClients = Collections.synchronizedMap(new HashMap<>());
        _dataStoreAddress = new ArrayList<>();
        _nodeAddressToNeSocket = Maps.newHashMap();
        _neSocketToNmSocket = Maps.newHashMap();
        List<String> nmPorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_MONITOR_THRIFT_PORTS)));
        List<String> nePorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_ENQUEUE_THRIFT_PORTS)));

        _tasks = Collections.synchronizedSet(new HashSet<>());

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
            _probeRateForPrequal = config.getInt(DodoorConf.PREQUAL_PROBE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_RATE);
            _rifQuantile = config.getDouble(DodoorConf.PREQUAL_RIF_QUANTILE, DodoorConf.DEFAULT_PREQUAL_RIF_QUANTILE);
            _probeInfo = Collections.synchronizedMap(new LinkedHashMap<>());
            _probeDeleteRate = config.getInt(DodoorConf.PREQUAL_PROBE_DELETE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_DELETE_RATE);
            _delta = config.getInt(DodoorConf.PREQUAL_DELTA, DodoorConf.DEFAULT_PREQUAL_DELTA);
            _probePoolSize = config.getInt(DodoorConf.PREQUAL_PROBE_POOL_SIZE, DodoorConf.DEFAULT_PREQUAL_PROBE_POOL_SIZE);
            _probeAgeBudget = config.getInt(DodoorConf.PREQUAL_PROBE_AGE_BUDGET_MS, DodoorConf.DEFAULT_PREQUAL_PROBE_AGE_BUDGET_MS);
        }

        _taskPlacer = TaskPlacer.createTaskPlacer(beta,
                _nodeEqueueSocketToNodeMonitorClients,
                schedulerServiceMetrics,
                config,
                _nodeMonitorServiceAsyncClientPool,
                _nodeAddressToNeSocket,
                _neSocketToNmSocket,
                _probeInfo);
        _numTasksToUpdateDataStore = config.getInt(DodoorConf.SCHEDULER_NUM_TASKS_TO_UPDATE,
                DodoorConf.DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE);

        if (SchedulerUtils.isLateBindingScheduler(_schedulingStrategy)) {
            _nodePreservedForTask = Collections.synchronizedMap(new HashMap<>());
            _roundOfReservations = config.getInt(DodoorConf.LATE_BINDING_PROBE_COUNT, DodoorConf.DEFAULT_LATE_BINDING_PROBE_COUNT);
            _nodeAskToExecute = Collections.synchronizedMap(new HashMap<>());
            _taskToRequest = Collections.synchronizedMap(new HashMap<>());
        } else {
            _nodePreservedForTask = null;
            _roundOfReservations = 1;
        }
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
                LOG.debug("Current node {} load is {}", nodeEnqueueSocket.getHostName(),
                        _loadMapEqueueSocketToNodeState.get(nodeEnqueueSocket));
            } else {
                LOG.error("Invalid address: {}", entry.getKey());
            }
        }
    }

    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        long start = System.currentTimeMillis();
        int numTasksBefore = _counter.get();
        if (request.tasks.isEmpty()) {
            return;
        }
        _schedulerServiceMetrics.taskSubmitted(request.tasks.size());
        for (TTaskSpec task : request.tasks) {
            if (_tasks.contains(task.taskId)) {
                String newTaskId = task.taskId + "-" + System.currentTimeMillis();
                LOG.error("Task {} already submitted, renamed to {}", task.taskId, newTaskId);
                task.taskId = newTaskId;
            }
            _taskReceivedTime.put(task.taskId, System.currentTimeMillis());
        }
        Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> mapOfNodesToPlacedTasks = new HashMap<>();
        for (int i = 0; i < _roundOfReservations; i++) {
            Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> placedTasks = handleJobSubmission(request, start);
            for (InetSocketAddress nodeEnqueueAddress : placedTasks.keySet()) {
                if (!mapOfNodesToPlacedTasks.containsKey(nodeEnqueueAddress)) {
                    mapOfNodesToPlacedTasks.put(nodeEnqueueAddress, new ArrayList<>());
                }
                mapOfNodesToPlacedTasks.get(nodeEnqueueAddress).addAll(placedTasks.get(nodeEnqueueAddress));
                for (TEnqueueTaskReservationRequest task : placedTasks.get(nodeEnqueueAddress)) {
                    if (_nodePreservedForTask != null) {
                        _nodePreservedForTask.putIfAbsent(task.taskId,new HashSet<>());
                        _nodePreservedForTask.get(task.taskId).add(nodeEnqueueAddress);
                    }
                    if (_taskToRequest != null) {
                        _taskToRequest.put(task.taskId, task);
                    }
                }
            }
        }
        _counter.getAndAdd(request.tasks.size());
        if (SchedulerUtils.isCachedEnabled(_schedulingStrategy)) {
            updateDataStoreLoad(numTasksBefore, request, mapOfNodesToPlacedTasks);
        } else if (_schedulingStrategy.equals(DodoorConf.PREQUAL)) {
            synchronized (_probeInfo) {
                updatePrequalPool();
            }
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
                client.getNodeState(new GetNodeStateWithUpdateCallBack(neSocket, nmSocket, client, _probeInfo));
                _schedulerServiceMetrics.probeNode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        removeNodeFromPrequalPool();
    }

    private synchronized void removeNodeFromPrequalPool() {
        Random ran = new Random();
        int probeReuseBudget = SchedulerUtils.getProbeReuseBudget(_loadMapEqueueSocketToNodeState.size(),
                _probeInfo.size(), _probeRateForPrequal, _probeDeleteRate, _delta);
        long currentTime = System.currentTimeMillis();
        List<InetSocketAddress> reversedProbeAddresses = new ArrayList<>(_probeInfo.keySet());
        Collections.reverse(reversedProbeAddresses);
        for (int i = 0; i < reversedProbeAddresses.size(); i++) {
            InetSocketAddress probeAddress = reversedProbeAddresses.get(i);
            long probeTime = _probeInfo.get(probeAddress).getKey();
            if (i >= _probePoolSize || _probeInfo.get(probeAddress).getValue() >= probeReuseBudget ||
                    (currentTime - probeTime) >= _probeAgeBudget) {
                _probeInfo.remove(probeAddress);
                reversedProbeAddresses.remove(probeAddress);
            }
        }
        for (int i = 0; i < _probeDeleteRate && !reversedProbeAddresses.isEmpty(); i++) {
            InetSocketAddress probeAddressToRemove = reversedProbeAddresses.get(reversedProbeAddresses.size() - 1);
            if (ran.nextBoolean()) {
                int[] numPendingTasks = _loadMapEqueueSocketToNodeState.values().stream().mapToInt(e -> e.numTasks).toArray();
                int cutoff = MetricsUtils.getQuantile(numPendingTasks, _rifQuantile);
                probeAddressToRemove = selectWorstNodeFromPrequalPool(cutoff, reversedProbeAddresses);
            }
            _probeInfo.remove(probeAddressToRemove);
            reversedProbeAddresses.remove(probeAddressToRemove);
        }
    }

    private InetSocketAddress selectWorstNodeFromPrequalPool(int cutOff, List<InetSocketAddress> probeAddressesFILO) {
        InetSocketAddress selectedHotNode = null;
        InetSocketAddress selectedColdNode = probeAddressesFILO.get(probeAddressesFILO.size() - 1);
        int maxLoad = 0;
        long maxDuration = 0;
        for (InetSocketAddress nodeEnqueueAddress : probeAddressesFILO) {
            TNodeState nodeState = _loadMapEqueueSocketToNodeState.get(nodeEnqueueAddress);
            if (nodeState.numTasks > cutOff && nodeState.numTasks >= maxLoad) {
                selectedHotNode = nodeEnqueueAddress;
                maxLoad = nodeState.numTasks;
            } else {
                if (nodeState.totalDurations >= maxDuration) {
                    selectedColdNode = nodeEnqueueAddress;
                    maxDuration = nodeState.totalDurations;
                }

            }
        }
        return selectedHotNode != null ? selectedHotNode : selectedColdNode;
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
            _nodeLoadChanges.get(nodeEnqueueAddressStr).totalDurations =
                    newTotalDurations + _nodeLoadChanges.get(nodeEnqueueAddressStr).totalDurations;
            _nodeLoadChanges.get(nodeEnqueueAddressStr).numTasks = mapOfNodesToPlacedTasks.get(nodeEnqueueAddress).size()
                    + _nodeLoadChanges.get(nodeEnqueueAddressStr).numTasks;
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

    @Override
    public boolean confirmTaskReadyToExecute(TFullTaskId taskId, String nodeAddressStr) throws TException {
        _schedulerServiceMetrics.taskReadyToExecute();
        long triggerTime = System.currentTimeMillis();
        if (_schedulingStrategy.equals(DodoorConf.SPARROW_SCHEDULER)) {
            Optional<InetSocketAddress> nodeAddress = Serialization.strToSocket(nodeAddressStr);
            Set<InetSocketAddress> preservedNodes = _nodePreservedForTask.get(taskId.taskId);
            if (nodeAddress.isPresent()) {
                InetSocketAddress nodeEnqueueAddress = nodeAddress.get();
                _nodeAskToExecute.putIfAbsent(taskId.taskId, new HashSet<>());
                _nodeAskToExecute.get(taskId.taskId).add(nodeEnqueueAddress);
                LOG.debug("Task {} ready to execute", taskId.taskId);
                if (!_nodePreservedForTask.containsKey(taskId.taskId)) {
                    LOG.debug("Task {} not preserved for any node and may has been executed.", taskId.taskId);
                    return false;
                }
                if (!preservedNodes.contains(nodeEnqueueAddress)) {
                    LOG.debug("Task {} not preserved for node {}", taskId.taskId, nodeEnqueueAddress.getHostName());
                }
                try {
                    NodeEnqueueService.AsyncClient client =
                            _nodeEnqueueServiceAsyncClientPool.borrowClient(nodeEnqueueAddress);
                    _schedulerServiceMetrics.infoNodeToExecute();
                    Set<InetSocketAddress> nodesToCancel = _nodePreservedForTask.get(taskId.taskId);
                    nodesToCancel.remove(nodeEnqueueAddress);
                    _nodePreservedForTask.remove(taskId.taskId);
                    TEnqueueTaskReservationRequest task = _taskToRequest.get(taskId.taskId);
                    client.executeTask(task, new ExecuteTaskCallBack(nodeEnqueueAddress, client,
                            taskId, nodesToCancel, triggerTime));
                    return true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new TException("Invalid address: " + nodeAddressStr);
            }
        } else {
            throw new TException("confirmTaskReadyToExecute is not supported by " + _schedulingStrategy);
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

            if (!aBoolean) {
                _schedulerServiceMetrics.failedToScheduling();
            }
            long taskEnqueueTime = System.currentTimeMillis() - _taskReceivedTime.get(_taskId);
            if (!SchedulerUtils.isLateBindingScheduler(_schedulingStrategy)) {
                _schedulerServiceMetrics.taskScheduled(taskEnqueueTime);
            } else {
                if (_taskEnqueueTime.containsKey(_taskId)) {
                    _taskEnqueueTime.put(_taskId, Math.max(taskEnqueueTime, _taskEnqueueTime.get(_taskId)));
                } else {
                    _taskEnqueueTime.put(_taskId, taskEnqueueTime);
                }
            }
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:{}", exception.getMessage());
            _schedulerServiceMetrics.failedToScheduling();
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
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
        private final Map<InetSocketAddress, Map.Entry<Long, Integer>> _probeInfo;

        public GetNodeStateWithUpdateCallBack(InetSocketAddress neAddress,
                                              InetSocketAddress nmAddress,
                                              NodeMonitorService.AsyncClient client,
                                              Map<InetSocketAddress, Map.Entry<Long, Integer>> probeInfo) {
            if (client == null) {
                throw new IllegalArgumentException("Client cannot be null");
            }
            if (!neAddress.getAddress().equals(nmAddress.getAddress())){
                throw new IllegalArgumentException("Node monitor address and node enqueue address should have the same IP");
            }
            _client = client;
            _neAddress = neAddress;
            _nmAddress = nmAddress;
            _probeInfo = probeInfo;
        }

        @Override
        public void onComplete(TNodeState nodeState) {
            LOG.info("Node state received from {}", _nmAddress.getHostName());
            _loadMapEqueueSocketToNodeState.put(_neAddress, nodeState);
            synchronized (_probeInfo) {
                _probeInfo.remove(_neAddress);
                _probeInfo.put(_neAddress, new AbstractMap.SimpleEntry<>(System.currentTimeMillis(), 0));
            }
            returnNodeMonitorClient(_neAddress, _client);
        }

        @Override
        public void onError(Exception e) {
            LOG.warn("Failed to get node state from {}", _nmAddress.getHostName());
            returnNodeMonitorClient(_neAddress, _client);
        }
    }

    private class ExecuteTaskCallBack implements AsyncMethodCallback<Long> {

        private final Logger LOG = LoggerFactory.getLogger(ExecuteTaskCallBack.class);
        private final InetSocketAddress _nodeEnqueueAddress;
        private final NodeEnqueueService.AsyncClient _client;
        private final TFullTaskId _taskId;
        private final Set<InetSocketAddress> _otherNodesToCancel;
        private final long _triggerTime;


        public ExecuteTaskCallBack(InetSocketAddress nodeEnqueueAddress, NodeEnqueueService.AsyncClient client,
                                   TFullTaskId taskId,
                                   Set<InetSocketAddress> otherNodesToCancel,
                                   long triggerTime) {
            _nodeEnqueueAddress = nodeEnqueueAddress;
            _client = client;
            _taskId = taskId;
            _otherNodesToCancel = otherNodesToCancel;
            _triggerTime = triggerTime;
        }

        @Override
        public void onComplete(Long waitingTime) {
            LOG.debug("Task executed on node {}", _nodeEnqueueAddress.getHostName());
            long totalSchedulingTime = _taskEnqueueTime.getOrDefault(_taskId.taskId, 0L)
                    + System.currentTimeMillis() - _triggerTime;
            _schedulerServiceMetrics.taskScheduled(totalSchedulingTime);
            for (InetSocketAddress address : _otherNodesToCancel) {
                _schedulerServiceMetrics.infoNodeToCancel();
                try {
                    NodeEnqueueService.AsyncClient clientToCancel = _nodeEnqueueServiceAsyncClientPool.borrowClient(address);
                    clientToCancel.cancelTaskReservation(_taskId, new CancelTaskReservationCallBack(address, clientToCancel));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
        }

        @Override
        public void onError(Exception e) {
            LOG.error("Error executing task on node {}", _nodeEnqueueAddress.getHostName(), e);
            _nodePreservedForTask.put(_taskId.taskId, _otherNodesToCancel);
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
        }
    }

    private class CancelTaskReservationCallBack implements AsyncMethodCallback<Boolean> {

        private final Logger LOG = LoggerFactory.getLogger(CancelTaskReservationCallBack.class);

        private final InetSocketAddress _nodeEnqueueAddress;
        private final NodeEnqueueService.AsyncClient _client;


        public CancelTaskReservationCallBack(InetSocketAddress nodeEnqueueAddress,
                                             NodeEnqueueService.AsyncClient client) {
            _nodeEnqueueAddress = nodeEnqueueAddress;
            _client = client;
        }

        @Override
        public void onComplete(Boolean aBoolean) {
            LOG.debug("Task reservation cancelled on node {}", _nodeEnqueueAddress.getHostName());
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
        }

        @Override
        public void onError(Exception e) {
            LOG.error("Error cancelling task reservation on node {}", _nodeEnqueueAddress.getHostName(), e);
            returnNodeEnqueueClient(_nodeEnqueueAddress, _client);
        }
    }

    private void returnNodeEnqueueClient(InetSocketAddress nodeEnqueueAddress, NodeEnqueueService.AsyncClient client) {
        try {
            _nodeEnqueueServiceAsyncClientPool.returnClient(nodeEnqueueAddress, client);
        } catch (Exception e) {
            LOG.error("Error returning client to node enqueue client pool: {}", e.getMessage());
        }
    }

    private void returnNodeMonitorClient(InetSocketAddress nodeMonitorAddress, NodeMonitorService.AsyncClient client) {
        try {
            _nodeMonitorServiceAsyncClientPool.returnClient(nodeMonitorAddress, client);
        } catch (Exception e) {
            LOG.error("Error returning client to node monitor client pool: {}", e.getMessage());
        }
    }
}
