package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.thavam.util.concurrent.blockingMap.BlockingHashMap;

public class AsyncSparrowTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(AsyncSparrowTaskPlacer.class);
    private final ThriftClientPool<NodeMonitorService.AsyncClient> _nodeMonitorClientPool;
    private final SchedulerServiceMetrics _schedulerMetrics;
    private final boolean _useLoadScores;
    private final Map<TSchedulingRequest, Map.Entry<TNodeState, TNodeState>> _probedNodeStates;
    // has to be a blocking map to ensure that the allocations are available when the scheduler needs them
    private final BlockingHashMap<TSchedulingRequest, Map<TEnqueueTaskReservationRequest, InetSocketAddress>> _schedulingAllocations;
    private final Map<String, InetSocketAddress> _nodeAddressToNeSocket;
    private final static Integer MAX_WAIT_TIME_IN_MS = 20;

    public AsyncSparrowTaskPlacer(double beta,
                                  boolean useLoadScores,
                                  TResourceVector resourceCapacity,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  ThriftClientPool<NodeMonitorService.AsyncClient> nodeMonitorClientPool,
                                  Map<String, InetSocketAddress> nodeAddressToNeSocket) {
        super(beta, useLoadScores, resourceCapacity, 1, 1, 1, 1);
        _schedulerMetrics = schedulerMetrics;
        _nodeMonitorClientPool = nodeMonitorClientPool;
        _useLoadScores = useLoadScores;
        _probedNodeStates = new ConcurrentHashMap<>();
        _schedulingAllocations = new BlockingHashMap<>();
        _nodeAddressToNeSocket = nodeAddressToNeSocket;
    }

    public AsyncSparrowTaskPlacer(double beta,
                                  boolean useLoadScores,
                                  TResourceVector resourceCapacity,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  ThriftClientPool<NodeMonitorService.AsyncClient> nodeMonitorClientPool,
                                  Map<String, InetSocketAddress> nodeAddressToNeSocket,
                                  float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        super(beta, useLoadScores, resourceCapacity, cpuWeight, memWeight, diskWeight, totalDurationWeight);
        _schedulerMetrics = schedulerMetrics;
        _nodeMonitorClientPool = nodeMonitorClientPool;
        _useLoadScores = useLoadScores;
        _probedNodeStates = new ConcurrentHashMap<>();
        _schedulingAllocations = new BlockingHashMap<>();
        _nodeAddressToNeSocket = nodeAddressToNeSocket;
    }


    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        // Randomly select a node to return in case not able to get the results in times.
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> randomAllocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            if (flag < _beta) {
                int secondIndex = ran.nextInt(loadMaps.size());
                try {
                    NodeMonitorService.AsyncClient client1 = _nodeMonitorClientPool.borrowClient(nodeAddresses.get(firstIndex));
                    _schedulerMetrics.probeNode();
                    NodeMonitorService.AsyncClient client2 = _nodeMonitorClientPool.borrowClient(nodeAddresses.get(secondIndex));
                    _schedulerMetrics.probeNode();
                    client1.getNodeState(new GetNodeStateCallBack(
                                    nodeAddresses.get(firstIndex), client1, schedulingRequest, allocations, taskResources, schedulerAddress,
                                    taskSpec));
                    client2.getNodeState(new GetNodeStateCallBack(
                                    nodeAddresses.get(secondIndex), client2, schedulingRequest, allocations, taskResources, schedulerAddress,
                                    taskSpec));
                    updateSchedulingResults(randomAllocations, nodeAddresses.get(firstIndex),
                            schedulingRequest, taskSpec, schedulerAddress, taskResources);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            return _schedulingAllocations.take(schedulingRequest, MAX_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.error("Failed to get scheduling allocations for {}. Use random selected index instead", schedulingRequest);
            return randomAllocations;
        }
    }

    private void allocateBasedOnNodeState(TNodeState nodeState1, TNodeState nodeState2, TResourceVector taskResources,
                   TSchedulingRequest schedulingRequest, TTaskSpec taskSpec, THostPort schedulerAddress,
                   Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations) throws InterruptedException {
        InetSocketAddress selectedNodeAddress = _nodeAddressToNeSocket.get(nodeState1.nodeIp);
        if (!nodeState1.nodeIp.equals(nodeState2.nodeIp)) {
            if (_useLoadScores) {
                Map.Entry<Double, Double> scores = LoadScore.getLoadScoresPairs(nodeState1, nodeState2, taskResources,
                        _cpuWeight, _memWeight, _diskWeight, _totalDurationWeight, _resourceCapacity);
                double loadScore1 = scores.getKey();
                double loadScore2 = scores.getValue();
                if (loadScore1 > loadScore2) {
                    selectedNodeAddress = _nodeAddressToNeSocket.get(nodeState2.nodeIp);
                }
            } else {
                int numPendingTasks1 = nodeState1.numTasks;
                int numPendingTasks2 = nodeState2.numTasks;
                if (numPendingTasks1 > numPendingTasks2) {
                    selectedNodeAddress = _nodeAddressToNeSocket.get(nodeState2.nodeIp);
                }
            }
        }
        updateSchedulingResults(allocations, selectedNodeAddress,
                schedulingRequest, taskSpec, schedulerAddress, taskResources);
        _schedulingAllocations.offer(schedulingRequest, allocations);
    }

    private class GetNodeStateCallBack implements AsyncMethodCallback<edu.cam.dodoor.thrift.TNodeState> {
        private final NodeMonitorService.AsyncClient _client;
        private final InetSocketAddress _address;
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> _allocations;
        TSchedulingRequest _schedulingRequest;
        TResourceVector _taskResources;
        THostPort _schedulerAddress;
        TTaskSpec _taskSpec;

        public GetNodeStateCallBack(InetSocketAddress address, NodeMonitorService.AsyncClient client,
                                    TSchedulingRequest schedulingRequest,
                                    Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations,
                                    TResourceVector taskResources,
                                    THostPort schedulerAddress,
                                    TTaskSpec taskSpec) {
            _client = client;
            _address = address;
            _allocations = allocations;
            _schedulingRequest = schedulingRequest;
            _taskResources = taskResources;
            _schedulerAddress = schedulerAddress;
            _taskSpec = taskSpec;
        }

        @Override
        public void onComplete(TNodeState nodeState) {
            LOG.info("Node state received from {}", _address.getHostName());
            if (_probedNodeStates.containsKey(_schedulingRequest)) {
                Map.Entry<TNodeState, TNodeState> nodeStates = _probedNodeStates.get(_schedulingRequest);
                LOG.info("Node state already probed for {} with {}", _schedulingRequest, nodeStates.getKey().nodeIp);
                if (nodeStates.getValue() == null) {
                    nodeStates.setValue(nodeState);
                }
            } else {
                _probedNodeStates.put(_schedulingRequest, new AbstractMap.SimpleEntry<>(nodeState, null));
            }
            if (_probedNodeStates.containsKey(_schedulingRequest) &&
                    _probedNodeStates.get(_schedulingRequest).getValue() != null) {
                try {
                    allocateBasedOnNodeState(
                            _probedNodeStates.get(_schedulingRequest).getKey(),
                            _probedNodeStates.get(_schedulingRequest).getValue(),
                            _taskResources, _schedulingRequest, _taskSpec, _schedulerAddress, _allocations);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            returnClient();
        }

        @Override
        public void onError(Exception e) {
            LOG.warn("Failed to get node state from {}", _address.getHostName());
            if (_probedNodeStates.containsKey(_schedulingRequest) &&
                    _probedNodeStates.get(_schedulingRequest).getValue() == null) {
                LOG.info("Only one node state received for {} from {}, use it for placement.",
                        _schedulingRequest, _probedNodeStates.get(_schedulingRequest).getKey().nodeIp);
                try {
                    allocateBasedOnNodeState(
                            _probedNodeStates.get(_schedulingRequest).getKey(),
                            _probedNodeStates.get(_schedulingRequest).getKey(),
                            _taskResources, _schedulingRequest, _taskSpec, _schedulerAddress, _allocations);
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                }
            }
            returnClient();
        }

        private void returnClient() {
            try {
                _nodeMonitorClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}