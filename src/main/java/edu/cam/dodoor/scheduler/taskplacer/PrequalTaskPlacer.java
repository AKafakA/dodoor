package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.MetricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class PrequalTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(PrequalTaskPlacer.class);
    private final double _rifQuantile;
    private final Queue<InetSocketAddress> _prequalQueue;
    private final Map<InetSocketAddress, Integer> _probeReuseCount;

    public PrequalTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                             double rifQuantile, Queue<InetSocketAddress> prequalQueue,
                             Map<InetSocketAddress, Integer> probeReuseCount) {
        super(beta, useLoadScores, resourceCapacity, 1, 1, 1, 1);
        _rifQuantile = rifQuantile;
        _prequalQueue = prequalQueue;
        _probeReuseCount = probeReuseCount;
    }


    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        int[] numPendingTasks = loadMaps.values().stream().mapToInt(e -> e.numTasks).toArray();
        int cutoff = MetricsUtils.getQuantile(numPendingTasks, _rifQuantile);
        // TODO(wda): Always send the tasks inside one request to the same node, which can be improved in the future.
        InetSocketAddress selectedNode = selectLeastNodeFromPrequalPool(loadMaps, cutoff);
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            updateSchedulingResults(allocations, selectedNode, schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }


    private InetSocketAddress selectLeastNodeFromPrequalPool(Map<InetSocketAddress, TNodeState> loadMaps,
                                                             int taskCountCutoff) {
        Map<InetSocketAddress, TNodeState> prequalLoadMaps = new HashMap<>();
        synchronized (_prequalQueue) {
            synchronized (_probeReuseCount) {
                if (_prequalQueue.isEmpty() || _prequalQueue.size() <= 2) {
                    LOG.debug("Prequal queue is empty or too small, selecting random node");
                    Random random = new Random();
                    return (InetSocketAddress) loadMaps.keySet().toArray()[random.nextInt(loadMaps.size())];
                }
                for (InetSocketAddress nodeAddress: _prequalQueue) {
                    prequalLoadMaps.put(nodeAddress, loadMaps.get(nodeAddress));
                    if (!_probeReuseCount.containsKey(nodeAddress)) {
                        throw new RuntimeException("Node address not found in probe reuse count map");
                    }
                    _probeReuseCount.put(nodeAddress, _probeReuseCount.get(nodeAddress) + 1);
                }
            }
        }
        Optional<InetSocketAddress> selectedNodeOptional = prequalLoadMaps.entrySet().stream().filter(e -> e.getValue().numTasks < taskCountCutoff)
                .sorted(Comparator.comparingLong(e -> e.getValue().totalDurations)).map(Map.Entry::getKey).findFirst();
        return selectedNodeOptional.orElseGet(() -> prequalLoadMaps.entrySet().stream().min(Comparator.comparingInt(e -> e.getValue().numTasks)).get().getKey());
    }
}
