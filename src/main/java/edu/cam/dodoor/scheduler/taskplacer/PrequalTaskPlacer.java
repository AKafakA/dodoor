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
    private final Map<InetSocketAddress, Integer> _probeReuseCount;
    private final int _probePoolSize;
    private final int _probeReuseBudget;

    public PrequalTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                             double rifQuantile,
                             Map<InetSocketAddress, Integer> probeReuseCount,
                             int probePoolSize,
                             int probeReuseBudget) {
        super(beta, useLoadScores, resourceCapacity, 1, 1, 1, 1);
        _rifQuantile = rifQuantile;
        _probeReuseCount = probeReuseCount;
        _probePoolSize = probePoolSize;
        _probeReuseBudget = probeReuseBudget;
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

        Set<InetSocketAddress> addressToRemove = new HashSet<>();
        List<InetSocketAddress> probeAddresses = new ArrayList<>(_probeReuseCount.keySet());
        Collections.reverse(probeAddresses);
        for (int i = 0; i < probeAddresses.size(); i++) {
            if (i < _probePoolSize) {
                prequalLoadMaps.put(probeAddresses.get(i), loadMaps.get(probeAddresses.get(i)));
                _probeReuseCount.put(probeAddresses.get(i), _probeReuseCount.get(probeAddresses.get(i)) + 1);
                if ( _probeReuseCount.get(probeAddresses.get(i)) > _probeReuseBudget) {
                    addressToRemove.add(probeAddresses.get(i));
                }
            } else {
                addressToRemove.add(probeAddresses.get(i));
            }
        }

        if (prequalLoadMaps.isEmpty() || prequalLoadMaps.size() <= 2) {
            LOG.debug("Prequal queue is empty or too small, selecting random node");
            Random random = new Random();
            return (InetSocketAddress) loadMaps.keySet().toArray()[random.nextInt(loadMaps.size())];
        }

        Optional<InetSocketAddress> selectedNodeOptional = prequalLoadMaps.entrySet().stream().filter(e -> e.getValue().numTasks < taskCountCutoff)
                .sorted(Comparator.comparingLong(e -> e.getValue().totalDurations)).map(Map.Entry::getKey).findFirst();
        InetSocketAddress selectedAddress = selectedNodeOptional.orElseGet(() -> prequalLoadMaps.entrySet().stream().min(Comparator.comparingInt(e -> e.getValue().numTasks)).get().getKey());

        if (!addressToRemove.isEmpty()) {
            for (InetSocketAddress address: addressToRemove) {
                _probeReuseCount.remove(address);
            }
        }
        return selectedAddress;
    }
}
