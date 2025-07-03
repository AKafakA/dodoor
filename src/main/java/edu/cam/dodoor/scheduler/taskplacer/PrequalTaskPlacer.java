package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.node.TaskMapsPerNodeType;
import edu.cam.dodoor.node.TaskTypeID;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.MetricsUtils;
import edu.cam.dodoor.utils.SchedulerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class PrequalTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(PrequalTaskPlacer.class);
    private final double _rifQuantile;
    private final Map<InetSocketAddress, Map.Entry<Long, Integer>> _probeInfo;
    private final int _probePoolSize;
    private final int _delta;
    private final int _probeRate;
    private final int _probeDeleteRate;
    private final long _probeAgeBudget;

    public PrequalTaskPlacer(double beta,
                             Map<String, TResourceVector> resourceCapacityMap,
                             double rifQuantile,
                             Map<InetSocketAddress, Map.Entry<Long, Integer>> probeInfo,
                             int probePoolSize,
                             int delta,
                             int probeRate,
                             int probeDeleteRate,
                             long probeAgeBudget,
                             Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        super(beta, PackingStrategy.NONE, resourceCapacityMap, 1, 1, 1, 1,
                taskNodeStateMap);
        _rifQuantile = rifQuantile;
        _probePoolSize = probePoolSize;
        _delta = delta;
        _probeRate = probeRate;
        _probeDeleteRate = probeDeleteRate;
        _probeInfo = probeInfo;
        _probeAgeBudget = probeAgeBudget;
    }


    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        int[] numPendingTasks = loadMaps.values().stream().mapToInt(e -> e.numTasks).toArray();
        int cutoff = MetricsUtils.getQuantile(numPendingTasks, _rifQuantile);
        // TODO(wda): Always send the tasks inside one request to the same node, which can be improved in the future.
        Map.Entry<InetSocketAddress, TNodeState> selectedResults = selectLeastNodeFromPrequalPool(loadMaps, cutoff);
        InetSocketAddress selectedNode = selectedResults.getKey();
        TNodeState selectedNodeState = selectedResults.getValue();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources;
            if (taskSpec.taskType.equals(TaskTypeID.SIMULATED.toString())) {
                // Simulated tasks are not placed, so we skip them.
                taskResources = taskSpec.resourceRequest;
            } else {
                taskResources = _taskNodeStateMap.get(selectedNodeState.nodeType).getResourceVector(taskSpec.taskType);
                taskSpec.resourceRequest = taskResources;
                taskSpec.durationInMs = _taskNodeStateMap.get(selectedNodeState.nodeType).getTaskDuration(taskSpec.taskType);
            }
            updateSchedulingResults(allocations, selectedNode, schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }


    private Map.Entry<InetSocketAddress, TNodeState> selectLeastNodeFromPrequalPool(Map<InetSocketAddress, TNodeState> loadMaps,
                                                                                    int taskCountCutoff) {
        Map<InetSocketAddress, TNodeState> prequalLoadMaps = new HashMap<>();
        InetSocketAddress selectedAddress;
        TNodeState selectedNodeState;
        synchronized (_probeInfo) {
            List<InetSocketAddress> probeAddresses = new ArrayList<>(_probeInfo.keySet());
            Collections.reverse(probeAddresses);
            int probeReuseBudget = SchedulerUtils.getProbeReuseBudget(loadMaps.size(), _probePoolSize, _probeRate,
                    _probeDeleteRate, _delta);
            for (int i = 0; i < Math.min(probeAddresses.size(), _probePoolSize); i++) {
                InetSocketAddress probeAddress = probeAddresses.get(i);
                long probedTime = _probeInfo.get(probeAddress).getKey();
                int probedUsedCount = _probeInfo.get(probeAddress).getValue();
                if (probedUsedCount < probeReuseBudget && (System.currentTimeMillis() - probedTime) < _probeAgeBudget) {
                    prequalLoadMaps.put(probeAddresses.get(i), loadMaps.get(probeAddresses.get(i)));
                    _probeInfo.get(probeAddresses.get(i)).setValue(_probeInfo.get(probeAddresses.get(i)).getValue() + 1);
                }
            }
        }

        if (prequalLoadMaps.isEmpty() || prequalLoadMaps.size() <= 2) {
            LOG.debug("Prequal queue is empty or too small, selecting random node");
            Random random = new Random();
            selectedAddress = (InetSocketAddress) loadMaps.keySet().toArray()[random.nextInt(loadMaps.size())];
        } else {
            Optional<InetSocketAddress> selectedNodeOptional = prequalLoadMaps.entrySet().stream().filter(e -> e.getValue().numTasks < taskCountCutoff)
                    .sorted(Comparator.comparingLong(e -> e.getValue().totalDurations)).map(Map.Entry::getKey).findFirst();
            selectedAddress  =
                    selectedNodeOptional.orElseGet(() -> prequalLoadMaps.entrySet().stream().min(Comparator.comparingInt(e -> e.getValue().numTasks)).get().getKey());
        }
        selectedNodeState = loadMaps.get(selectedAddress);
        return new AbstractMap.SimpleEntry<>(selectedAddress, selectedNodeState);
    }
}
