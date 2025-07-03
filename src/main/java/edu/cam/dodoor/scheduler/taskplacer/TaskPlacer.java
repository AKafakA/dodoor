package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.node.TaskMapsPerNodeType;
import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import edu.cam.dodoor.utils.SchedulerUtils;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public abstract class TaskPlacer {
    double _beta;
    final PackingStrategy _packingStrategy;
    // Map from node to resource capacity
    final Map<String, TResourceVector> _resourceCapacityMap;
    // Map from node to each task's resource vector, durations
    final Map<String, TaskMapsPerNodeType> _taskNodeStateMap;
    final float _cpuWeight;
    final float _memWeight;
    final float _diskWeight;
    final float _totalDurationWeight;

    public TaskPlacer(double beta, PackingStrategy packingStrategy, Map<String, TResourceVector> resourceCapacityMap,
                      float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight,
                      Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        _beta = beta;
        _resourceCapacityMap = resourceCapacityMap;
        _cpuWeight = cpuWeight;
        _memWeight = memWeight;
        _diskWeight = diskWeight;
        _totalDurationWeight = totalDurationWeight;
        _packingStrategy = packingStrategy;
        _taskNodeStateMap = taskNodeStateMap;
    }

    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        return null;
    }

    public static TaskPlacer createTaskPlacer(double beta,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                              SchedulerServiceMetrics schedulerMetrics,
                                              Configuration staticConfig,
                                              Map<String, TResourceVector> resourceCapacityMap,
                                              Map<InetSocketAddress, Map.Entry<Long, Integer>> probeInfo,
                                              Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        String schedulingStrategy = staticConfig.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        float cpuWeight = staticConfig.getFloat(DodoorConf.CPU_WEIGHT, 1);
        float memWeight = staticConfig.getFloat(DodoorConf.MEMORY_WEIGHT, 1);
        float diskWeight = 0;
        if (staticConfig.getBoolean(DodoorConf.REPLAY_WITH_DISK, DodoorConf.DEFAULT_REPLAY_WITH_DISK)) {
            diskWeight = staticConfig.getFloat(DodoorConf.DISK_WEIGHT, 1);
        }
        float totalDurationWeight = staticConfig.getFloat(DodoorConf.TOTAL_PENDING_DURATION_WEIGHT, DodoorConf.DEFAULT_TOTAL_PENDING_DURATION_WEIGHT);
        double rifQuantile = staticConfig.getDouble(DodoorConf.PREQUAL_RIF_QUANTILE, DodoorConf.DEFAULT_PREQUAL_RIF_QUANTILE);
        int probePoolSize = staticConfig.getInt(DodoorConf.PREQUAL_PROBE_POOL_SIZE, DodoorConf.DEFAULT_PREQUAL_PROBE_POOL_SIZE);
        int delta = staticConfig.getInt(DodoorConf.PREQUAL_DELTA, DodoorConf.DEFAULT_PREQUAL_DELTA);
        int probeDelete = staticConfig.getInt(DodoorConf.PREQUAL_PROBE_DELETE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_DELETE_RATE);
        int probeRate = staticConfig.getInt(DodoorConf.PREQUAL_PROBE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_RATE);
        int probeAgeBudget = staticConfig.getInt(DodoorConf.PREQUAL_PROBE_AGE_BUDGET_MS, DodoorConf.DEFAULT_PREQUAL_PROBE_AGE_BUDGET_MS);
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, PackingStrategy.SCORE, resourceCapacityMap,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight, taskNodeStateMap);
            case DodoorConf.POWER_OF_TWO_SCHEDULER
                        -> new RunTimeProbeTaskPlacer(beta, PackingStrategy.RIF, resourceCapacityMap,
                    nodeMonitorClients, schedulerMetrics, taskNodeStateMap);
            case DodoorConf.LOAD_SCORE_POWER_OF_TWO_SCHEDULER -> new RunTimeProbeTaskPlacer(beta,
                    resourceCapacityMap, nodeMonitorClients, schedulerMetrics,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight, taskNodeStateMap);
            case DodoorConf.CACHED_POWER_OF_TWO_SCHEDULER-> new CachedTaskPlacer(beta,
                    PackingStrategy.RIF, resourceCapacityMap,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight, taskNodeStateMap);
            case DodoorConf.RANDOM_SCHEDULER, DodoorConf.SPARROW_SCHEDULER
                    -> new CachedTaskPlacer(-1.0, PackingStrategy.NONE, resourceCapacityMap, taskNodeStateMap);
            case DodoorConf.PREQUAL -> new PrequalTaskPlacer(beta,  resourceCapacityMap, rifQuantile,
                    probeInfo, probePoolSize, delta, probeRate, probeDelete, probeAgeBudget, taskNodeStateMap);
            case DodoorConf.POWER_OF_TWO_DURATION_SCHEDULER -> new RunTimeProbeTaskPlacer(beta,
                    PackingStrategy.DURATION, resourceCapacityMap, nodeMonitorClients, schedulerMetrics, taskNodeStateMap);
            default -> throw new IllegalArgumentException("Unknown scheduling strategy: " + schedulingStrategy);
        };
    }

    static void updateSchedulingResults(Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations,
                                        InetSocketAddress nodeAddress,
                                        TSchedulingRequest schedulingRequest,
                                        TTaskSpec taskSpec,
                                        THostPort schedulerAddress,
                                        TResourceVector taskResources) {
        allocations.put(new TEnqueueTaskReservationRequest(
                schedulingRequest.user,
                taskSpec.taskId,
                schedulerAddress,
                taskResources,
                taskSpec.durationInMs,
                Network.socketAddressToThrift(nodeAddress),
                System.currentTimeMillis(),
                taskSpec.taskType
        ), nodeAddress);
    }
}
