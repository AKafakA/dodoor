package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import edu.cam.dodoor.utils.SchedulerUtils;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class TaskPlacer {
    double _beta;
    final PackingStrategy _packingStrategy;
    final TResourceVector _resourceCapacity;
    final float _cpuWeight;
    final float _memWeight;
    final float _diskWeight;
    final float _totalDurationWeight;

    public TaskPlacer(double beta, PackingStrategy packingStrategy, TResourceVector resourceCapacity,
                      float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        _beta = beta;
        _resourceCapacity = resourceCapacity;
        _cpuWeight = cpuWeight;
        _memWeight = memWeight;
        _diskWeight = diskWeight;
        _totalDurationWeight = totalDurationWeight;
        _packingStrategy = packingStrategy;
    }

    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        return null;
    }

    public static TaskPlacer createTaskPlacer(double beta,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                              SchedulerServiceMetrics schedulerMetrics,
                                              Configuration configuration,
                                              ThriftClientPool<NodeMonitorService.AsyncClient> asyncNodeMonitorClientPool,
                                              Map<String, InetSocketAddress> nodeAddressToNeSocket,
                                              Map<InetSocketAddress, InetSocketAddress> neSocketToNmSocket,
                                              Map<InetSocketAddress, Map.Entry<Long, Integer>> probeInfo) {
        TResourceVector resourceCapacity = Resources.getSystemResourceVector(configuration);
        String schedulingStrategy = configuration.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        float cpuWeight = configuration.getFloat(DodoorConf.CPU_WEIGHT, 1);
        float memWeight = configuration.getFloat(DodoorConf.MEMORY_WEIGHT, 1);
        float diskWeight = 0;
        if (configuration.getBoolean(DodoorConf.REPLAY_WITH_DISK, DodoorConf.DEFAULT_REPLAY_WITH_DISK)) {
            diskWeight = configuration.getFloat(DodoorConf.DISK_WEIGHT, 1);
        }
        float totalDurationWeight = configuration.getFloat(DodoorConf.TOTAL_PENDING_DURATION_WEIGHT, DodoorConf.DEFAULT_TOTAL_PENDING_DURATION_WEIGHT);
        double rifQuantile = configuration.getDouble(DodoorConf.PREQUAL_RIF_QUANTILE, DodoorConf.DEFAULT_PREQUAL_RIF_QUANTILE);
        int probePoolSize = configuration.getInt(DodoorConf.PREQUAL_PROBE_POOL_SIZE, DodoorConf.DEFAULT_PREQUAL_PROBE_POOL_SIZE);
        int delta = configuration.getInt(DodoorConf.PREQUAL_DELTA, DodoorConf.DEFAULT_PREQUAL_DELTA);
        int probeDelete = configuration.getInt(DodoorConf.PREQUAL_PROBE_DELETE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_DELETE_RATE);
        int probeRate = configuration.getInt(DodoorConf.PREQUAL_PROBE_RATE, DodoorConf.DEFAULT_PREQUAL_PROBE_RATE);
        int probeAgeBudget = configuration.getInt(DodoorConf.PREQUAL_PROBE_AGE_BUDGET_MS, DodoorConf.DEFAULT_PREQUAL_PROBE_AGE_BUDGET_MS);
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, PackingStrategy.SCORE, resourceCapacity,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.POWER_OF_TWO_SCHEDULER
                        -> new RunTimeProbeTaskPlacer(beta, PackingStrategy.RIF, resourceCapacity,
                    nodeMonitorClients, schedulerMetrics);
            case DodoorConf.LOAD_SCORE_POWER_OF_TWO_SCHEDULER -> new RunTimeProbeTaskPlacer(beta,
                    resourceCapacity, nodeMonitorClients, schedulerMetrics,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.CACHED_POWER_OF_TWO_SCHEDULER-> new CachedTaskPlacer(beta,
                    PackingStrategy.RIF, resourceCapacity,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.RANDOM_SCHEDULER, DodoorConf.SPARROW_SCHEDULER
                    -> new CachedTaskPlacer(-1.0, PackingStrategy.NONE, resourceCapacity);
            case DodoorConf.PREQUAL -> new PrequalTaskPlacer(beta,  resourceCapacity, rifQuantile,
                    probeInfo, probePoolSize, delta, probeRate, probeDelete, probeAgeBudget);
            case DodoorConf.POWER_OF_TWO_DURATION_SCHEDULER -> new RunTimeProbeTaskPlacer(beta,
                    PackingStrategy.DURATION, resourceCapacity, nodeMonitorClients, schedulerMetrics);
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
                System.currentTimeMillis()
        ), nodeAddress);
    }
}
