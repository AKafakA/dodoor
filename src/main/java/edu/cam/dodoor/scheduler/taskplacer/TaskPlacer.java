package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;

public abstract class TaskPlacer {
    double _beta;
    final boolean _useLoadScores;
    final TResourceVector _resourceCapacity;
    final float _cpuWeight;
    final float _memWeight;
    final float _diskWeight;
    final float _totalDurationWeight;

    public TaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                      float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        _beta = beta;
        _useLoadScores = useLoadScores;
        _resourceCapacity = resourceCapacity;
        _cpuWeight = cpuWeight;
        _memWeight = memWeight;
        _diskWeight = diskWeight;
        _totalDurationWeight = totalDurationWeight;
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
                                              Queue<InetSocketAddress> prequalQueue,
                                              Map<InetSocketAddress, Integer> probeReuseCount) {
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
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, true, resourceCapacity,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.SPARROW_SCHEDULER -> new SparrowTaskPlacer(beta, false, resourceCapacity,
                    nodeMonitorClients, schedulerMetrics);
            case DodoorConf.LOAD_SCORE_SPARROW -> new SparrowTaskPlacer(beta, true, resourceCapacity,
                    nodeMonitorClients, schedulerMetrics, cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.CACHED_SPARROW_SCHEDULER -> new CachedTaskPlacer(beta, false, resourceCapacity,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.RANDOM_SCHEDULER -> new CachedTaskPlacer(-1.0, false, resourceCapacity);
            case DodoorConf.ASYNC_SPARROW_SCHEDULER -> new AsyncSparrowTaskPlacer(beta, false, resourceCapacity,
                     schedulerMetrics, asyncNodeMonitorClientPool, nodeAddressToNeSocket, neSocketToNmSocket);
            case DodoorConf.ASYNC_LOAD_SCORE_SPARROW -> new AsyncSparrowTaskPlacer(beta, true, resourceCapacity,
                    schedulerMetrics, asyncNodeMonitorClientPool, nodeAddressToNeSocket, neSocketToNmSocket,
                    cpuWeight, memWeight, diskWeight, totalDurationWeight);
            case DodoorConf.PREQUAL -> new PrequalTaskPlacer(beta, true, resourceCapacity, rifQuantile,
                    prequalQueue, probeReuseCount);
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
                Network.socketAddressToThrift(nodeAddress)
        ), nodeAddress);
    }
}
