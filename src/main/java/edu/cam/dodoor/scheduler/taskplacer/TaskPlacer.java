package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Resources;
import edu.cam.dodoor.utils.Serialization;
import org.apache.commons.configuration.Configuration;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class TaskPlacer {
    double _beta;

    public TaskPlacer(double beta) {
        _beta = beta;
    }

    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        return null;
    }

    public static TaskPlacer createTaskPlacer(double beta,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                              SchedulerServiceMetrics schedulerMetrics,
                                              Configuration configuration) {
        TResourceVector resourceCapacity = Resources.getSystemResourceVector(configuration);
        String schedulingStrategy = configuration.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        float cpuWeight = configuration.getFloat(DodoorConf.CPU_WEIGHT, 1);
        float memWeight = configuration.getFloat(DodoorConf.MEMORY_WEIGHT, 1);
        float diskWeight = 0;
        if (configuration.getBoolean(DodoorConf.REPLAY_WITH_DISK, DodoorConf.DEFAULT_REPLAY_WITH_DISK)) {
            diskWeight = configuration.getFloat(DodoorConf.DISK_WEIGHT, 1);
        }
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, true, resourceCapacity, false,
                    cpuWeight, memWeight, diskWeight);
            case DodoorConf.SPARROW_SCHEDULER -> new SparrowTaskPlacer(beta, nodeMonitorClients, schedulerMetrics);
            case DodoorConf.CACHED_SPARROW_SCHEDULER -> new CachedTaskPlacer(beta, false, resourceCapacity, false,
                    cpuWeight, memWeight, diskWeight);
            case DodoorConf.RANDOM_SCHEDULER -> new CachedTaskPlacer(-1.0, false, resourceCapacity, false);
            case DodoorConf.REVERSE_DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, true, resourceCapacity, true,
                    cpuWeight, memWeight, diskWeight);
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
