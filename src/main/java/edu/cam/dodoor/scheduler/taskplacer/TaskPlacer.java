package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Serialization;

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

    public static TaskPlacer createTaskPlacer(double beta, String schedulingStrategy,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, true);
            case DodoorConf.SPARROW_SCHEDULER -> new SparrowTaskPlacer(beta, nodeMonitorClients);
            case DodoorConf.CACHED_SPARROW_SCHEDULER -> new CachedTaskPlacer(beta, false);
            case DodoorConf.RANDOM_SCHEDULER -> new CachedTaskPlacer(-1.0, false);
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
                Serialization.getStrFromSocket(nodeAddress)
        ), nodeAddress);
    }
}
