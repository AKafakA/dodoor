package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Serialization;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class TaskPlacer {
    double _beta;
    int _numProbe;
    public static final String NO_PLACED = "NO_PLACED";

    public TaskPlacer(double beta, int numProbe) {
        _beta = beta;
        _numProbe = numProbe;
    }

    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequestsToSingleAddress(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Map<TEnqueueTaskReservationRequest, Set<InetSocketAddress>> getEnqueueTaskReservationRequestsToSet(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = getEnqueueTaskReservationRequestsToSingleAddress(
                schedulingRequest, loadMaps, schedulerAddress);
        Map<TEnqueueTaskReservationRequest, Set<InetSocketAddress>> result = new HashMap<>();
        for (TEnqueueTaskReservationRequest request : allocations.keySet()) {
            result.put(request, Set.of(allocations.get(request)));
        }
        return result;
    }

    public static TaskPlacer createTaskPlacer(double beta,
                                              int numProbe,
                                              String schedulingStrategy,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        return switch (schedulingStrategy) {
            case DodoorConf.DODOOR_SCHEDULER -> new CachedTaskPlacer(beta, numProbe,true);
            case DodoorConf.SPARROW_SCHEDULER -> new SparrowTaskPlacer(beta, numProbe, nodeMonitorClients);
            case DodoorConf.CACHED_SPARROW_SCHEDULER -> new CachedTaskPlacer(beta,numProbe, false);
            case DodoorConf.RANDOM_SCHEDULER -> new CachedTaskPlacer(-1.0, numProbe, false);
            case DodoorConf.LATE_BINDING_SPARROW -> new CachedTaskPlacer(-1.0, numProbe,true);
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
