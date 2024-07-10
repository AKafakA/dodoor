package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class TaskPlacer {
    long _requestId;
    double _beta;

    public TaskPlacer(long requestId, double beta) {
        _requestId = requestId;
        _beta = beta;
    }

    public Map<InetSocketAddress, TEnqueueTaskReservationsRequest> getEnqueueTaskReservationsRequests(
            TSchedulingRequest schedulingRequest, long requestId,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        return null;
    }

    public static TaskPlacer createTaskPlacer(long requestId, double beta, String schedulingStrategy,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        if (schedulingStrategy.equals(DodoorConf.DODOOR_SCHEDULER)) {
            return new CachedTaskPlacer(requestId, beta, true);
        } else if (schedulingStrategy.equals(DodoorConf.SPARROW_SCHEDULER)) {
            return new SparrowTaskPlacer(requestId, beta, nodeMonitorClients);
        } else if (schedulingStrategy.equals(DodoorConf.CACHED_SPARROW_SCHEDULER)) {
            return new CachedTaskPlacer(requestId, beta, false);
        } else {
            throw new IllegalArgumentException("Unknown scheduling strategy: " + schedulingStrategy);
        }
    }
}
