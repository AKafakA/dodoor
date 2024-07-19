package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class TaskPlacer {
    double _beta;

    public TaskPlacer(double beta) {
        _beta = beta;
    }

    public Map<InetSocketAddress, TEnqueueTaskReservationRequest> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        return null;
    }

    public static TaskPlacer createTaskPlacer(double beta, String schedulingStrategy,
                                              Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        if (schedulingStrategy.equals(DodoorConf.DODOOR_SCHEDULER)) {
            return new CachedTaskPlacer(beta, true);
        } else if (schedulingStrategy.equals(DodoorConf.SPARROW_SCHEDULER)) {
            return new SparrowTaskPlacer(beta, nodeMonitorClients);
        } else if (schedulingStrategy.equals(DodoorConf.CACHED_SPARROW_SCHEDULER)) {
            return new CachedTaskPlacer(beta, false);
        } else {
            throw new IllegalArgumentException("Unknown scheduling strategy: " + schedulingStrategy);
        }
    }
}
