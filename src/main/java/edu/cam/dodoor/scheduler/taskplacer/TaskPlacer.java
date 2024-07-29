package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.node.TaskSpec;
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
