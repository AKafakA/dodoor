package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;

import java.net.InetSocketAddress;
import java.util.*;

public class DummyTaskPlacer extends TaskPlacer {
    public DummyTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                           float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        super(beta, useLoadScores, resourceCapacity, cpuWeight, memWeight, diskWeight, totalDurationWeight);
    }

    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        InetSocketAddress selectedNode = (InetSocketAddress) loadMaps.keySet().toArray()[0];
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            updateSchedulingResults(allocations, selectedNode, schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }
}
