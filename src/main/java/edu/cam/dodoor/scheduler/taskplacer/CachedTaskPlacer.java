package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;

import java.net.InetSocketAddress;
import java.util.*;

public class CachedTaskPlacer extends TaskPlacer{
    private boolean _useLoadScores;
    
    public CachedTaskPlacer(double beta, boolean useLoadScores) {
        super(beta);
        _useLoadScores = useLoadScores;
    }

    @Override
    public Map<InetSocketAddress, TEnqueueTaskReservationRequest> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<InetSocketAddress, TEnqueueTaskReservationRequest> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> internalAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            if (flag < _beta) {
                int secondIndex = ran.nextInt(loadMaps.size());
                double score1, score2;
                if (_useLoadScores) {
                    score1 = getLoadScores(loadMaps.get(internalAddresses.get(firstIndex)).resourceRequested,
                            taskResources);
                    score2 = getLoadScores(loadMaps.get(internalAddresses.get(secondIndex)).resourceRequested,
                            taskResources);
                } else {
                    score1 = loadMaps.get(internalAddresses.get(firstIndex)).numTasks;
                    score2 = loadMaps.get(internalAddresses.get(secondIndex)).numTasks;
                }
                if (score1 > score2) {
                    firstIndex = secondIndex;
                }
            }
            allocations.put(internalAddresses.get(firstIndex), new TEnqueueTaskReservationRequest(
                    schedulingRequest.user,
                    taskSpec.taskId,
                    schedulerAddress,
                    taskResources,
                    taskSpec.durationInMs
            ));
        }
        return allocations;
    }

    private double getLoadScores(TResourceVector requestedResources, TResourceVector taskResources) {
        return (double) requestedResources.cores * taskResources.cores + requestedResources.memory * taskResources.memory +
                requestedResources.disks * taskResources.disks;
    }
}
