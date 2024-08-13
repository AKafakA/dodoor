package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import org.apache.logging.log4j.LogBuilder;

import java.net.InetSocketAddress;
import java.util.*;

public class CachedTaskPlacer extends TaskPlacer {
    private final boolean _useLoadScores;
    
    public CachedTaskPlacer(double beta, int numProbe,
                            boolean useLoadScores) {
        super(beta, numProbe);
        _useLoadScores = useLoadScores;
    }

    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequestsToSingleAddress(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            double miniScore = getLoad(loadMaps, nodeAddresses.get(firstIndex), taskResources);
            if (flag < _beta) {
                for (int i = 1; i < _numProbe; i++) {
                    int secondIndex = ran.nextInt(loadMaps.size());
                    double newScore = getLoad(loadMaps, nodeAddresses.get(secondIndex), taskResources);
                    if (miniScore > newScore) {
                        firstIndex = secondIndex;
                        miniScore = newScore;
                    }
                }
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }

    private double getLoadScores(TResourceVector requestedResources, TResourceVector taskResources) {
        return (double) requestedResources.cores * taskResources.cores + requestedResources.memory * taskResources.memory +
                requestedResources.disks * taskResources.disks;
    }

    private double getLoad(Map<InetSocketAddress, TNodeState> loadMaps, InetSocketAddress firstAddress,
                           TResourceVector taskResources) {
        if (_useLoadScores) {
            return getLoadScores(loadMaps.get(firstAddress).resourceRequested, taskResources);
        } else {
            return loadMaps.get(firstAddress).numTasks;
        }
    }
}
