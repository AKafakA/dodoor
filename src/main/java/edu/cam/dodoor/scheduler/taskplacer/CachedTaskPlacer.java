package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class CachedTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(CachedTaskPlacer.class);

    public CachedTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity) {
        super(beta, useLoadScores, resourceCapacity, 1, 1, 1);
    }

    
    public CachedTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                            float cpuWeight, float memWeight, float diskWeight) {
        super(beta, useLoadScores, resourceCapacity, cpuWeight, memWeight, diskWeight);
    }

    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            if (flag < _beta) {
                int secondIndex = ran.nextInt(loadMaps.size());
                double score1, score2;
                if (_useLoadScores) {
                    score1 = LoadScore.getLoadScores(loadMaps.get(nodeAddresses.get(firstIndex)).resourceRequested,
                            taskResources, _cpuWeight, _memWeight, _diskWeight, _resourceCapacity);
                    score2 = LoadScore.getLoadScores(loadMaps.get(nodeAddresses.get(secondIndex)).resourceRequested,
                            taskResources, _cpuWeight, _memWeight, _diskWeight, _resourceCapacity);
                } else {
                    score1 = loadMaps.get(nodeAddresses.get(firstIndex)).numTasks;
                    score2 = loadMaps.get(nodeAddresses.get(secondIndex)).numTasks;
                }
                LOG.debug("node {} with score {}, node {} with score {} ",
                        new Object[]{nodeAddresses.get(firstIndex).getHostName(), score1,
                                nodeAddresses.get(secondIndex).getHostName(), score2});
                if (score1 > score2) {
                    firstIndex = secondIndex;
                }
                LOG.debug("node {} is selected", nodeAddresses.get(firstIndex).getHostName());
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }
}