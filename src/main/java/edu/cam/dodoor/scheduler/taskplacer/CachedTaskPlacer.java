package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.node.TaskMapsPerNodeType;
import edu.cam.dodoor.node.TaskTypeID;
import edu.cam.dodoor.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class CachedTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(CachedTaskPlacer.class);

    public CachedTaskPlacer(double beta, PackingStrategy packingStrategy,
                            Map<String, TResourceVector> resourceCapacityMap,
                            Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        this(beta, packingStrategy, resourceCapacityMap, 1, 1, 1, 1,
                taskNodeStateMap);
        if (packingStrategy == PackingStrategy.SCORE) {
            throw new IllegalArgumentException("Packing strategy should not be SCORE without resource weights");
        }
    }

    
    public CachedTaskPlacer(double beta, PackingStrategy packingStrategy,
                            Map<String, TResourceVector> resourceCapacityMap,
                            float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight,
                            Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        super(beta, packingStrategy, resourceCapacityMap, cpuWeight, memWeight, diskWeight, totalDurationWeight,
                taskNodeStateMap);
    }

    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequests(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector rawTaskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            if (_beta > 0 && flag < _beta) {
                int secondIndex = ran.nextInt(loadMaps.size());
                double score1, score2;
                if (_packingStrategy == PackingStrategy.SCORE) {
                    Map.Entry<Double, Double> scores = LoadScore.getLoadScoresPairs(loadMaps.get(nodeAddresses.get(firstIndex)),
                            loadMaps.get(nodeAddresses.get(secondIndex)), taskSpec.taskType,
                            rawTaskResources, _cpuWeight, _memWeight, _diskWeight,
                            _totalDurationWeight, _resourceCapacityMap, _taskNodeStateMap);
                    score1 = scores.getKey();
                    score2 = scores.getValue();
                } else if (_packingStrategy == PackingStrategy.RIF) {
                    score1 = loadMaps.get(nodeAddresses.get(firstIndex)).numTasks;
                    score2 = loadMaps.get(nodeAddresses.get(secondIndex)).numTasks;
                } else if (_packingStrategy == PackingStrategy.DURATION) {
                    score1 = loadMaps.get(nodeAddresses.get(firstIndex)).totalDurations;
                    score2 = loadMaps.get(nodeAddresses.get(secondIndex)).totalDurations;
                } else {
                    throw new IllegalArgumentException("Unknown packing strategy");
                }
                LOG.debug("node {} with score {}, node {} with score {} ",
                        new Object[]{nodeAddresses.get(firstIndex).getHostName(), score1,
                                nodeAddresses.get(secondIndex).getHostName(), score2});
                if (score1 > score2) {
                    firstIndex = secondIndex;
                }
                LOG.debug("node {} is selected", nodeAddresses.get(firstIndex).getHostName());
            }
            TResourceVector placedResources;
            if (taskSpec.taskType.equals(TaskTypeID.SIMULATED.toString())) {
                placedResources = rawTaskResources;
            } else {
                String selectedNodeTypeId = loadMaps.get(nodeAddresses.get(firstIndex)).nodeType;
                TaskMapsPerNodeType taskMapsPerNodeType = _taskNodeStateMap.get(selectedNodeTypeId);
                placedResources = taskMapsPerNodeType.getResourceVector(taskSpec.taskType);
                taskSpec.resourceRequest = placedResources;
                taskSpec.durationInMs = _taskNodeStateMap.get(loadMaps.get(nodeAddresses.get(firstIndex)).nodeType)
                        .getTaskDuration(taskSpec.taskType);
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, placedResources);
        }
        return allocations;
    }
}