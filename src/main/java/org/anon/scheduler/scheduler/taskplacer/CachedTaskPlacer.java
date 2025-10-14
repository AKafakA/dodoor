package org.anon.scheduler.scheduler.taskplacer;

import org.anon.scheduler.node.TaskMapsPerNodeType;
import org.anon.scheduler.node.TaskTypeID;
import org.anon.scheduler.thrift.*;
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
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress, int round) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector rawTaskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            // Use task ID hash combined with round number for reproducible but varied placement across rounds
            Random ran = new Random(taskSpec.taskId.hashCode() + round);
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            if (_beta > 0 && flag < _beta) {
                // Dodoor scheduler: optimize with cached load scoring
                // Optimize Dodoor power-of-two selection
                int secondIndex;
                do {
                    secondIndex = ran.nextInt(loadMaps.size());
                } while (secondIndex == firstIndex && loadMaps.size() > 1);

                InetSocketAddress node1 = nodeAddresses.get(firstIndex);
                InetSocketAddress node2 = nodeAddresses.get(secondIndex);
                TNodeState state1 = loadMaps.get(node1);
                TNodeState state2 = loadMaps.get(node2);

                double score1, score2;
                if (_packingStrategy == PackingStrategy.SCORE) {
                    Map.Entry<Double, Double> scores = LoadScore.getLoadScoresPairs(state1, state2, 
                            taskSpec.taskType, rawTaskResources, _cpuWeight, _memWeight, _diskWeight,
                            _totalDurationWeight, _resourceCapacityMap, _taskNodeStateMap, taskSpec.taskMode,
                            taskSpec.durationInMs);
                    score1 = scores.getKey();
                    score2 = scores.getValue();
                } else if (_packingStrategy == PackingStrategy.RIF) {
                    score1 = state1.numTasks;
                    score2 = state2.numTasks;
                } else if (_packingStrategy == PackingStrategy.DURATION) {
                    score1 = state1.totalDurations;
                    score2 = state2.totalDurations;
                } else if (_packingStrategy == PackingStrategy.NONE) {
                    score1 = 0;
                    score2 = 0;
                } else {
                    throw new IllegalArgumentException("Unknown packing strategy");
                }
                
                if (score1 > score2) {
                    firstIndex = secondIndex;
                }
            }
            TResourceVector placedResources;
            if (taskSpec.taskType.equals(TaskTypeID.SIMULATED.toString())) {
                placedResources = rawTaskResources;
            } else {
                String selectedNodeTypeId = loadMaps.get(nodeAddresses.get(firstIndex)).nodeType;
                TaskMapsPerNodeType taskMapsPerNodeType = _taskNodeStateMap.get(selectedNodeTypeId);
                placedResources = taskMapsPerNodeType.getResourceVector(taskSpec.taskType, taskSpec.taskMode);
                taskSpec.resourceRequest = placedResources;
                taskSpec.durationInMs = _taskNodeStateMap.get(loadMaps.get(nodeAddresses.get(firstIndex)).nodeType)
                        .getTaskDuration(taskSpec.taskType, taskSpec.taskMode);
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, placedResources);
        }
        return allocations;
    }
}