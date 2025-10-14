package org.anon.scheduler.scheduler.taskplacer;

import org.anon.scheduler.node.TaskMapsPerNodeType;
import org.anon.scheduler.node.TaskTypeID;
import org.anon.scheduler.thrift.TNodeState;
import org.anon.scheduler.thrift.TResourceVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LoadScore {

    public static final Logger LOG = LoggerFactory.getLogger(LoadScore.class);

    public LoadScore() {
    }

    public static double getResourceLoadScores(TResourceVector requestedResources, TResourceVector taskResources,
                                       double cpuWeight, double memWeight, double diskWeight,
                                       TResourceVector resourceCapacity) {
        double cpuLoad = cpuWeight * (requestedResources.cores * taskResources.cores) /
                (resourceCapacity.cores * resourceCapacity.cores) ;
        double memLoad = memWeight * ((double) (requestedResources.memory) / (resourceCapacity.memory)) *
                ((double) taskResources.memory / resourceCapacity.memory);
        double diskLoad = 0.0;
        if (resourceCapacity.disks > 0) {
            diskLoad = diskWeight * ((double) (requestedResources.disks) / (resourceCapacity.disks)) *
                    ((double) taskResources.disks / resourceCapacity.disks);
        }
        double normalizedResourceLoad = (cpuLoad + memLoad + diskLoad) / (cpuWeight + memWeight + diskWeight);
        LOG.debug("cpuLoad: {}, memLoad: {}, diskLoad: {}, requested cpu: {}, task cpu: {}, cpu capacity: {} " +
                "requested mem: {}, task mem: {}, mem capacity: {}, final resourceScore: {} ", new Object[]{cpuLoad, memLoad, diskLoad,
                requestedResources.cores, taskResources.cores, resourceCapacity.cores,
                requestedResources.memory, taskResources.memory, resourceCapacity.memory, normalizedResourceLoad});
        return normalizedResourceLoad;
    }

    public static Map.Entry<Double, Double> getLoadScoresPairs(TNodeState firstNodeState,
                                                               TNodeState secondNodeState,
                                                               String taskTypeId,
                                                               TResourceVector taskResources,
                                                               double cpuWeight,
                                                               double memWeight,
                                                               double diskWeight,
                                                               double totalDurationWeight,
                                                               Map<String, TResourceVector> resourceCapacityMap,
                                                               Map<String, TaskMapsPerNodeType> taskNodeStateMap,
                                                               String taskMode,
                                                               long taskEstimatedDuration) {
      if (totalDurationWeight < 0 || totalDurationWeight > 1) {
        throw new IllegalArgumentException("totalDurationWeight must be between 0 and 1");
      }
      TResourceVector firstResourceVector = taskResources;
      TResourceVector secondResourceVector = taskResources;
      if (!taskTypeId.equals(TaskTypeID.SIMULATED.toString())) {
          firstResourceVector = taskNodeStateMap.get(firstNodeState.nodeType).getResourceVector(taskTypeId, taskMode);
          secondResourceVector = taskNodeStateMap.get(secondNodeState.nodeType).getResourceVector(taskTypeId, taskMode);
      }
      double firstResourceLoad = getResourceLoadScores(firstNodeState.resourceRequested,
              firstResourceVector, cpuWeight, memWeight, diskWeight, resourceCapacityMap.get(firstNodeState.nodeType));
      double secondResourceLoad = getResourceLoadScores(secondNodeState.resourceRequested,
              secondResourceVector, cpuWeight, memWeight, diskWeight, resourceCapacityMap.get(secondNodeState.nodeType));

      double firstNormalizedResourceLoad = firstResourceLoad / (firstResourceLoad + secondResourceLoad);
      double secondNormalizedResourceLoad = secondResourceLoad / (firstResourceLoad + secondResourceLoad);

      double firstTotalDuration = firstNodeState.totalDurations;
      double secondTotalDuration = secondNodeState.totalDurations;

      if (taskTypeId.equals(TaskTypeID.SIMULATED.toString()) && taskEstimatedDuration > 0) {
          firstTotalDuration += taskEstimatedDuration;
          secondTotalDuration += taskEstimatedDuration;
      } else if (!taskTypeId.equals(TaskTypeID.SIMULATED.toString())) {
          long firstTaskDuration = taskNodeStateMap.get(firstNodeState.nodeType).getTaskDuration(taskTypeId,
                  taskMode);
          long secondTaskDuration = taskNodeStateMap.get(secondNodeState.nodeType).getTaskDuration(taskTypeId,
                  taskMode);
          firstTotalDuration += firstTaskDuration;
          secondTotalDuration += secondTaskDuration;
      } else {
          throw new IllegalArgumentException("For non-simulated tasks, estimated duration must be provided");
      }

      double firstNormalizedTotalDuration = firstTotalDuration / (firstTotalDuration + secondTotalDuration);
      double secondNormalizedTotalDuration = secondTotalDuration / (firstTotalDuration + secondTotalDuration);

      double firstLoadScore = firstNormalizedResourceLoad * (1 - totalDurationWeight) + firstNormalizedTotalDuration * totalDurationWeight;
      double secondLoadScore = secondNormalizedResourceLoad * (1 - totalDurationWeight) + secondNormalizedTotalDuration * totalDurationWeight;

      LOG.debug("firstResourceLoad: {}, firstTotalPendingDuration: {}, firstLoadScore: {}, "
                      + "secondResourceLoad: {}, secondPendingTotalDuration: {}, secondLoadScore: {}",
                new Object[]{firstResourceLoad, firstTotalDuration, firstLoadScore, secondResourceLoad, secondTotalDuration, secondLoadScore});
      return Map.entry(firstLoadScore, secondLoadScore);
    }
}
