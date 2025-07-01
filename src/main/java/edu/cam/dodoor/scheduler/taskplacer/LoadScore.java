package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.thrift.TResourceVector;
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
                                                               TResourceVector taskResources,
                                                               double cpuWeight,
                                                               double memWeight,
                                                               double diskWeight,
                                                               double totalDurationWeight,
                                                               Map<String, TResourceVector> resourceCapacityMap) {
      if (totalDurationWeight < 0 || totalDurationWeight > 1) {
        throw new IllegalArgumentException("totalDurationWeight must be between 0 and 1");
      }

      double firstResourceLoad = getResourceLoadScores(firstNodeState.resourceRequested,
              taskResources, cpuWeight, memWeight, diskWeight, resourceCapacityMap.get(firstNodeState.nodeType));
      double secondResourceLoad = getResourceLoadScores(secondNodeState.resourceRequested,
              taskResources, cpuWeight, memWeight, diskWeight, resourceCapacityMap.get(firstNodeState.nodeType));

      double firstNormalizedResourceLoad = firstResourceLoad / (firstResourceLoad + secondResourceLoad);
      double secondNormalizedResourceLoad = secondResourceLoad / (firstResourceLoad + secondResourceLoad);

      double firstTotalDuration = firstNodeState.totalDurations;
      double secondTotalDuration = secondNodeState.totalDurations;

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
