package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.TResourceVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadScore {

    public static final Logger LOG = LoggerFactory.getLogger(LoadScore.class);

    public LoadScore() {
    }

    public static double getLoadScores(TResourceVector requestedResources, TResourceVector taskResources,
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
        LOG.debug("cpuLoad: {}, memLoad: {}, diskLoad: {}, requested cpu: {}, task cpu: {}, cpu capacity: {}" +
                "requested mem: {}, task mem: {}, mem capacity: {} ", new Object[]{cpuLoad, memLoad, diskLoad,
                requestedResources.cores, taskResources.cores, resourceCapacity.cores,
                requestedResources.memory, taskResources.memory, resourceCapacity.memory});
        return cpuLoad + memLoad + diskLoad;
    }
}
