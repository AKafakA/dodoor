package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class CachedTaskPlacer extends TaskPlacer{
    public static final Logger LOG = LoggerFactory.getLogger(CachedTaskPlacer.class);

    private final boolean _useLoadScores;

    private final TResourceVector _resourceCapacity;

    private final boolean _reverse;

    private final float _cpuWeight;

    private final float _memWeight;

    private final float _diskWeight;

    public CachedTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                            boolean reverse) {
        super(beta);
        _useLoadScores = useLoadScores;
        _resourceCapacity = resourceCapacity;
        _reverse = reverse;
        _cpuWeight = 1;
        _memWeight = 1;
        _diskWeight = 1;
    }

    
    public CachedTaskPlacer(double beta, boolean useLoadScores, TResourceVector resourceCapacity,
                            boolean reverse, float cpuWeight, float memWeight, float diskWeight) {
        super(beta);
        _useLoadScores = useLoadScores;
        _resourceCapacity = resourceCapacity;
        _reverse = reverse;
        _cpuWeight = cpuWeight;
        _memWeight = memWeight;
        _diskWeight = diskWeight;
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
                    score1 = getLoadScores(loadMaps.get(nodeAddresses.get(firstIndex)).resourceRequested,
                            taskResources);
                    score2 = getLoadScores(loadMaps.get(nodeAddresses.get(secondIndex)).resourceRequested,
                            taskResources);
                } else {
                    score1 = loadMaps.get(nodeAddresses.get(firstIndex)).numTasks;
                    score2 = loadMaps.get(nodeAddresses.get(secondIndex)).numTasks;
                }
                LOG.debug("node {} with score {}, node {} with score {} ",
                        new Object[]{nodeAddresses.get(firstIndex).getHostName(), score1,
                                nodeAddresses.get(secondIndex).getHostName(), score2});
                if (score1 > score2 && !_reverse) {
                    firstIndex = secondIndex;
                } else if (score1 < score2 && _reverse) {
                    firstIndex = secondIndex;
                }
                LOG.debug("node {} is selected", nodeAddresses.get(firstIndex).getHostName());
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }

    private double getLoadScores(TResourceVector requestedResources, TResourceVector taskResources) {
        double cpuLoad = _cpuWeight * (requestedResources.cores * taskResources.cores) /
                (_resourceCapacity.cores * _resourceCapacity.cores) ;
        double memLoad = _memWeight * ((double) (requestedResources.memory) / (_resourceCapacity.memory)) *
                ((double) taskResources.memory / _resourceCapacity.memory);
        double diskLoad = 0.0;
        if (_resourceCapacity.disks > 0) {
            diskLoad = _diskWeight * ((double) (requestedResources.disks) / (_resourceCapacity.disks)) *
                    ((double) taskResources.disks / _resourceCapacity.disks);
        }
        LOG.debug("cpuLoad: {}, memLoad: {}, diskLoad: {}, requested cpu: {}, task cpu: {}, cpu capacity: {}" +
                "requested mem: {}, task mem: {}, mem capacity: {} ", new Object[]{cpuLoad, memLoad, diskLoad,
                requestedResources.cores, taskResources.cores, _resourceCapacity.cores,
                requestedResources.memory, taskResources.memory, _resourceCapacity.memory});
        return cpuLoad + memLoad + diskLoad;
    }
}