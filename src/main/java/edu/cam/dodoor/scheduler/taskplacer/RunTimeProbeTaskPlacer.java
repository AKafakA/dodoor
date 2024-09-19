package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.util.*;


public class RunTimeProbeTaskPlacer extends TaskPlacer{
    Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;
    SchedulerServiceMetrics _schedulerMetrics;
    PackingStrategy _packingStrategy;

    public RunTimeProbeTaskPlacer(double beta,
                                  PackingStrategy packingStrategy,
                                  TResourceVector resourceCapacity,
                                  Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                  SchedulerServiceMetrics schedulerMetrics) {
        this(beta, packingStrategy, resourceCapacity, nodeMonitorClients, schedulerMetrics,
                1, 1, 1, 1);
        if (packingStrategy == PackingStrategy.SCORE) {
            throw new IllegalArgumentException("Packing strategy should not be SCORE without resource weights");
        }
    }

    public RunTimeProbeTaskPlacer(double beta,
                             TResourceVector resourceCapacity,
                             Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                             SchedulerServiceMetrics schedulerMetrics,
                             float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        this(beta, PackingStrategy.SCORE, resourceCapacity, nodeMonitorClients, schedulerMetrics,
                cpuWeight, memWeight, diskWeight, totalDurationWeight);
    }

    public RunTimeProbeTaskPlacer(double beta,
                                  PackingStrategy packingStrategy,
                                  TResourceVector resourceCapacity,
                                  Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight) {
        super(beta, packingStrategy, resourceCapacity, cpuWeight, memWeight, diskWeight, totalDurationWeight);
        _schedulerMetrics = schedulerMetrics;
        _nodeMonitorClients = nodeMonitorClients;
        _packingStrategy = packingStrategy;
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
                NodeMonitorService.Client nodeMonitorClient1 = _nodeMonitorClients.get(nodeAddresses.get(firstIndex));
                NodeMonitorService.Client nodeMonitorClient2 = _nodeMonitorClients.get(nodeAddresses.get(secondIndex));
                TNodeState nodeState1;
                TNodeState nodeState2;
                try {
                    synchronized (nodeMonitorClient1) {
                        synchronized (nodeMonitorClient2) {
                            nodeState1 = nodeMonitorClient1.getNodeState();
                            _schedulerMetrics.probeNode();
                            nodeState2 = nodeMonitorClient2.getNodeState();
                            _schedulerMetrics.probeNode();
                        }
                    }
                    if (_packingStrategy == PackingStrategy.SCORE) {
                        Map.Entry<Double, Double> scores = LoadScore.getLoadScoresPairs(nodeState1, nodeState2, taskResources,
                                _cpuWeight, _memWeight, _diskWeight, _totalDurationWeight, _resourceCapacity);
                        double loadScore1 = scores.getKey();
                        double loadScore2 = scores.getValue();
                        if (loadScore1 > loadScore2) {
                            firstIndex = secondIndex;
                        }
                    } else if (_packingStrategy == PackingStrategy.RIF) {
                        int numPendingTasks1 = nodeState1.numTasks;
                        int numPendingTasks2 = nodeState2.numTasks;
                        if (numPendingTasks1 > numPendingTasks2) {
                            firstIndex = secondIndex;
                        }
                    } else if (_packingStrategy == PackingStrategy.DURATION) {
                        long totalPendingDuration1 = nodeState1.totalDurations;
                        long totalPendingDuration2 = nodeState2.totalDurations;
                        if (totalPendingDuration1 > totalPendingDuration2) {
                            firstIndex = secondIndex;
                        }
                    } else {
                        throw new IllegalArgumentException("Unknown packing strategy: " + _packingStrategy);
                    }
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, taskResources);
        }
        return allocations;
    }
}
