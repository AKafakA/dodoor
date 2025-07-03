package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.node.TaskMapsPerNodeType;
import edu.cam.dodoor.node.TaskTypeID;
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
                                  Map<String, TResourceVector> resourceCapacityMap,
                                  Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        this(beta, packingStrategy, resourceCapacityMap, nodeMonitorClients, schedulerMetrics,
                1, 1, 1, 1, taskNodeStateMap);
        if (packingStrategy == PackingStrategy.SCORE) {
            throw new IllegalArgumentException("Packing strategy should not be SCORE without resource weights");
        }
    }

    public RunTimeProbeTaskPlacer(double beta,
                                  Map<String, TResourceVector> resourceCapacityMap,
                                  Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight,
                                  Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        this(beta, PackingStrategy.SCORE, resourceCapacityMap, nodeMonitorClients, schedulerMetrics,
                cpuWeight, memWeight, diskWeight, totalDurationWeight, taskNodeStateMap);
    }

    public RunTimeProbeTaskPlacer(double beta,
                                  PackingStrategy packingStrategy,
                                  Map<String, TResourceVector> resourceCapacityMap,
                                  Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                                  SchedulerServiceMetrics schedulerMetrics,
                                  float cpuWeight, float memWeight, float diskWeight, float totalDurationWeight,
                                  Map<String, TaskMapsPerNodeType> taskNodeStateMap) {
        super(beta, packingStrategy, resourceCapacityMap, cpuWeight, memWeight, diskWeight, totalDurationWeight,
                taskNodeStateMap);
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
            TResourceVector rawTaskResources = taskSpec.resourceRequest;
            TResourceVector placedTaskResources;
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
                        Map.Entry<Double, Double> scores = LoadScore.getLoadScoresPairs(nodeState1, nodeState2,
                                taskSpec.taskType,
                                rawTaskResources,
                                _cpuWeight, _memWeight, _diskWeight, _totalDurationWeight,
                                _resourceCapacityMap,
                                _taskNodeStateMap);
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
            if (taskSpec.taskType.equals(TaskTypeID.SIMULATED.toString())) {
                placedTaskResources = rawTaskResources;
            } else {
                TNodeState nodeState1 = loadMaps.get(nodeAddresses.get(firstIndex));
                placedTaskResources = _taskNodeStateMap.get(nodeState1.nodeType).getResourceVector(taskSpec.taskType);
                taskSpec.resourceRequest = placedTaskResources;
                taskSpec.durationInMs = _taskNodeStateMap.get(nodeState1.nodeType).getTaskDuration(taskSpec.taskType);
            }

            updateSchedulingResults(allocations, nodeAddresses.get(firstIndex),
                    schedulingRequest, taskSpec, schedulerAddress, placedTaskResources);
        }
        return allocations;
    }
}
