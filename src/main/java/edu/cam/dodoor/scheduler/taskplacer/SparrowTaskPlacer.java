package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.scheduler.SchedulerServiceMetrics;
import edu.cam.dodoor.thrift.*;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.util.*;

public class SparrowTaskPlacer extends TaskPlacer{
    Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;
    SchedulerServiceMetrics _schedulerMetrics;
    boolean _useLoadScores;

    public SparrowTaskPlacer(double beta,
                             boolean useLoadScores,
                             TResourceVector resourceCapacity,
                             Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                             SchedulerServiceMetrics schedulerMetrics) {
        super(beta, useLoadScores, resourceCapacity, 1, 1, 1);
        _schedulerMetrics = schedulerMetrics;
        _nodeMonitorClients = nodeMonitorClients;
        _useLoadScores = useLoadScores;
    }

    public SparrowTaskPlacer(double beta,
                             boolean useLoadScores,
                             TResourceVector resourceCapacity,
                             Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients,
                             SchedulerServiceMetrics schedulerMetrics,
                             float cpuWeight, float memWeight, float diskWeight) {
        super(beta, useLoadScores, resourceCapacity, cpuWeight, memWeight, diskWeight);
        _schedulerMetrics = schedulerMetrics;
        _nodeMonitorClients = nodeMonitorClients;
        _useLoadScores = useLoadScores;
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
                try {
                    TNodeState nodeState1 = _nodeMonitorClients.get(nodeAddresses.get(firstIndex)).getNodeState();
                    _schedulerMetrics.probeNode();
                    TNodeState nodeState2 = _nodeMonitorClients.get(nodeAddresses.get(secondIndex)).getNodeState();
                    _schedulerMetrics.probeNode();
                    if (_useLoadScores) {
                        double loadScore1 = LoadScore.getLoadScores(nodeState1.resourceRequested, taskResources,
                                _cpuWeight, _memWeight, _diskWeight, _resourceCapacity);
                        double loadScore2 = LoadScore.getLoadScores(nodeState2.resourceRequested, taskResources,
                                _cpuWeight, _memWeight, _diskWeight, _resourceCapacity);
                        if (loadScore1 > loadScore2) {
                            firstIndex = secondIndex;
                        }
                    } else {
                        int numPendingTasks1 = nodeState1.numTasks;
                        int numPendingTasks2 = nodeState2.numTasks;
                        if (numPendingTasks1 > numPendingTasks2) {
                            firstIndex = secondIndex;
                        }
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
