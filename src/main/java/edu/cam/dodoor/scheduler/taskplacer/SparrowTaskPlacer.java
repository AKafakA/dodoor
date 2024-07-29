package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.util.*;

public class SparrowTaskPlacer extends TaskPlacer{
    Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;
    public SparrowTaskPlacer(double beta,
                             Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        super(beta);
        _nodeMonitorClients = nodeMonitorClients;
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
                    int numPendingTasks1 = _nodeMonitorClients.get(nodeAddresses.get(firstIndex)).getNumTasks();
                    int numPendingTasks2 = _nodeMonitorClients.get(nodeAddresses.get(secondIndex)).getNumTasks();
                    if (numPendingTasks1 > numPendingTasks2) {
                        firstIndex = secondIndex;
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
