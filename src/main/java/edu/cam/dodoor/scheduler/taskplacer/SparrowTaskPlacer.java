package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.util.*;

public class SparrowTaskPlacer extends TaskPlacer{
    Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;
    public SparrowTaskPlacer(double beta,
                             int numProbe,
                             Map<InetSocketAddress, NodeMonitorService.Client> nodeMonitorClients) {
        super(beta, numProbe);
        _nodeMonitorClients = nodeMonitorClients;
    }

    @Override
    public Map<TEnqueueTaskReservationRequest, InetSocketAddress> getEnqueueTaskReservationRequestsToSingleAddress(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, InetSocketAddress> allocations = new HashMap<>();
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            TResourceVector taskResources = taskSpec.resourceRequest;
            List<InetSocketAddress> nodeAddresses = new ArrayList<>(loadMaps.keySet());
            Random ran = new Random();
            double flag = ran.nextFloat();
            int firstIndex = ran.nextInt(loadMaps.size());
            int miniPendingTasks;
            if (flag < _beta) {
                try {
                    miniPendingTasks = _nodeMonitorClients.get(nodeAddresses.get(firstIndex)).getNumTasks();
                    for (int i = 1; i < _numProbe; i++) {
                        int secondIndex = ran.nextInt(loadMaps.size());
                        int newNumPendingTasks = _nodeMonitorClients.get(nodeAddresses.get(secondIndex)).getNumTasks();
                        if (miniPendingTasks > newNumPendingTasks) {
                            firstIndex = secondIndex;
                            miniPendingTasks = newNumPendingTasks;
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
