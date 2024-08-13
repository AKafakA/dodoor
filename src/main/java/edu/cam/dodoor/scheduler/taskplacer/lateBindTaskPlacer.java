package edu.cam.dodoor.scheduler.taskplacer;

import edu.cam.dodoor.thrift.*;

import java.net.InetSocketAddress;
import java.util.*;

public class lateBindTaskPlacer extends TaskPlacer {
    public lateBindTaskPlacer(double beta, int numProbe) {
        super(beta, numProbe);
    }

    public void getEnqueueTaskReservationRequestsToSingleAddress() {
        throw new UnsupportedOperationException("Not implemented");
    }

    // Randomly select a set of nodes to place the task reservations
    @Override
    public Map<TEnqueueTaskReservationRequest, Set<InetSocketAddress>> getEnqueueTaskReservationRequestsToSet(
            TSchedulingRequest schedulingRequest,
            Map<InetSocketAddress, TNodeState> loadMaps, THostPort schedulerAddress) {
        Map<TEnqueueTaskReservationRequest, Set<InetSocketAddress>> allocations = new HashMap<>();
        Set<InetSocketAddress> nodeAddresses = loadMaps.keySet();
        List<InetSocketAddress> nodelist = new LinkedList<>(nodeAddresses);
        for (TTaskSpec taskSpec : schedulingRequest.tasks) {
            Collections.shuffle(nodelist);
            Set<InetSocketAddress> randomSet = new HashSet<>(nodelist.subList(0, _numProbe));
            allocations.put(new TEnqueueTaskReservationRequest(
                    schedulingRequest.user,
                    taskSpec.taskId,
                    schedulerAddress,
                    taskSpec.resourceRequest,
                    taskSpec.durationInMs,
                    NO_PLACED
            ), randomSet);
        }
        return allocations;
    }
}
