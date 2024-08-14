package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.node.TaskSpec;
import edu.cam.dodoor.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public interface Scheduler {

    void initialize(Configuration conf, InetSocketAddress socket,
                    SchedulerServiceMetrics schedulerServiceMetrics) throws IOException;
    void submitJob(TSchedulingRequest request) throws TException;
    Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> handleJobSubmission(TSchedulingRequest request) throws TException;
    void updateNodeState(Map<String, TNodeState> snapshot);
    void registerNode(String nodeAddress) throws TException;
    void registerDataStore(String dataStoreAddress) throws TException;
    void taskFinished(TFullTaskId taskId) throws TException;
}
