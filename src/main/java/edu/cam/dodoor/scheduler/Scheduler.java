package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.node.TaskSpec;
import edu.cam.dodoor.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public interface Scheduler {

    void initialize(Configuration staticConf, THostPort schedulerAddress,
                    SchedulerServiceMetrics schedulerServiceMetrics,
                    JSONObject hostConfig,
                    JSONObject taskTypeConfig) throws IOException;
    void submitJob(TSchedulingRequest request) throws TException;
    Map<InetSocketAddress, List<TEnqueueTaskReservationRequest>> handleJobSubmission(TSchedulingRequest request,
                                                                                     long startTime) throws TException;
    void updateNodeState(Map<String, TNodeState> snapshot);
    void registerNode(String nodeAddress, String nodeType) throws TException;
    void registerDataStore(String dataStoreAddress) throws TException;
    void taskFinished(TFullTaskId taskId, long nodeWallTime) throws TException;
    boolean confirmTaskReadyToExecute(TFullTaskId taskId, String nodeAddressStr) throws TException;
}
