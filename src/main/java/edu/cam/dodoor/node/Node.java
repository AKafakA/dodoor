package edu.cam.dodoor.node;

import edu.cam.dodoor.thrift.TEnqueueTaskReservationRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.json.JSONObject;

public interface Node {

    void initialize(Configuration config, NodeThrift nodeMonitorClient, JSONObject nodeConfig,
                    JSONObject taskConfig);

    void taskFinished(TFullTaskId task) throws TException;

    boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException;

    boolean cancelTaskReservation(TFullTaskId taskId) throws TException;

    TResourceVector getRequestedResourceVector();

    TNodeState getNodeState();
}
