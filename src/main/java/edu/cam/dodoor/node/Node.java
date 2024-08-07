package edu.cam.dodoor.node;

import edu.cam.dodoor.thrift.TEnqueueTaskReservationRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

public interface Node {

    void initialize(Configuration config, NodeThrift nodeMonitorClient);

    void taskFinished(TFullTaskId task) throws TException;

    boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException;

    TResourceVector getRequestedResourceVector();

    int getNumTasks();
}
