package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.TCancelTaskReservationsRequest;
import edu.cam.dodoor.thrift.TEnqueueTaskReservationsRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.util.List;

public interface NodeMonitor {

    void initialize(Configuration config, int internalPort);

    void taskFinished(TFullTaskId task);

    boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException;

    TResourceVector getRequestedResourceVector();

    int getNumTasks() throws TException;
}
