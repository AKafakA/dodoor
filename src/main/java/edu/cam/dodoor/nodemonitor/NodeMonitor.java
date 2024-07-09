package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.TCancelTaskReservationsRequest;
import edu.cam.dodoor.thrift.TEnqueueTaskReservationsRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

public interface NodeMonitor {

    public void initialize(Configuration config, int internalPort);

    public void taskFinished(List<TFullTaskId> tasks);


    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request)
            throws TException;

    public void cancelTaskReservations(TCancelTaskReservationsRequest request)
            throws TException;
}
