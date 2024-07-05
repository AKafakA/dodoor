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

    public void assignTask(TFullTaskId taskId, int status, boolean message);

    public void sendFrontendMessage(String app, TFullTaskId taskId,
                                    int status, ByteBuffer message) throws TException;

    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request)
            throws TException;

    public void cancelTaskReservations(TCancelTaskReservationsRequest request)
            throws TException;

    public boolean registerBackend(String appId, InetSocketAddress nmAddr,
                                   InetSocketAddress backendAddr);
}
