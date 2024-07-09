package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Logging;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public interface Scheduler {

    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException;
    public boolean registerFrontend(String appId, String addr);
    public void submitJob(TSchedulingRequest request) throws TException;
    public void handleJobSubmission(TSchedulingRequest request) throws TException;
    public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress);
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message);
    public void updateNodeState(Map<String, TNodeState> snapshot);

}
