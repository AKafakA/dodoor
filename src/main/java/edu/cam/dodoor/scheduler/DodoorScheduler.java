package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class DodoorScheduler implements Scheduler{
    @Override
    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
    }

    @Override
    public boolean registerFrontend(String appId, String addr) {
        return false;
    }

    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
    }

    @Override
    public void handleJobSubmission(TSchedulingRequest request) throws TException {
    }

    @Override
    public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress) {
        return List.of();
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) {
    }

    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
    }
}
