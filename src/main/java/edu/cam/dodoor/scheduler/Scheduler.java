package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface Scheduler {

    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException;
    public void submitJob(TSchedulingRequest request) throws TException;
    public void handleJobSubmission(TSchedulingRequest request) throws TException;
    public void updateNodeState(Map<String, TNodeState> snapshot);
}
