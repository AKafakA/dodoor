package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class SparrowScheduler implements Scheduler{
    @Override
    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {

    }


    @Override
    public void submitJob(TSchedulingRequest request) throws TException {

    }

    @Override
    public void handleJobSubmission(TSchedulingRequest request) throws TException {

    }

    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {

    }
}
