package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;


public class SchedulerThrift implements SchedulerService.Iface{
    Scheduler _scheduler;


    @Override
    public void submitJob(TSchedulingRequest req) throws TException {
        _scheduler.submitJob(req);
    }
    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        _scheduler.updateNodeState(snapshot);
    }

    @Override
    public void registerNode(String nodeAddress) throws TException {
        _scheduler.registerNode(nodeAddress);
    }

    public void initialize(Configuration config, int port) throws TException, IOException {
        _scheduler = new SchedulerImpl();
        SchedulerService.Processor<SchedulerService.Iface> processor =
                new SchedulerService.Processor<>(this);
        int threads = config.getInt(DodoorConf.SCHEDULER_THRIFT_THREADS,
                DodoorConf.DEFAULT_SCHEDULER_THRIFT_THREADS);
        String hostname = Network.getHostName(config);
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
        _scheduler.initialize(config, addr);
        TServers.launchThreadedThriftServer(port, threads, processor);
    }
}
