package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class SchedulerThrift implements SchedulerService.Iface, GetTaskService.Iface{
    Scheduler _scheduler;

    public SchedulerThrift(Configuration config) {
        String schedulerType = config.getString(DodoorConf.SCHEDULER_TYPE, "DodoorScheduler");
        if (schedulerType.equals(DodoorConf.DODOOR_SCHEDULER)) {
            _scheduler = new DodoorScheduler();
        } else if (schedulerType.equals(DodoorConf.SPARROW_SCHEDULER)) {
            _scheduler = new SparrowScheduler();
        }
    }

    @Override
    public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress) throws TException {
        return _scheduler.getTask(requestId, nodeMonitorAddress);
    }

    @Override
    public boolean registerFrontend(String app, String socketAddress) throws TException {
        return _scheduler.registerFrontend(app, socketAddress);
    }

    @Override
    public void submitJob(TSchedulingRequest req) throws IncompleteRequestException, TException {
        _scheduler.submitJob(req);
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {
        _scheduler.sendFrontendMessage(app, taskId, status, message);
    }

    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) throws TException {
        _scheduler.updateNodeState(snapshot);
    }

    public void initialize(Configuration config) throws TException, IOException {
        SchedulerService.Processor<SchedulerService.Iface> processor =
                new SchedulerService.Processor<>(this);
        int port = config.getInt(DodoorConf.SCHEDULER_THRIFT_PORT,
                DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT);
        int threads = config.getInt(DodoorConf.NUM_SCHEDULER,
                DodoorConf.DEFAULT_NUM_SCHEDULER);
        String hostname = Network.getHostName(config);
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
        _scheduler.initialize(config, addr);
        TServers.launchThreadedThriftServer(port, threads, processor);
        int getTaskPort = config.getInt(DodoorConf.GET_TASK_PORT,
                DodoorConf.DEFAULT_GET_TASK_PORT);
        GetTaskService.Processor<GetTaskService.Iface> getTaskprocessor =
                new GetTaskService.Processor<>(this);
        TServers.launchSingleThreadThriftServer(getTaskPort, getTaskprocessor);
    }
}
