package edu.cam.dodoor.nodemonitor;

import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class NodeMonitorImpl implements NodeMonitor{


    private final static Logger LOG = Logger.getLogger(NodeMonitorImpl.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

    private final HashMap<String, InetSocketAddress>  _appSockets =
            new HashMap<String, InetSocketAddress>();

    private final HashMap<String, List<TFullTaskId>> _appTasks =
            new HashMap<String, List<TFullTaskId>>();

    // Map to scheduler socket address for each request id.
    private final ConcurrentMap<String, InetSocketAddress> _requestSchedulers =
            Maps.newConcurrentMap();
    private final ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool =
            new ThriftClientPool<SchedulerService.AsyncClient>(
                    new ThriftClientPool.SchedulerServiceMakerFactory());
    private TaskScheduler _taskScheduler;
    private TaskLauncherService _taskLauncherService;
    private String _ipAddress;
    private int _internalPort;

    @Override
    public void initialize(Configuration config, int internalPort) {
        _ipAddress = Network.getIPAddress(config);
        _internalPort = internalPort;
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);

        // TODO(wda): add more task scheduler
        _taskScheduler = new FifoTaskScheduler(numSlots);

        _taskScheduler.initialize(config, internalPort);
        _taskLauncherService = new TaskLauncherService();
        _taskLauncherService.initialize(config, _taskScheduler, internalPort);
    }

    @Override
    public void taskFinished(List<TFullTaskId> tasks) {

    }

    @Override
    public void assignTask(TFullTaskId taskId, int status, boolean message) {

    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {

    }

    @Override
    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException {
        return false;
    }

    @Override
    public void cancelTaskReservations(TCancelTaskReservationsRequest request) throws TException {

    }

    @Override
    public boolean registerBackend(String appId, InetSocketAddress nmAddr, InetSocketAddress backendAddr) {
        return false;
    }
}
