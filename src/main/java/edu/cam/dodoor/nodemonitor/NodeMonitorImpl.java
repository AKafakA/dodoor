package edu.cam.dodoor.nodemonitor;

import com.google.common.collect.Maps;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Logging;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

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
    private TaskScheduler scheduler;
    private TaskLauncherService taskLauncherService;
    private String ipAddress;

    @Override
    public void initialize(Configuration config, int internalPort) {

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
