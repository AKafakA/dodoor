package edu.cam.dodoor.nodemonitor;

import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

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

    // Map to scheduler socket address for each request id.
    private final ConcurrentMap<String, InetSocketAddress> _requestSchedulers =
            Maps.newConcurrentMap();
    private final ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.SchedulerServiceMakerFactory());
    private TaskScheduler _taskScheduler;
    private String _ipAddress;

    @Override
    public void initialize(Configuration config, int internalPort) {
        _ipAddress = Network.getIPAddress(config);
        int numSlots = config.getInt(DodoorConf.NUM_SLOTS, DodoorConf.DEFAULT_NUM_SLOTS);
        // TODO(wda): add more task scheduler
        _taskScheduler = new FifoTaskScheduler(numSlots);
        _taskScheduler.initialize(config, internalPort);
        TaskLauncherService taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(config, _taskScheduler, internalPort);
    }

    @Override
    public void taskFinished(List<TFullTaskId> tasks) {
        _taskScheduler.tasksFinished(tasks);
    }

    private class SendFrontendMessageCallback implements
            AsyncMethodCallback<Void> {
        private final InetSocketAddress _frontendSocket;
        private final SchedulerService.AsyncClient _client;
        public SendFrontendMessageCallback(InetSocketAddress socket, SchedulerService.AsyncClient client) {
            _frontendSocket = socket;
            _client = client;
        }

        @Override
        public void onComplete(Void unused) {
            try { _schedulerClientPool.returnClient(_frontendSocket, _client); }
            catch (Exception e) { LOG.error(e); }
        }

        public void onError(Exception exception) {
            try { _schedulerClientPool.returnClient(_frontendSocket, _client); }
            catch (Exception e) { LOG.error(e); }
            LOG.error(exception);
        }
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {
        LOG.debug(Logging.functionCall(app, taskId, message));
        InetSocketAddress schedulerAddress = _requestSchedulers.get(taskId.requestId);
        if (schedulerAddress == null) {
            LOG.error("Did not find any scheduler info for request: " + taskId);
            return;
        }

        try {
            SchedulerService.AsyncClient client = _schedulerClientPool.borrowClient(schedulerAddress);
            client.sendFrontendMessage(app, taskId, status, message,
                    new SendFrontendMessageCallback(schedulerAddress, client));
            LOG.debug("finished sending message");
        } catch (Exception e) {
            LOG.error(e);
        };
    }

    @Override
    public void cancelTaskReservations(TCancelTaskReservationsRequest request) throws TException {
        _taskScheduler.cancelTaskReservations(request.requestId);
    }

    @Override
    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation",
                _ipAddress, request.requestId));
        LOG.info("Received enqueue task reservation request from " + _ipAddress + " for request " +
                request.requestId);

        InetSocketAddress schedulerAddress = new InetSocketAddress(
                request.getSchedulerAddress().getHost(), request.getSchedulerAddress().getPort());
        _requestSchedulers.put(request.getRequestId(), schedulerAddress);

        InetSocketAddress socket = _appSockets.get(request.getAppId());
        if (socket == null) {
            LOG.error("No socket stored for " + request.getAppId() + " (never registered?). " +
                    "Can't launch task.");
            return false;
        }
        _taskScheduler.submitTaskReservations(request, socket);
        return true;
    }

    @Override
    public boolean registerBackend(String appId, InetSocketAddress nmAddr, InetSocketAddress backendAddr) {
        LOG.debug(Logging.functionCall(appId, nmAddr, backendAddr));
        if (_appSockets.containsKey(appId)) {
            LOG.warn("Attempt to re-register app " + appId);
            return false;
        }
        _appSockets.put(appId, backendAddr);
        return true;
    }
}
