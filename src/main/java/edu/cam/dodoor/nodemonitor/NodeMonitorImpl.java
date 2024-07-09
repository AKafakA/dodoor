package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;

public class NodeMonitorImpl implements NodeMonitor{
    private final static Logger LOG = Logger.getLogger(NodeMonitorImpl.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

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


    @Override
    public void cancelTaskReservations(TCancelTaskReservationsRequest request) throws TException {
        _taskScheduler.cancelTaskReservations(request.requestId);
    }

    @Override
    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation",
                _ipAddress, request.taskId));
        LOG.info("Received enqueue task reservation request from " + _ipAddress + " for request " +
                request.taskId);

        _taskScheduler.submitTaskReservations(request);
        return true;
    }
}
