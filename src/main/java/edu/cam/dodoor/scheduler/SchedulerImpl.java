package edu.cam.dodoor.scheduler;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerImpl implements Scheduler{

    private final static Logger LOG = Logger.getLogger(SchedulerImpl.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(SchedulerImpl.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private final AtomicInteger _counter = new AtomicInteger(0);


    /** Thrift client pool for async communicating with node monitors */
    private final ThriftClientPool<InternalService.AsyncClient> _nodeMonitorAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.InternalServiceMakerFactory());
    
    private Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;

    private ConcurrentMap<InetSocketAddress, TNodeState> _loadMaps;
    private THostPort _address;
    private String _schedulingStrategy;
    private double _beta;
    private int _batchSize;
    private TaskPlacer _taskPlacer;

    @Override
    public void initialize(Configuration config, InetSocketAddress socket) throws IOException {
        _address = Network.socketAddressToThrift(socket);
        _loadMaps = Maps.newConcurrentMap();
        _schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        _beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
        _batchSize = config.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        List<String> nodeMonitorAddresses = ConfigUtil.parseNodeAddress(config, DodoorConf.STATIC_NODE_MONITORS);
        _nodeMonitorClients = Maps.newHashMap();
        for (String nodeMonitorAddress : nodeMonitorAddresses) {
            try {
                this.registerNodeMonitor(nodeMonitorAddress);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }
        _taskPlacer = TaskPlacer.createTaskPlacer(_beta,
                _schedulingStrategy, _nodeMonitorClients);
    }


    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        if (request.tasks.isEmpty()) {
            return;
        }
        handleJobSubmission(request);
        _counter.getAndAdd(request.tasks.size());
        if (_counter.get() / _batchSize == 0) {
            LOG.info(_counter.get() + " tasks scheduled.");
        }
    }

    @Override
    public void handleJobSubmission(TSchedulingRequest request) throws TException {
        LOG.debug(Logging.functionCall(request));

        long start = System.currentTimeMillis();


        String user = "";
        if (request.getUser() != null && request.getUser().getUser() != null) {
            user = request.getUser().getUser();
        }
        String description = "";
        if (request.getDescription() != null) {
            description = request.getDescription();
        }


        AUDIT_LOG.info(Logging.auditEventString("arrived",
                request.requestId,
                request.getTasks().size(),
                _address.getHost(),
                _address.getPort(),
                user, description));

        Map<InetSocketAddress, TEnqueueTaskReservationsRequest> enqueueTaskReservationsRequests;
        enqueueTaskReservationsRequests = _taskPlacer.getEnqueueTaskReservationsRequests(
                request, _loadMaps, _address);

        for (Map.Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry :
                enqueueTaskReservationsRequests.entrySet())  {
            try {
                InternalService.AsyncClient client = _nodeMonitorAsyncClientPool.borrowClient(entry.getKey());
                LOG.debug("Launching enqueueTask for request " + request.requestId + "on node: " + entry.getKey());
                AUDIT_LOG.debug(Logging.auditEventString(
                        "scheduler_launch_enqueue_task", entry.getValue().taskId,
                        entry.getKey().getAddress().getHostAddress()));
                client.enqueueTaskReservations(entry.getValue(), new EnqueueTaskReservationsCallback(
                        entry.getValue().taskId, entry.getKey(), client));
            } catch (Exception e) {
                LOG.error("Error enqueuing task on node " + entry.getKey().toString() + ":" + e);
            }
        }
        long end = System.currentTimeMillis();
        LOG.debug("All tasks enqueued for request " + request.requestId + "; returning. Total time: " +
                (end - start) + " milliseconds");
    }


    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        for (Map.Entry<String, TNodeState> entry : snapshot.entrySet()) {
            Optional<InetSocketAddress> address = Serialization.strToSocket(entry.getKey());
            if (address.isPresent()) {
                if (_loadMaps.containsKey(address.get())) {
                    LOG.debug("Updating load for node: " + entry.getKey());
                } else {
                    LOG.error("Adding load for unregistered node: " + entry.getKey());
                }
                _loadMaps.put(address.get(), entry.getValue());
            } else {
                LOG.error("Invalid address: " + entry.getKey());
            }
        }
    }

    @Override
    public void registerNodeMonitor(String nodeMonitorAddress) throws TException {
        LOG.info("Registering node monitor at " + nodeMonitorAddress);
        Optional<InetSocketAddress> address = Serialization.strToSocket(nodeMonitorAddress);
        if (address.isPresent()) {
            InetSocketAddress socket = address.get();
            _loadMaps.put(socket, new TNodeState(new TResourceVector(0, 0, 0), 0));
            try {
                _nodeMonitorClients.put(address.get(), TClients.createBlockingNodeMonitorClient(socket));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new TException("Invalid address: " + nodeMonitorAddress);
        }
    }

    private class EnqueueTaskReservationsCallback implements AsyncMethodCallback<Boolean> {
        String _taskId;
        InetSocketAddress _nodeMonitorAddress;
        long _startTimeMillis;
        InternalService.AsyncClient _client;

        public EnqueueTaskReservationsCallback(String taskId, InetSocketAddress nodeMonitorAddress,
                                               InternalService.AsyncClient client) {
            _taskId = taskId;
            _nodeMonitorAddress = nodeMonitorAddress;
            _startTimeMillis = System.currentTimeMillis();
            _client = client;
        }

        @Override
        public void onComplete(Boolean aBoolean) {
            if (!aBoolean) {
                LOG.error("Error enqueuing task on node " + _nodeMonitorAddress.toString());
            }
            AUDIT_LOG.debug(Logging.auditEventString(
                    "scheduler_complete_enqueue_task", _taskId,
                    _nodeMonitorAddress.getAddress().getHostAddress()));
            long totalTime = System.currentTimeMillis() - _startTimeMillis;
            LOG.debug("Enqueue Task RPC to " + _nodeMonitorAddress.getAddress().getHostAddress() +
                    " for request " + _taskId + " completed in " + totalTime + "ms");
            try {
                _nodeMonitorAsyncClientPool.returnClient(_nodeMonitorAddress, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool: " + e);
            }
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
        }
    }
}
