package edu.cam.dodoor.scheduler;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Logging;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Serialization;
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

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
    private AtomicInteger _counter = new AtomicInteger(0);


    /** Thrift client pool for async communicating with node monitors */
    private ThriftClientPool<InternalService.AsyncClient> _nodeMonitorAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.InternalServiceMakerFactory());
    
    private Map<InetSocketAddress, NodeMonitorService.Client> _nodeMonitorClients;

    private Map<InetSocketAddress, TNodeState> _loadMaps;
    private THostPort _address;

    private ConcurrentMap<Long, TaskPlacer> _requestTaskPlacers;
    private String _schedulingStrategy;

    private double _beta;

    @Override
    public void initialize(Configuration config, InetSocketAddress socket) throws IOException {
        _address = Network.socketAddressToThrift(socket);
        _requestTaskPlacers = Maps.newConcurrentMap();
        _loadMaps = Maps.newConcurrentMap();
        _schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        _beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
    }


    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        if (request.tasks.isEmpty()) {
            return;
        }
        handleJobSubmission(request);
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

        TaskPlacer taskPlacer = TaskPlacer.createTaskPlacer(request.requestId, _beta,
                _schedulingStrategy, _nodeMonitorClients);
        long requestId = request.requestId;

        _requestTaskPlacers.put(requestId, taskPlacer);
        Map<InetSocketAddress, TEnqueueTaskReservationsRequest> enqueueTaskReservationsRequests;
        enqueueTaskReservationsRequests = taskPlacer.getEnqueueTaskReservationsRequests(
                request, requestId, _loadMaps, _address);

        for (Map.Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry :
                enqueueTaskReservationsRequests.entrySet())  {
            try {
                InternalService.AsyncClient client = _nodeMonitorAsyncClientPool.borrowClient(entry.getKey());
                LOG.debug("Launching enqueueTask for request " + requestId + "on node: " + entry.getKey());
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
        LOG.debug("All tasks enqueued for request " + requestId + "; returning. Total time: " +
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
            _loadMaps.put(socket,
                    new TNodeState(new TResourceVector(0, 0, 0), 0));
            _nodeMonitorClients.put(address.get(), createNodeMonitorClient(socket));
        } else {
            throw new TException("Invalid address: " + nodeMonitorAddress);
        }
    }

    private NodeMonitorService.Client createNodeMonitorClient(InetSocketAddress socket) {
        try {
            TNonblockingTransport nbTr = new TNonblockingSocket(socket.getAddress().getHostAddress(), socket.getPort());
            TProtocolFactory factory = new TBinaryProtocol.Factory();
            return new NodeMonitorService.Client.Factory().getClient(factory.getProtocol(nbTr));
        } catch (IOException | TTransportException e) {
            throw new RuntimeException(e);
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
