package edu.cam.dodoor.scheduler;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.scheduler.taskplacer.TaskPlacer;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerImpl implements Scheduler{

    private final static Logger LOG = LoggerFactory.getLogger(SchedulerImpl.class);

    MetricRegistry _metrics;

    /** Used to uniquely identify requests arriving at this scheduler. */
    private final AtomicInteger _counter = new AtomicInteger(0);


    /** Thrift client pool for async communicating with node monitors */
    private final ThriftClientPool<NodeEnqueueService.AsyncClient> _nodeEnqueueServiceAsyncClientPool =
            new ThriftClientPool<>(
                    new ThriftClientPool.NodeEnqueuServiceMakerFactory());
    
    private Map<InetSocketAddress, NodeMonitorService.Client> _nodeEqueueSocketToNodeMonitorClients;

    private ConcurrentMap<InetSocketAddress, TNodeState> _mapEqueueSocketToNodeState;
    private THostPort _address;
    private int _batchSize;
    private TaskPlacer _taskPlacer;
    private SchedulerServiceMetrics _schedulerServiceMetrics;

    @Override
    public void initialize(Configuration config, InetSocketAddress socket,
                           SchedulerServiceMetrics schedulerServiceMetrics) throws IOException {
        _schedulerServiceMetrics = schedulerServiceMetrics;
        _address = Network.socketAddressToThrift(socket);
        _mapEqueueSocketToNodeState = Maps.newConcurrentMap();
        String schedulingStrategy = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
        double beta = config.getDouble(DodoorConf.BETA, DodoorConf.DEFAULT_BETA);
        _batchSize = config.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _nodeEqueueSocketToNodeMonitorClients = Maps.newHashMap();

        List<String> nmPorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_MONITOR_THRIFT_PORTS)));
        List<String> nePorts = new ArrayList<>(List.of(config.getStringArray(DodoorConf.NODE_ENQUEUE_THRIFT_PORTS)));

        if (nmPorts.size() != nePorts.size()) {
            throw new IllegalArgumentException(DodoorConf.NODE_MONITOR_THRIFT_PORTS + " and " +
                    DodoorConf.NODE_ENQUEUE_THRIFT_PORTS + " not of equal length");
        }
        if (nmPorts.isEmpty()) {
            nmPorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_MONITOR_THRIFT_PORT));
            nePorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_ENQUEUE_THRIFT_PORT));
        }
        for (String nodeIp : config.getStringArray(DodoorConf.STATIC_NODE)) {
            for (int i = 0; i < nmPorts.size(); i++) {
                String nodeFullAddress = nodeIp + ":" + nmPorts.get(i) + ":" + nePorts.get(i);
                try {
                    this.registerNode(nodeFullAddress);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        _taskPlacer = TaskPlacer.createTaskPlacer(beta,
                schedulingStrategy, _nodeEqueueSocketToNodeMonitorClients);
    }


    @Override
    public void submitJob(TSchedulingRequest request) throws TException {
        if (request.tasks.isEmpty()) {
            return;
        }
        handleJobSubmission(request);
        _counter.getAndAdd(request.tasks.size());
        _schedulerServiceMetrics.taskSubmitted(request.tasks.size());
        if (_counter.get() / _batchSize == 0) {
            LOG.info("{} tasks scheduled.", _counter.get());
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


        LOG.info(Logging.auditEventString("arrived",
                request.requestId,
                request.getTasks().size(),
                _address.getHost(),
                _address.getPort(),
                user, description));

        Map<InetSocketAddress, TEnqueueTaskReservationRequest> enqueueTaskReservationRequests
                = _taskPlacer.getEnqueueTaskReservationRequests(request, _mapEqueueSocketToNodeState, _address);

        for (Map.Entry<InetSocketAddress, TEnqueueTaskReservationRequest> entry :
                enqueueTaskReservationRequests.entrySet())  {
            try {
                NodeEnqueueService.AsyncClient client = _nodeEnqueueServiceAsyncClientPool.borrowClient(entry.getKey());
                LOG.debug("Launching enqueueTask for request {}on node: {}", request.requestId, entry.getKey().getHostName());
                client.enqueueTaskReservation(entry.getValue(), new EnqueueTaskReservationCallback(
                        entry.getValue().taskId, entry.getKey(), client, _schedulerServiceMetrics));
            } catch (Exception e) {
                LOG.error("Error enqueuing task on node {}", entry.getKey().getHostName(), e);
            }
        }
        long end = System.currentTimeMillis();
        LOG.debug("All tasks enqueued for request {}; returning. Total time: {} milliseconds", request.requestId, end - start);
    }


    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        _schedulerServiceMetrics.loadUpdated();
        for (Map.Entry<String, TNodeState> entry : snapshot.entrySet()) {
            Optional<InetSocketAddress> neAddressOptional = Serialization.strToSocket(entry.getKey());
            if (neAddressOptional.isPresent()) {
                InetSocketAddress nodeEnqueueSocket = neAddressOptional.get();
                if (_mapEqueueSocketToNodeState.containsKey(nodeEnqueueSocket)) {
                    LOG.debug("Updating load for node: {}", nodeEnqueueSocket.getHostName());
                } else {
                    LOG.error("Adding load for unregistered node: {}", nodeEnqueueSocket.getHostName());
                }
                _mapEqueueSocketToNodeState.put(nodeEnqueueSocket, entry.getValue());
            } else {
                LOG.error("Invalid address: {}", entry.getKey());
            }
        }
    }

    @Override
    public void registerNode(String nodeAddress) throws TException {
        String[] nodeAddressParts = nodeAddress.split(":");
        if (nodeAddressParts.length != 3) {
            throw new TException("Invalid address: " + nodeAddress);
        }
        String nodeIp = nodeAddressParts[0];
        String nodeMonitorPort = nodeAddressParts[1];
        String nodeEnqueuePort = nodeAddressParts[2];
        String nodeMonitorAddress = nodeIp + ":" + nodeMonitorPort;
        Optional<InetSocketAddress> nmAddress = Serialization.strToSocket(nodeMonitorAddress);
        String nodeEnqueueAddress = nodeIp + ":" + nodeEnqueuePort;
        Optional<InetSocketAddress> neAddress = Serialization.strToSocket(nodeEnqueueAddress);
        if (nmAddress.isPresent() && neAddress.isPresent()) {
            InetSocketAddress nmSocket = nmAddress.get();
            InetSocketAddress neSocket = neAddress.get();
            _mapEqueueSocketToNodeState.put(neSocket, new TNodeState(
                    new TResourceVector(0, 0, 0), 0));
            try {
                _nodeEqueueSocketToNodeMonitorClients.put(neSocket,
                        TClients.createBlockingNodeMonitorClient(nmSocket));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Registering node monitor at {}", nmAddress.get().getHostName());
        } else {
            throw new TException("Invalid address: " + nodeMonitorAddress);
        }
    }

    private class EnqueueTaskReservationCallback implements AsyncMethodCallback<Boolean> {
        String _taskId;
        InetSocketAddress _nodeMonitorAddress;
        long _startTimeMillis;
        NodeEnqueueService.AsyncClient _client;
        SchedulerServiceMetrics _schedulerServiceMetrics;

        public EnqueueTaskReservationCallback(String taskId, InetSocketAddress nodeMonitorAddress,
                                              NodeEnqueueService.AsyncClient client,
                                              SchedulerServiceMetrics schedulerServiceMetrics) {
            _taskId = taskId;
            _nodeMonitorAddress = nodeMonitorAddress;
            _startTimeMillis = System.currentTimeMillis();
            _client = client;
            _schedulerServiceMetrics = schedulerServiceMetrics;
        }

        @Override
        public void onComplete(Boolean aBoolean) {
            if (!aBoolean) {
                LOG.error("Error enqueuing task on node {}", _nodeMonitorAddress.getHostName());
            }
            long totalTime = System.currentTimeMillis() - _startTimeMillis;
            _schedulerServiceMetrics.taskScheduled(totalTime);
            LOG.debug("Enqueue Task RPC to {} for request {} completed in {} ms",
                    new Object[]{_nodeMonitorAddress.getHostName(), _taskId, totalTime});
            try {
                _nodeEnqueueServiceAsyncClientPool.returnClient(_nodeMonitorAddress, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool: {}", e.getMessage());
            }
        }

        @Override
        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:{}", exception.getMessage());
        }
    }
}
