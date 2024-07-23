package edu.cam.dodoor.node;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class NodeThrift implements NodeMonitorService.Iface, NodeEnqueueService.Iface {

    private final static Logger LOG = LoggerFactory.getLogger(NodeThrift.class);

    // Defaults if not specified by configuration
    final Node _node = new NodeImpl();
    List<InetSocketAddress> _dataStoreAddress;
    ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    String _neAddress;
    String _hostName;
    NodeServiceMetrics _nodeServiceMetrics;
    private Counter _numMessages;

    private boolean _cachedEnabled;


    /**
     * Initialize this thrift service.
     *
     * This spawns 2 multi-threaded thrift servers, one exposing the app-facing
     * agent service and the other exposing the internal-facing agent service,
     * and listens for requests to both servers. We require explicit specification of the
     * ports for these respective interfaces, since they cannot always be determined from
     * within this class under certain configurations (e.g. a config file specifies
     * multiple NodeMonitors).
     */
    public void initialize(Configuration config, int nmPort, int nePort)
            throws IOException, TException {
        MetricRegistry metrics = SharedMetricRegistries.getOrCreate(DodoorConf.NODE_METRICS_REGISTRY);
        _nodeServiceMetrics = new NodeServiceMetrics(metrics);
        _numMessages = metrics.counter("node.num.messages");
        _node.initialize(config, this);

        _cachedEnabled = SchedulerUtils.isCachedEnabled(config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));

        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<>(this);

        int threads = config.getInt(DodoorConf.NM_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nmPort, threads, processor);

        // Setup internal-facing agent service.
        NodeEnqueueService.Processor<NodeEnqueueService.Iface> nodeEnqueueProcessor =
                new NodeEnqueueService.Processor<>(this);
        int neThreads = config.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nePort,neThreads, nodeEnqueueProcessor);

        _dataStoreClientPool = new ThriftClientPool<>(new ThriftClientPool.DataStoreServiceMakerFactory());
        _dataStoreAddress = new ArrayList<>();

        if (_cachedEnabled) {
            for (String dataStoreAddress : ConfigUtil.parseNodeAddress(config, DodoorConf.STATIC_DATA_STORE,
                    DodoorConf.DATA_STORE_THRIFT_PORTS)) {
               handleRegisterDataStore(dataStoreAddress);
            }
        }

        _neAddress = null;
        _hostName = null;
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        _numMessages.inc();
        if (_neAddress == null) {
            Optional<InetSocketAddress> neAddressSocketOptional = Serialization.strToSocket(request.nodeEnqueueAddress);
            if (neAddressSocketOptional.isPresent()) {
                _neAddress = request.nodeEnqueueAddress;
                _hostName = neAddressSocketOptional.get().getHostName();
            } else {
                throw new TException("Node enqueue address " + _neAddress + " not valid");
            }
            LOG.info(Logging.auditEventString("register_ne_address_local_host", _hostName));
        } else if (!_neAddress.equals(request.nodeEnqueueAddress)) {
            throw new TException("Node enqueue address mismatch: " + _neAddress + " vs " + request.nodeEnqueueAddress);
        }
        _nodeServiceMetrics.taskEnqueued();
        return _node.enqueueTaskReservation(request);
    }

    @Override
    public void registerDataStore(String dataStoreAddress) throws TException {
        _numMessages.inc();
        handleRegisterDataStore(dataStoreAddress);
    }

    private void handleRegisterDataStore(String dataStoreAddress) throws TException {
        Optional<InetSocketAddress> dataStoreAddressOptional = Serialization.strToSocket(dataStoreAddress);
        if (dataStoreAddressOptional.isPresent()) {
            _dataStoreAddress.add(dataStoreAddressOptional.get());
            LOG.debug(Logging.auditEventString("register_datastore",
                    dataStoreAddressOptional.get().getHostName()));
        } else {
            throw new TException("Data store address " + dataStoreAddress + " not found");
        }
    }

    @Override
    public void taskFinished(TFullTaskId task) throws TException {
        _numMessages.inc();
        _node.taskFinished(task);
        LOG.debug(Logging.auditEventString("task_finished_from_node", task.taskId, _hostName));
    }

    @Override
    public int getNumTasks() throws TException {
        _numMessages.inc();
        return _node.getNumTasks();
    }
}
