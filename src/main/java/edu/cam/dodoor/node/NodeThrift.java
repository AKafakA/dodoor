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
    protected Node _node;
    private List<InetSocketAddress> _dataStoreAddress;
    private String _neAddressStr;
    private NodeServiceMetrics _nodeServiceMetrics;
    private Counter _numMessages;
    private String _nodeIp;


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
        _numMessages = metrics.counter(DodoorConf.NODE_METRICS_NUM_MESSAGES);
        _dataStoreAddress = new ArrayList<>();

        boolean cachedEnabled = SchedulerUtils.isCachedEnabled(
                config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));

        // Setup internal-facing agent service.
        NodeEnqueueService.Processor<NodeEnqueueService.Iface> nodeEnqueueProcessor =
                new NodeEnqueueService.Processor<>(this);
        int neThreads = config.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nePort,neThreads, nodeEnqueueProcessor);

        if (cachedEnabled) {
            for (String dataStoreAddress : ConfigUtil.parseNodeAddress(config, DodoorConf.STATIC_DATA_STORE,
                    DodoorConf.DATA_STORE_THRIFT_PORTS)) {
               handleRegisterDataStore(dataStoreAddress);
            }
        }
        _neAddressStr = Network.thriftToSocketStr(Network.getInternalHostPort(nePort, config));
        _nodeIp = Network.getInternalHostPort(nePort, config).host;

        _node = new NodeImpl();
        _node.initialize(config, this);

        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<>(this);

        int threads = config.getInt(DodoorConf.NM_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_THRIFT_THREADS);

        TServers.launchThreadedThriftServer(nmPort, threads, processor);
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        _numMessages.inc();
        if (!_neAddressStr.equals(Network.thriftToSocketStr(request.nodeEnqueueAddress))) {
            throw new TException("Node enqueue address mismatch: " + _neAddressStr + " vs " + request.nodeEnqueueAddress);
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
                    dataStoreAddressOptional.get().getHostName(), dataStoreAddressOptional.get().getPort()));
        } else {
            throw new TException("Data store address " + dataStoreAddress + " not found");
        }
    }

    @Override
    public void taskFinished(TFullTaskId task) throws TException {
        _numMessages.inc();
        _node.taskFinished(task);
        LOG.debug(Logging.auditEventString("task_finished_from_node", task.taskId));
    }

    @Override
    public TNodeState getNodeState() throws TException {
        _numMessages.inc();
        return _node.getNodeState();
    }

    public List<InetSocketAddress> getDataStoreAddress() {
        return _dataStoreAddress;
    }

    public String getNeAddressStr() {
        return _neAddressStr;
    }

    public NodeServiceMetrics getNodeServiceMetrics() {
        return _nodeServiceMetrics;
    }

    public String getNodeIp() {
        return _nodeIp;
    }
}
