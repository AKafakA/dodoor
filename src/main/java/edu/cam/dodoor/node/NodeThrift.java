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
import org.json.JSONArray;
import org.json.JSONObject;
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
    private NodeServiceMetrics _nodeServiceMetrics;
    private Counter _numMessages;
    private String _nodeIp;
    private String _schedulerType;
    protected String _neAddressStr;
    protected String _nodeType;


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
    public void initialize(Configuration staticConfig, int nmPort, int nePort, JSONObject hostConfig,
                           JSONObject taskConfig)
            throws IOException, TException {
        MetricRegistry metrics = SharedMetricRegistries.getOrCreate(DodoorConf.NODE_METRICS_REGISTRY);
        _nodeServiceMetrics = new NodeServiceMetrics(metrics);
        _numMessages = metrics.counter(DodoorConf.NODE_METRICS_NUM_MESSAGES);
        _dataStoreAddress = new ArrayList<>();

        boolean cachedEnabled = SchedulerUtils.isCachedEnabled(
                staticConfig.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));

        // Setup internal-facing agent service.
        NodeEnqueueService.Processor<NodeEnqueueService.Iface> nodeEnqueueProcessor =
                new NodeEnqueueService.Processor<>(this);
        int neThreads = staticConfig.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nePort,neThreads, nodeEnqueueProcessor);

        _neAddressStr = Network.thriftToSocketStr(Network.getInternalHostPort(nePort, staticConfig));
        _nodeIp = Network.getInternalHostPort(nePort, staticConfig).host;

        JSONObject nodeConfig = hostConfig.getJSONObject(DodoorConf.NODE_SERVICE_NAME);
        JSONArray nodeTypes = nodeConfig.getJSONArray(DodoorConf.NODE_TYPE_LIST_KEY);
        JSONObject nodeTypeConfig = null;
        for (int i = 0; i < nodeTypes.length(); i++) {
            JSONObject nodeTypeJson = nodeTypes.getJSONObject(i);
            JSONArray host = nodeTypeJson.getJSONArray(DodoorConf.SERVICE_HOST_LIST_KEY);
            for (int j = 0; j < host.length(); j++) {
                String hostName = host.getString(j);
                if (_nodeIp.equals(hostName)) {
                    // If the node IP matches the host name, we can use this node type.
                    _nodeType = nodeTypeJson.getString(DodoorConf.NODE_TYPE);
                    nodeTypeConfig = nodeTypeJson;
                }
            }
        }
        if (nodeTypeConfig == null) {
            throw new TException("Node type not found for node IP: " + _nodeIp);
        } else {
            _node = new NodeImpl();
            _node.initialize(staticConfig, this, nodeTypeConfig, taskConfig);
        }

        if (cachedEnabled) {
            for (String dataStoreAddress : ConfigUtil.parseNodeAddress(hostConfig,
                    DodoorConf.DATA_STORE_SERVICE_NAME)) {
               handleRegisterDataStore(dataStoreAddress);
            }
        }


        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<>(this);

        int threads = staticConfig.getInt(DodoorConf.NM_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_THRIFT_THREADS);

        TServers.launchThreadedThriftServer(nmPort, threads, processor);

        _schedulerType = staticConfig.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER);
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
    public boolean cancelTaskReservation(TFullTaskId taskId) throws TException {
        if (_schedulerType.equals(DodoorConf.SPARROW_SCHEDULER)) {
            return _node.cancelTaskReservation(taskId);
        } else {
            throw new TException("Task reservation cancellation not supported for scheduler type " + _schedulerType);
        }
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
