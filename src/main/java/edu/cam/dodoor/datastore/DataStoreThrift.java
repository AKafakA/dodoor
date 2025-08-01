package edu.cam.dodoor.datastore;

import com.codahale.metrics.*;
import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import edu.cam.dodoor.utils.ConfigUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataStoreThrift implements DataStoreService.Iface {
    private static Logger LOG;
    DataStore _dataStore;
    Configuration _config;
    private ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _schedulerAddress;
    private AtomicLong _counter;
    private int _batchSize;
    MetricRegistry _metrics;
    private Meter _overrideRequestsRate;
    private Meter _addRequestsRate;
    private Meter _getRequestsRate;
    private Counter _numMessages;

    public void initialize(Configuration staticConfig, int port, boolean logKicked, JSONObject hostConfig)
            throws TException, IOException {
        _metrics = SharedMetricRegistries.getOrCreate(DodoorConf.DATA_STORE_METRICS_REGISTRY);
        _overrideRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_OVERRIDE_REQUEST_RATE);
        _getRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_GET_REQUEST_RATE);
        _addRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_ADD_REQUEST_RATE);
        _numMessages = _metrics.counter(DodoorConf.DATA_STORE_METRICS_NUM_MESSAGES);
        boolean cachedEnabled =
                SchedulerUtils.isCachedEnabled(staticConfig.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));

        _dataStore = new BasicDataStoreImpl();
        _config = staticConfig;
        _dataStore.initialize(_config);
        _counter = new AtomicLong(0);
        _batchSize = staticConfig.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());

        _schedulerAddress = new ArrayList<>();
        LOG = LoggerFactory.getLogger(DataStoreThrift.class);


        List<String> nmPorts = new ArrayList<>();
        List<String> nePorts = new ArrayList<>();

        JSONObject nodeConfig = hostConfig.getJSONObject(DodoorConf.NODE_SERVICE_NAME);
        JSONArray nmPortsArray = nodeConfig.getJSONArray(DodoorConf.NODE_MONITOR_THRIFT_PORTS);
        JSONArray nePortsArray = nodeConfig.getJSONArray(DodoorConf.NODE_ENQUEUE_THRIFT_PORTS);
        for (int i = 0; i < nmPortsArray.length(); i++) {
            nmPorts.add(Integer.toString(nmPortsArray.getInt(i)));
        }
        for (int i = 0; i < nePortsArray.length(); i++) {
            nePorts.add(Integer.toString(nePortsArray.getInt(i)));
        }
        if (nmPorts.size() != nePorts.size()) {
            throw new IllegalArgumentException(DodoorConf.NODE_MONITOR_THRIFT_PORTS + " and " +
                    DodoorConf.NODE_ENQUEUE_THRIFT_PORTS + " not of equal length");
        }
        if (nmPorts.isEmpty()) {
            nmPorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_MONITOR_THRIFT_PORT));
            nePorts.add(Integer.toString(DodoorConf.DEFAULT_NODE_ENQUEUE_THRIFT_PORT));
        }

        if (cachedEnabled) {
            for (String schedulerAddress : ConfigUtil.parseNodeAddress(hostConfig, DodoorConf.SCHEDULER_SERVICE_NAME)) {
                this.handleRegisterScheduler(schedulerAddress);
            }
            JSONArray nodeTypes = nodeConfig.getJSONArray(DodoorConf.NODE_TYPE_LIST_KEY);
            for (int i = 0; i < nodeTypes.length(); i++) {
                JSONObject nodeTypeJson = nodeTypes.getJSONObject(i);
                String nodeType = nodeTypeJson.getString(DodoorConf.NODE_TYPE);
                JSONArray nodeIps = nodeTypeJson.getJSONArray(DodoorConf.SERVICE_HOST_LIST_KEY);
                for (int j = 0; j < nodeIps.length(); j++) {
                    String nodeIp = nodeIps.getString(j);
                    for (int k = 0; k < nmPorts.size(); k++) {
                        String nodeFullAddress = nodeIp + ":" + nmPorts.get(k) + ":" + nePorts.get(k);
                        try {
                            this.handleRegisterNode(nodeFullAddress, nodeType);
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        if (staticConfig.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED) && !logKicked) {
            String datastoreLogPathSuffix = staticConfig.getString(DodoorConf.DATA_STORE_METRICS_LOG_FILE_SUFFIX,
                    DodoorConf.DEFAULT_DATA_STORE_METRICS_LOG_FILE_SUFFIX);
            String datastoreLogPath =
                    staticConfig.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER) + "_"
                            + datastoreLogPathSuffix;
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DataStoreThrift.class);
            logger.setAdditivity(false);
            try {
                logger.addAppender(new FileAppender(new PatternLayout(), datastoreLogPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final Slf4jReporter reporter = Slf4jReporter.forRegistry(_metrics)
                    .outputTo(LoggerFactory.getLogger(DataStoreThrift.class))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            reporter.start(staticConfig.getInt(DodoorConf.TRACKING_INTERVAL_IN_SECONDS,
                            DodoorConf.DEFAULT_TRACKING_INTERVAL), TimeUnit.SECONDS);
        }

        DataStoreService.Processor<DataStoreService.Iface> processor = new DataStoreService.Processor<>(this);
        int threads = _config.getInt(DodoorConf.DATA_STORE_THRIFT_THREADS,
                DodoorConf.DEFAULT_DATA_STORE_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(port, threads, processor);
    }

    @Override
    public void registerScheduler(String schedulerAddress) throws TException {
        _numMessages.inc();
        handleRegisterScheduler(schedulerAddress);
    }

    private void handleRegisterScheduler(String schedulerAddress) throws TException {
        Optional<InetSocketAddress> schedulerAddressOptional = Serialization.strToSocket(schedulerAddress);
        if (schedulerAddressOptional.isPresent()) {
            _schedulerAddress.add(schedulerAddressOptional.get());
        } else {
            throw new TException("Scheduler address " + schedulerAddress + " not found");
        }
    }

    @Override
    public void registerNode(String nodeFullAddress, String nodeType) throws TException {
        _numMessages.inc();
        handleRegisterNode(nodeFullAddress, nodeType);
    }

    private void handleRegisterNode(String nodeFullAddress, String nodeType) throws TException {
        String[] nodeAddressParts = nodeFullAddress.split(":");
        if (nodeAddressParts.length != 3) {
            throw new TException("Invalid address: " + nodeFullAddress);
        }
        String nodeIp = nodeAddressParts[0];
        String nodeEnqueuePort = nodeAddressParts[2];
        String nodeEnqueueAddress = nodeIp + ":" + nodeEnqueuePort;
        Optional<InetSocketAddress> neAddress = Serialization.strToSocket(nodeEnqueueAddress);
        if (neAddress.isPresent()) {
            LOG.debug(Logging.auditEventString("register_node", neAddress.get().getHostName()));
            _dataStore.overrideNodeLoad(nodeEnqueueAddress, new TNodeState(new TResourceVector(), 0, 0, nodeIp,
                    nodeType));
        } else {
            throw new TException("Node monitor address " + nodeEnqueueAddress + " not found");
        }
    }

    @Override
    public void overrideNodeState(String nodeEnqueueAddress, TNodeState nodeState) {
        _numMessages.inc();
        _dataStore.overrideNodeLoad(nodeEnqueueAddress, nodeState);
        _overrideRequestsRate.mark(1);
    }

    @Override
    public synchronized void addNodeLoads(Map<String, TNodeState> additionNodeStates, int sign)
            throws TException {
        long numScheduledBefore = _counter.get();
        int numTasks = 0;
        for (TNodeState nodeState : additionNodeStates.values()) {
            numTasks += nodeState.numTasks;
        }
        _addRequestsRate.mark(numTasks);
        _numMessages.inc();
        _dataStore.addNodeLoads(additionNodeStates, sign);
        long numScheduledAfter = _counter.addAndGet(numTasks);
        if (numScheduledAfter  / _batchSize != numScheduledBefore / _batchSize) {
            for (InetSocketAddress socket : _schedulerAddress) {
                SchedulerService.AsyncClient client;
                try {
                    client = _schedulerClientPool.borrowClient(socket);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                client.updateNodeState(getNodeStates(), new UpdateNodeLoadCallBack(socket, client));
            }
        }
    }

    @Override
    public Map<String, TNodeState> getNodeStates() {
        _numMessages.inc();
        _getRequestsRate.mark();
        return _dataStore.getNodeStates();
    }

    private class UpdateNodeLoadCallBack implements AsyncMethodCallback<Void> {
        private final SchedulerService.AsyncClient _client;
        private final InetSocketAddress _address;

        public UpdateNodeLoadCallBack(InetSocketAddress address, SchedulerService.AsyncClient client) {
            _client = client;
            _address = address;
        }
        @Override
        public void onComplete(Void unused) {
            try {
                _schedulerClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception e) {
            LOG.error("Error updating node state", e);
            try {
                _schedulerClientPool.returnClient(_address, _client);
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        }
    }
}
