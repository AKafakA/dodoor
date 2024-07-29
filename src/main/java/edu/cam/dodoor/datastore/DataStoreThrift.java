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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import edu.cam.dodoor.utils.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataStoreThrift implements DataStoreService.Iface {
    private static Logger LOG;
    DataStore _dataStore;
    Configuration _config;
    THostPort _networkPort;
    private ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _schedulerAddress;
    private AtomicLong _counter;
    private int _batchSize;
    private int _numTasksPerUpdateFromNode;
    MetricRegistry _metrics;
    private Meter _overrideRequestsRate;
    private Meter _addRequestsRate;
    private Meter _getRequestsRate;
    private Counter _numMessages;

    public void initialize(Configuration config, int port)
            throws TException, IOException {
        _metrics = SharedMetricRegistries.getOrCreate(DodoorConf.DATA_STORE_METRICS_REGISTRY);
        _overrideRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_OVERRIDE_REQUEST_RATE);
        _getRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_GET_REQUEST_RATE);
        _addRequestsRate = _metrics.meter(DodoorConf.DATA_STORE_METRICS_ADD_REQUEST_RATE);
        _numMessages = _metrics.counter(DodoorConf.DATA_STORE_METRICS_NUM_MESSAGES);
        boolean _cachedEnabled =
                SchedulerUtils.isCachedEnabled(config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER));

        _dataStore = new BasicDataStoreImpl();
        _config = config;

        String hostname = Network.getHostName(config);
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
        _networkPort = Network.socketAddressToThrift(addr);

        _dataStore.initialize(_config);
        _counter = new AtomicLong(0);
        _batchSize = config.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());

        _schedulerAddress = new ArrayList<>();
        LOG = LoggerFactory.getLogger(DataStoreThrift.class);


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

        if (_cachedEnabled) {
            for (String schedulerAddress : ConfigUtil.parseNodeAddress(config, DodoorConf.STATIC_SCHEDULER,
                    DodoorConf.SCHEDULER_THRIFT_PORTS)) {
                this.handleRegisterScheduler(schedulerAddress);
            }

            for (String nodeIp : config.getStringArray(DodoorConf.STATIC_NODE)) {
                for (int i = 0; i < nmPorts.size(); i++) {
                    String nodeFullAddress = nodeIp + ":" + nmPorts.get(i) + ":" + nePorts.get(i);
                    try {
                        this.handleRegisterNode(nodeFullAddress);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        DataStoreService.Processor<DataStoreService.Iface> processor = new DataStoreService.Processor<>(this);
        int threads = _config.getInt(DodoorConf.DATA_STORE_THRIFT_THREADS, DodoorConf.DEFAULT_DATA_STORE_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(port, threads, processor);

        _numTasksPerUpdateFromNode = config.getInt(DodoorConf.NODE_NUM_TASKS_TO_UPDATE, DodoorConf.DEFAULT_NODE_NUM_TASKS_TO_UPDATE);

        if (config.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED)) {
            String datastoreLogPath = config.getString(DodoorConf.DATA_STORE_METRICS_LOG_FILE,
                    DodoorConf.DEFAULT_DATA_STORE_METRICS_LOG_FILE);
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
            reporter.start(config.getInt(DodoorConf.TRACKING_INTERVAL_IN_SECONDS, DodoorConf.DEFAULT_TRACKING_INTERVAL),
                    TimeUnit.SECONDS);
        }
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
            LOG.debug(Logging.auditEventString("register_scheduler",
                    schedulerAddressOptional.get().getHostName()));
        } else {
            throw new TException("Scheduler address " + schedulerAddress + " not found");
        }
    }

    @Override
    public void registerNode(String nodeFullAddress) throws TException {
        _numMessages.inc();
        handleRegisterNode(nodeFullAddress);
    }

    private void handleRegisterNode(String nodeFullAddress) throws TException {
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
            _dataStore.updateNodeLoad(nodeEnqueueAddress, new TNodeState());
        } else {
            throw new TException("Node monitor address " + nodeEnqueueAddress + " not found");
        }
    }

    @Override
    public void overrideNodeState(String nodeEnqueueAddress, TNodeState nodeState) throws TException {
        _numMessages.inc();
        Optional<InetSocketAddress> nodeEnqueueAddressSocket = Serialization.strToSocket(nodeEnqueueAddress);
        if (!nodeEnqueueAddressSocket.isPresent()) {
            throw new TException("Invalid address: " + nodeEnqueueAddress);
        }
        InetSocketAddress nodeEnqueueAddressInet = nodeEnqueueAddressSocket.get();
        LOG.debug(Logging.auditEventString("update_node_load", nodeEnqueueAddressInet.getHostName()));
        _dataStore.updateNodeLoad(nodeEnqueueAddress, nodeState);
        _overrideRequestsRate.mark(_numTasksPerUpdateFromNode);
    }

    @Override
    public synchronized void addNodeLoad(String nodeEnqueueAddress, TResourceVector resourceLoad, int numTasks, int sign)
            throws TException {
        long numScheduledBefore = _counter.get();
        _numMessages.inc();
        _dataStore.addNodeLoad(nodeEnqueueAddress, resourceLoad, numTasks, sign);
        _counter.getAndAdd(numTasks);
        _addRequestsRate.mark(numTasks);
        if (_counter.get() / _batchSize != numScheduledBefore / _batchSize) {
            for (InetSocketAddress socket : _schedulerAddress) {
                SchedulerService.AsyncClient client = null;
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
            LOG.info(Logging.auditEventString("deliver_nodes_load_to_scheduler",
                    _address.getHostName()));
            try {
                _schedulerClientPool.returnClient(_address, _client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception e) {
            LOG.warn(Logging.auditEventString("failed_deliver_nodes_load_to_scheduler",
                    _address.getHostName()));
        }
    }
}
