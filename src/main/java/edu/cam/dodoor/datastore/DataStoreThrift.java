package edu.cam.dodoor.datastore;

import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.DataStoreService;
import edu.cam.dodoor.thrift.SchedulerService;
import edu.cam.dodoor.thrift.THostPort;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;


public class DataStoreThrift implements DataStoreService.Iface {
    private final static Logger LOG = Logger.getLogger(DataStoreThrift.class);
    DataStore _dataStore;
    Configuration _config;
    THostPort _networkPort;
    private ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _schedulerAddress;
    List<InetSocketAddress> _nodeMonitorAddress;
    private AtomicInteger _counter;
    private int _batchSize;

    void initialize(Configuration conf,
                    InetSocketAddress socket,
                    List<String> schedulerAddresses,
                    List<String> nodeMonitorAddresses)
            throws TException, IOException {
        _dataStore = new BasicDataStoreImpl(new HashMap<>());
        _config = conf;
        _networkPort = Network.socketAddressToThrift(socket);
        _dataStore.initialize(_config);
        _counter = new AtomicInteger(0);
        _batchSize = conf.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());

        _schedulerAddress = new ArrayList<>();
        _nodeMonitorAddress = new ArrayList<>();

        for (String schedulerAddress : schedulerAddresses) {
            this.registerScheduler(schedulerAddress);
        }

        for (String nodeMonitorAddress : nodeMonitorAddresses) {
            this.registerNodeMonitor(nodeMonitorAddress);
        }

        DataStoreService.Processor<DataStoreService.Iface> processor = new DataStoreService.Processor<>(this);
        int dataStorePort = _config.getInt(DodoorConf.DATA_STORE_THRIFT_PORT, DodoorConf.DEFAULT_DATA_STORE_THRIFT_THREADS);
        int threads = _config.getInt(DodoorConf.DATA_STORE_THRIFT_THREADS, DodoorConf.DEFAULT_DATA_STORE_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(dataStorePort, threads, processor);
    }

    @Override
    public void registerScheduler(String schedulerAddress) throws TException {
        Optional<InetSocketAddress> schedulerAddressOptional = Serialization.strToSocket(schedulerAddress);
        if (schedulerAddressOptional.isPresent()) {
            _schedulerAddress.add(schedulerAddressOptional.get());
        } else {
            throw new TException("Scheduler address " + schedulerAddress + " not found");
        }
    }

    @Override
    public void registerNodeMonitor(String nodeMonitorAddress) throws TException {
        Optional<InetSocketAddress> nodeMonitorAddressOptional = Serialization.strToSocket(nodeMonitorAddress);
        if (nodeMonitorAddressOptional.isPresent()) {
            _nodeMonitorAddress.add(nodeMonitorAddressOptional.get());
            _dataStore.updateNodeLoad(nodeMonitorAddress, new TNodeState());
        } else {
            throw new TException("Node monitor address " + nodeMonitorAddress + " not found");
        }
    }

    @Override
    public void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates) throws TException{
        if (!_dataStore.containsNode(nodeMonitorAddress)) {
            LOG.error("Received updated loads from unregistered nodes" + nodeMonitorAddress);
        }
        _dataStore.updateNodeLoad(nodeMonitorAddress, nodeStates);
        _counter.getAndAdd(1);

        if (_counter.get() > _batchSize) {
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
        _counter.set(0);
    }

    @Override
    public Map<String, TNodeState> getNodeStates() {
        return _dataStore.getNodeStates();
    }

    private class UpdateNodeLoadCallBack implements AsyncMethodCallback<Void> {
        private SchedulerService.AsyncClient _client;
        private InetSocketAddress _address;

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
