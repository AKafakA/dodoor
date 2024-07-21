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

import edu.cam.dodoor.utils.ConfigUtil;


public class DataStoreThrift implements DataStoreService.Iface {
    private final static Logger LOG = Logger.getLogger(DataStoreThrift.class);
    DataStore _dataStore;
    Configuration _config;
    THostPort _networkPort;
    private ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _schedulerAddress;
    private AtomicInteger _counter;
    private int _batchSize;
    private int _numTasksPerUpdate;

    public void initialize(Configuration config, int port)
            throws TException, IOException {
        _dataStore = new BasicDataStoreImpl(new HashMap<>());
        _config = config;

        String hostname = Network.getHostName(config);
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
        _networkPort = Network.socketAddressToThrift(addr);

        _dataStore.initialize(_config);
        _counter = new AtomicInteger(0);
        _batchSize = config.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());

        _schedulerAddress = new ArrayList<>();


        for (String schedulerAddress : ConfigUtil.parseNodeAddress(config, DodoorConf.STATIC_SCHEDULER,
                DodoorConf.SCHEDULER_THRIFT_PORTS)) {
            this.registerScheduler(schedulerAddress);
        }

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

        DataStoreService.Processor<DataStoreService.Iface> processor = new DataStoreService.Processor<>(this);
        int threads = _config.getInt(DodoorConf.DATA_STORE_THRIFT_THREADS, DodoorConf.DEFAULT_DATA_STORE_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(port, threads, processor);

        _numTasksPerUpdate = config.getInt(DodoorConf.NUM_TASKS_TO_UPDATE, DodoorConf.DEFAULT_NUM_TASKS_TO_UPDATE);
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
    public void registerNode(String nodeFullAddress) throws TException {
        String[] nodeAddressParts = nodeFullAddress.split(":");
        if (nodeAddressParts.length != 3) {
            throw new TException("Invalid address: " + nodeFullAddress);
        }
        LOG.debug(Logging.auditEventString("register_node", nodeFullAddress));
        String nodeIp = nodeAddressParts[0];
        String nodeEnqueuePort = nodeAddressParts[2];
        String nodeEnqueueAddress = nodeIp + ":" + nodeEnqueuePort;
        Optional<InetSocketAddress> neAddress = Serialization.strToSocket(nodeEnqueueAddress);
        if (neAddress.isPresent()) {
            _dataStore.updateNodeLoad(nodeEnqueueAddress, new TNodeState());
        } else {
            throw new TException("Node monitor address " + nodeEnqueueAddress + " not found");
        }
    }

    @Override
    public void updateNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates) throws TException{
        LOG.debug(Logging.auditEventString("update_node_load", nodeEnqueueAddress));
        _dataStore.updateNodeLoad(nodeEnqueueAddress, nodeStates);
        _counter.getAndAdd(_numTasksPerUpdate);

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
