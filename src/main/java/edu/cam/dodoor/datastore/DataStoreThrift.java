package edu.cam.dodoor.datastore;

import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.DataStoreService;
import edu.cam.dodoor.thrift.SchedulerService;
import edu.cam.dodoor.thrift.THostPort;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStoreThrift implements DataStoreService.Iface {
    private final static Logger LOG = Logger.getLogger(DataStoreThrift.class);
    DataStore _dataStore;
    Configuration _config;
    THostPort _networkPort;
    private ThriftClientPool<SchedulerService.AsyncClient> _schedulerClientPool;
    List<InetSocketAddress> _schedulerAddress;
    private AtomicInteger _counter;
    private int _batchSize;

    void initialize(Configuration conf, InetSocketAddress socket, List<String> schedulerAddresses)
            throws TException, IOException {
        _dataStore = new BasicDataStoreImpl(new HashMap<>());
        _config = conf;
        _networkPort = Network.socketAddressToThrift(socket);
        _dataStore.initialize(_config);
        _counter = new AtomicInteger(0);
        _batchSize = conf.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
        _schedulerClientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory());

        for (String address : schedulerAddresses) {
            this.registerScheduler(address);
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
    public void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates, int numTasks) throws Exception {
        _dataStore.updateNodeLoad(nodeMonitorAddress, nodeStates);
        _counter.getAndAdd(numTasks);

        if (_counter.get() > _batchSize) {
            for (InetSocketAddress socket : _schedulerAddress) {
                SchedulerService.AsyncClient client = _schedulerClientPool.borrowClient(socket);
                client.updateNodeState(getNodeStates(), new AsyncMethodCallback<>() {
                    @Override
                    public void onComplete(Void unused) {
                        LOG.info(Logging.auditEventString("deliver_nodes_load_to_scheduler",
                                socket, _counter.get()));
                    }

                    @Override
                    public void onError(Exception e) {
                        LOG.info(Logging.auditEventString("failed_deliver_nodes_load_to_scheduler",
                                socket));
                    }
                });
            }
        }
        _counter.set(0);
    }

    @Override
    public Map<String, TNodeState> getNodeStates() throws TException {
        return _dataStore.getNodeStates();
    }
}
