package edu.cam.dodoor.datastore;

import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.DataStoreService;
import edu.cam.dodoor.thrift.SchedulerService;
import edu.cam.dodoor.thrift.THostPort;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.utils.ConfigUtil;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.Serialization;
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStoreThrift implements DataStoreService.Iface {
    DataStore _dataStore;
    Configuration _config;
    THostPort _networkPort;
    List<InetSocketAddress> _schedulerAddress;
    List<InetSocketAddress> _nodeMonitorAddress;

    void initialize(Configuration conf, InetSocketAddress socket, List<String> schedulerAddresses,
                    List<String> nodeMonitorAddresses)
            throws TException, IOException {
        _dataStore = new BasicDataStoreImpl(new HashMap<>());
        _config = conf;
        _networkPort = Network.socketAddressToThrift(socket);
        _dataStore.initialize(_config);

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
    public void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates) throws TException {
        _dataStore.updateNodeLoad(nodeMonitorAddress, nodeStates);
    }

    @Override
    public Map<String, TNodeState> getNodeStates() throws TException {
        return _dataStore.getNodeStates();
    }
}
