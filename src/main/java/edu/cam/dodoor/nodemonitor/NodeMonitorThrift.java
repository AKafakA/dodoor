package edu.cam.dodoor.nodemonitor;

import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class NodeMonitorThrift implements NodeMonitorService.Iface, InternalService.Iface {

    private final static Logger LOG = Logger.getLogger(NodeMonitorThrift.class);

    // Defaults if not specified by configuration
    private static final NodeMonitor _nodeMonitor = new NodeMonitorImpl();
    private List<InetSocketAddress> _dataStoreAddress;
    private ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    InetSocketAddress _nmAddress;
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
    public void initialize(Configuration conf, int nmPort, int internalPort)
            throws IOException, TException {
        _nodeMonitor.initialize(conf, this);

        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<>(this);

        int threads = conf.getInt(DodoorConf.NM_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nmPort, threads, processor);

        // Setup internal-facing agent service.
        InternalService.Processor<InternalService.Iface> internalProcessor =
                new InternalService.Processor<>(this);
        int internalThreads = conf.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(internalPort, internalThreads, internalProcessor);

        _dataStoreClientPool = new ThriftClientPool<>(new ThriftClientPool.DataStoreServiceMakerFactory());
        _dataStoreAddress = new ArrayList<>();

        for (String dataStoreAddress : ConfigUtil.parseNodeAddress(conf, DodoorConf.STATIC_DATA_STORE)) {
            registerDataStore(dataStoreAddress);
        }

        String hostname = Network.getHostName(conf);
        _nmAddress = new InetSocketAddress(hostname, nmPort);
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        return _nodeMonitor.enqueueTaskReservation(request);
    }

    @Override
    public void registerDataStore(String dataStoreAddress) throws TException {
        Optional<InetSocketAddress> dataStoreAddressOptional = Serialization.strToSocket(dataStoreAddress);
        if (dataStoreAddressOptional.isPresent()) {
            _dataStoreAddress.add(dataStoreAddressOptional.get());
        } else {
            throw new TException("Data store address " + dataStoreAddress + " not found");
        }
    }

    @Override
    public void tasksFinished(TFullTaskId task) throws TException {
        _nodeMonitor.taskFinished(task);
        for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
            DataStoreService.AsyncClient dataStoreClient = null;
            try {
                dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreAddress);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            TNodeState nodeState = new TNodeState(_nodeMonitor.getRequestedResourceVector(), _nodeMonitor.getNumTasks());
            dataStoreClient.updateNodeLoad(_nmAddress.toString(), nodeState,
                    new UpdateNodeLoadCallBack(dataStoreAddress, dataStoreClient));
        }
    }

    @Override
    public int getNumTasks() throws TException {
        return _nodeMonitor.getNumTasks();
    }

    private class UpdateNodeLoadCallBack implements AsyncMethodCallback<Void> {
        private DataStoreService.AsyncClient _client;
        private InetSocketAddress _address;

        public UpdateNodeLoadCallBack(InetSocketAddress address, DataStoreService.AsyncClient client) {
            _client = client;
            _address = address;
        }

        @Override
        public void onComplete(Void unused) {
            LOG.info(Logging.auditEventString("deliver_nodes_load_to_scheduler",
                    _address.getHostName()));
            try {
                _dataStoreClientPool.returnClient(_address, _client);
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
