package edu.cam.dodoor.node;

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
import java.util.concurrent.atomic.AtomicInteger;

public class NodeThrift implements NodeMonitorService.Iface, NodeEnqueueService.Iface {

    private final static Logger LOG = Logger.getLogger(NodeThrift.class);

    // Defaults if not specified by configuration
    private static final Node _node = new NodeImpl();
    private List<InetSocketAddress> _dataStoreAddress;
    private ThriftClientPool<DataStoreService.AsyncClient> _dataStoreClientPool;
    InetSocketAddress _neAddress;
    private int _numTasksToUpdate;
    private AtomicInteger _counter;

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
    public void initialize(Configuration conf, int nmPort, int nePort)
            throws IOException, TException {
        _node.initialize(conf, this);
        _counter = new AtomicInteger(0);

        _numTasksToUpdate = conf.getInt(DodoorConf.NUM_TASKS_TO_UPDATE,
                DodoorConf.DEFAULT_NUM_TASKS_TO_UPDATE);

        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<>(this);

        int threads = conf.getInt(DodoorConf.NM_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nmPort, threads, processor);

        // Setup internal-facing agent service.
        NodeEnqueueService.Processor<NodeEnqueueService.Iface> nodeEnqueueProcessor =
                new NodeEnqueueService.Processor<>(this);
        int neThreads = conf.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DodoorConf.DEFAULT_NM_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nePort,neThreads, nodeEnqueueProcessor);

        _dataStoreClientPool = new ThriftClientPool<>(new ThriftClientPool.DataStoreServiceMakerFactory());
        _dataStoreAddress = new ArrayList<>();

        for (String dataStoreAddress : ConfigUtil.parseNodeAddress(conf, DodoorConf.STATIC_DATA_STORE,
                DodoorConf.DATA_STORE_THRIFT_PORTS)) {
            registerDataStore(dataStoreAddress);
        }

        String hostname = Network.getHostName(conf);
        _neAddress = new InetSocketAddress(hostname, nePort);
    }

    @Override
    public boolean enqueueTaskReservation(TEnqueueTaskReservationRequest request) throws TException {
        return _node.enqueueTaskReservation(request);
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
        _node.taskFinished(task);

        if (_counter.incrementAndGet() % _numTasksToUpdate == 0) {
            for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
                DataStoreService.AsyncClient dataStoreClient = null;
                try {
                    dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreAddress);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                TNodeState nodeState = new TNodeState(_node.getRequestedResourceVector(), _node.getNumTasks());
                dataStoreClient.updateNodeLoad(_neAddress.toString(), nodeState,
                        new UpdateNodeLoadCallBack(dataStoreAddress, dataStoreClient));
            }
            LOG.info(Logging.auditEventString("update_node_load_to_datastore",
                    _neAddress.getHostName()));
            _counter.set(0);
        }
    }

    @Override
    public int getNumTasks() throws TException {
        return _node.getNumTasks();
    }

    private class UpdateNodeLoadCallBack implements AsyncMethodCallback<Void> {
        private final DataStoreService.AsyncClient _client;
        private final InetSocketAddress _address;

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
