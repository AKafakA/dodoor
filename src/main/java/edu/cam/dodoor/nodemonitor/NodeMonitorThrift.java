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
    private final NodeMonitor _nodeMonitor = new NodeMonitorImpl();
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
    public void initialize(Configuration conf, int nmPort, int internalPort,
                           List<String> dataStoreAddresses)
            throws IOException, TException {
        _nodeMonitor.initialize(conf, internalPort);

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

        for (String dataStoreAddress : dataStoreAddresses) {
            registerDataStore(dataStoreAddress);
        }

        String hostname = Network.getHostName(conf);
        _nmAddress = new InetSocketAddress(hostname, nmPort);
    }

    @Override
    public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) throws TException {
        return _nodeMonitor.enqueueTaskReservations(request);
    }

    @Override
    public void cancelTaskReservations(TCancelTaskReservationsRequest request) throws TException {
        _nodeMonitor.cancelTaskReservations(request);
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
    public void tasksFinished(List<TFullTaskId> tasks) throws Exception {
        _nodeMonitor.taskFinished(tasks);
        for (InetSocketAddress dataStoreAddress : _dataStoreAddress) {
            DataStoreService.AsyncClient dataStoreClient = _dataStoreClientPool.borrowClient(dataStoreAddress);
            TNodeState nodeState = new TNodeState(_nodeMonitor.getRequestedResourceVector());
            dataStoreClient.updateNodeLoad(_nmAddress.toString(), nodeState, tasks.size(), new AsyncMethodCallback<Void>() {
                @Override
                public void onComplete(Void unused) {
                    LOG.info(Logging.auditEventString("update_node_state", _nmAddress.toString()));
                }

                @Override
                public void onError(Exception e) {
                    LOG.warn(Logging.auditEventString("fail_to_update_node_state", _nmAddress.toString()));
                }
            });
        }
    }

}
