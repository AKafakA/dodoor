package edu.cam.dodoor.nodemonitor;

import com.google.common.base.Optional;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Serialization;
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

public class NodeMonitorThrift implements NodeMonitorService.Iface, InternalService.Iface {
    // Defaults if not specified by configuration
    public final static int DEFAULT_NM_THRIFT_PORT = 20501;
    public final static int DEFAULT_NM_THRIFT_THREADS = 32;
    public final static int DEFAULT_INTERNAL_THRIFT_PORT = 20502;
    public final static int DEFAULT_INTERNAL_THRIFT_THREADS = 8;

    private final NodeMonitor _nodeMonitor = new NodeMonitorImpl();
    // The socket addr (ip:port) where we listen for requests from other Sparrow daemons.
    // Used when registering backends with the state store.
    private InetSocketAddress internalAddr;

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
            throws IOException {
        _nodeMonitor.initialize(conf, internalPort);

        // Setup application-facing agent service.
        NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
                new NodeMonitorService.Processor<NodeMonitorService.Iface>(this);

        int threads = conf.getInt(DodoorConf.NM_THRIFT_THREADS,
                DEFAULT_NM_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(nmPort, threads, processor);

        // Setup internal-facing agent service.
        InternalService.Processor<InternalService.Iface> internalProcessor =
                new InternalService.Processor<InternalService.Iface>(this);
        int internalThreads = conf.getInt(
                DodoorConf.INTERNAL_THRIFT_THREADS,
                DEFAULT_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(internalPort, internalThreads, internalProcessor);

        internalAddr = new InetSocketAddress(InetAddress.getLocalHost(), internalPort);
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
    public boolean registerBackend(String app, String listenSocket) throws TException {
        Optional<InetSocketAddress> backendAddr = Serialization.strToSocket(listenSocket);
        if (!backendAddr.isPresent()) {
            return false;
        }
        return _nodeMonitor.registerBackend(app, internalAddr, backendAddr.get());
    }

    @Override
    public void tasksFinished(List<TFullTaskId> tasks) throws TException {
        _nodeMonitor.taskFinished(tasks);
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {
        _nodeMonitor.sendFrontendMessage(app, taskId, status, message);
    }
}
